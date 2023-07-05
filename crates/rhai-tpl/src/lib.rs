use logos::Logos;
use rhai::EvalAltResult;
use tracing::trace;

#[derive(Debug, PartialEq, Clone, Copy, Default, thiserror::Error)]
#[error("parse error")]
pub struct ParseError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Logos)]
#[logos(
    skip r"[^<]+",
    error = ParseError,
)]
pub enum Tag {
    /// `<% control %>` tag
    #[token("<%")]
    Control,

    /// `<%= output %>` tag
    #[token("<%=")]
    Output,
}

#[derive(Debug, Logos)]
#[logos(skip r"[^%]+",)]
enum Closing {
    #[token("%>")]
    Match,
}

pub struct Engine {
    engine: rhai::Engine,
}

impl Engine {
    pub fn new() -> Self {
        let mut engine = rhai::Engine::new();
        engine.register_fn("hostname", || -> Result<String, Box<EvalAltResult>> {
            Ok(hostname::get()
                .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?
                .to_string_lossy()
                .to_string())
        });

        Self { engine }
    }

    pub fn compile(&self, input: &str) -> Result<Template, CompileError> {
        let mut lex = Tag::lexer(input);

        let mut program: String = "let __output = \"\";\n".into();

        let mut last_tag = None;
        let mut last = 0;

        while let Some(tag) = lex.next() {
            let tag = tag?;

            let before = &lex.source()[last..lex.span().start];
            last = lex.span().end;

            rhai_enquote(&mut program, before, matches!(last_tag, Some(Tag::Control)));

            let mut closing = lex.morph::<Closing>();

            let _tok = closing.next();
            if !matches!(Some(Closing::Match), _tok) {
                return Err(CompileError::UnclosedTag);
            }

            let content = &closing.source()[last..closing.span().start];
            last = closing.span().end;

            match tag {
                Tag::Control => {
                    trace!("CONTROL");
                    program.push_str(content);
                }
                Tag::Output => {
                    trace!("OUTPUT: {content:?}");
                    program.push_str("__output += `${");
                    program.push_str(content);
                    program.push_str("}`;\n");
                }
            }

            last_tag = Some(tag);

            lex = closing.morph();
        }
        trace!("DONE");

        let tail = &lex.source()[last..];

        rhai_enquote(&mut program, tail, matches!(last_tag, Some(Tag::Control)));

        program.push_str("return __output;");

        trace!("program: {program}");

        Ok(Template {
            ast: self.engine.compile(program)?,
            engine: &self,
        })
    }
}

fn rhai_enquote(program: &mut String, text: &str, strip_newline: bool) {
    if !text.is_empty() {
        trace!("enquoting: {text:?}");
        if text == "\n" {
            program.push_str("__output += '\\n';\n");
        } else {
            if !strip_newline && text.starts_with('\n') {
                program.push_str("__output += '\\n';\n");
            }

            program.push_str(r#"__output += "#);
            program.push_str(&enquote::enquote('`', text));
            program.push_str(";\n");
        }
    }
}

pub struct Template<'a> {
    ast: rhai::AST,
    engine: &'a Engine,
}

impl<'a> Template<'a> {
    pub fn render(&self) -> Result<String, Box<rhai::EvalAltResult>> {
        self.engine.engine.eval_ast(&self.ast)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CompileError {
    #[error(transparent)]
    Parse(#[from] ParseError),
    #[error(transparent)]
    Rhai(#[from] Box<rhai::EvalAltResult>),
    #[error(transparent)]
    RhaiParse(#[from] rhai::ParseError),
    #[error("unclosed tag")]
    UnclosedTag,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_basic() {
        _ = tracing_subscriber::fmt::try_init();
        let engine = Engine::new();

        let input = r#"<%
let a = [42, 123, 999, 0, true, "hello", "world!", 987.6543];

// Loop through the array
for (item, count) in a { %>
Item #<%= count + 1 %> = <%= item %>
<% } %>

<%= hostname() %>

tail"#;

        let tpl = engine.compile(input).unwrap();
        let output = tpl.render().unwrap();

        assert_eq!(
            output,
            format!(
                "Item #1 = 42
Item #2 = 123
Item #3 = 999
Item #4 = 0
Item #5 = true
Item #6 = hello
Item #7 = world!
Item #8 = 987.6543

{}

tail",
                hostname::get().unwrap().to_string_lossy()
            )
        );
    }
}
