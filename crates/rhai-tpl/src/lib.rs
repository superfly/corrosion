use std::io::BufWriter;
use std::io::Write;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;

use logos::Logos;
use parking_lot::RwLock;
use rhai::Dynamic;
use rhai::EvalAltResult;
use rhai::Scope;
use tracing::error;
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

#[derive(Clone)]
pub struct TemplateWriter<S> {
    writer: Arc<RwLock<BufWriter<Box<dyn Write + Send + Sync + 'static>>>>,
    pub state: S,
}

impl<S> Write for TemplateWriter<S> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.writer.write().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.write().flush()
    }
}

impl<S> TemplateWriter<S> {
    pub fn new<W: Write + Send + Sync + 'static>(w: W, state: S) -> Self {
        Self {
            writer: Arc::new(RwLock::new(BufWriter::new(Box::new(w)))),
            state,
        }
    }

    fn write_str(&mut self, data: &str) -> Result<(), Box<EvalAltResult>> {
        if data.is_empty() {
            return Ok(());
        }
        Ok(self
            .write_all(data.as_bytes())
            .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?)
    }

    fn write_char(&mut self, data: char) -> Result<(), Box<EvalAltResult>> {
        let mut b = [0; 2];
        let len = data.encode_utf8(&mut b).len();
        Ok(self
            .write_all(&b[0..len])
            .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?)
    }

    fn write_dynamic(&mut self, d: Dynamic) -> Result<(), Box<EvalAltResult>> {
        Ok(self
            .write_all(d.to_string().as_bytes())
            .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?)
    }
}

pub struct Engine {
    engine: rhai::Engine,
}

impl Deref for Engine {
    type Target = rhai::Engine;

    fn deref(&self) -> &Self::Target {
        &self.engine
    }
}

impl DerefMut for Engine {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.engine
    }
}

impl Engine {
    pub fn new<S: Clone + Send + Sync + 'static>() -> Self {
        let mut engine = rhai::Engine::new();

        engine.register_type::<TemplateWriter<S>>();
        engine.register_fn("write", TemplateWriter::<S>::write_str);
        engine.register_fn("write", TemplateWriter::<S>::write_char);
        engine.register_fn("write", TemplateWriter::<S>::write_dynamic);

        Self { engine }
    }

    pub fn compile(&self, input: &str) -> Result<Template, CompileError> {
        let mut lex = Tag::lexer(input);

        let mut program: String = String::new();

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
                    program.push_str("__tpl_writer.write(");
                    program.push_str(content);
                    program.push_str(");\n");
                }
            }

            last_tag = Some(tag);

            lex = closing.morph();
        }
        trace!("DONE");

        let tail = &lex.source()[last..];

        rhai_enquote(&mut program, tail, matches!(last_tag, Some(Tag::Control)));

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
            program.push_str("__tpl_writer.write('\\n');\n");
        } else {
            if !strip_newline && text.starts_with('\n') {
                program.push_str("__tpl_writer.write('\\n');\n");
            }

            program.push_str(r#"__tpl_writer.write("#);
            program.push_str(&enquote::enquote('`', text));
            program.push_str(");\n");
        }
    }
}

pub struct Template<'a> {
    ast: rhai::AST,
    engine: &'a Engine,
}

impl<'a> Template<'a> {
    pub fn render<W: Write + Send + Sync + 'static, S: Clone + Send + Sync + 'static>(
        &self,
        w: W,
        state: S,
    ) -> Result<(), Box<rhai::EvalAltResult>> {
        let mut scope = Scope::new();
        let mut w = TemplateWriter::new(w, state);
        scope.push("__tpl_writer", w.clone());

        self.engine
            .engine
            .eval_ast_with_scope(&mut scope, &self.ast)?;

        w.flush()
            .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?;

        Ok(())
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
        let tmpdir = tempfile::tempdir().unwrap();
        let filepath = tmpdir.path().join("output");

        let f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&filepath)
            .unwrap();

        let engine = Engine::new::<()>();

        let input = r#"<%
let a = [42, 123, 999, 0, true, "hello", "world!", 987.6543];

// Loop through the array
for (item, count) in a { %>
Item #<%= count + 1 %> = <%= item %>
<% } %>

tail"#;

        let tpl = engine.compile(input).unwrap();
        tpl.render(f, ()).unwrap();

        let output = std::fs::read_to_string(filepath).unwrap();

        assert_eq!(
            output,
            "Item #1 = 42
Item #2 = 123
Item #3 = 999
Item #4 = 0
Item #5 = true
Item #6 = hello
Item #7 = world!
Item #8 = 987.6543

tail",
        );
    }

    #[test]
    fn test_extend() {
        let tmpdir = tempfile::tempdir().unwrap();
        let filepath = tmpdir.path().join("output");

        let f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&filepath)
            .unwrap();

        let mut engine = Engine::new::<()>();

        engine.register_fn(
            "write",
            |tw: &mut TemplateWriter<()>, d: Dynamic| -> Result<(), Box<EvalAltResult>> {
                tw.write_all(format!("overloaded: {d}").as_bytes())
                    .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?;
                Ok(())
            },
        );

        let tpl = engine.compile("<%= 123 %>").unwrap();

        tpl.render(f, ()).unwrap();

        let output = std::fs::read_to_string(filepath).unwrap();

        assert_eq!(output, "overloaded: 123");
    }
}
