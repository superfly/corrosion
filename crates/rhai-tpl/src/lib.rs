use std::io::Write;
use std::sync::{Arc, RwLock};

use bytes::BytesMut;
use compact_str::CompactString;
use corro_types::api::QueryResult;
use corro_types::api::Statement;
use corro_types::change::SqliteValue;
use futures::StreamExt;
use indexmap::IndexMap;
use logos::Logos;
use rhai::{EvalAltResult, Map};
use serde::ser::{SerializeSeq, Serializer};
use serde_json::ser::Formatter;
use tokio_util::codec::{Decoder, LinesCodec};
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
struct QueryResponse {
    body: Arc<RwLock<hyper::Body>>,
}

impl IntoIterator for QueryResponse {
    type Item = Result<Row, Box<EvalAltResult>>;

    type IntoIter = QueryResponseIter;

    fn into_iter(self) -> Self::IntoIter {
        QueryResponseIter {
            body: self.body,
            codec: LinesCodec::new(),
            buf: BytesMut::new(),
            handle: tokio::runtime::Handle::current(),
            done: false,
            columns: None,
        }
    }
}

struct QueryResponseIter {
    body: Arc<RwLock<hyper::Body>>,
    buf: BytesMut,
    codec: LinesCodec,
    handle: tokio::runtime::Handle,
    done: bool,
    columns: Option<Arc<Vec<CompactString>>>,
}

#[derive(Clone)]
struct Row {
    columns: Arc<Vec<CompactString>>,
    cells: Vec<SqliteValue>,
}

impl IntoIterator for Row {
    type Item = Cell;

    type IntoIter = RowIter;

    fn into_iter(self) -> Self::IntoIter {
        RowIter {
            pos: 0,
            row: Arc::new(self),
        }
    }
}

struct RowIter {
    row: Arc<Row>,
    pos: usize,
}

impl Iterator for RowIter {
    type Item = Cell;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row.cells.len() > self.pos {
            let cell = Cell {
                index: self.pos,
                row: self.row.clone(),
            };
            self.pos += 1;
            Some(cell)
        } else {
            None
        }
    }
}

#[derive(Clone)]
struct Cell {
    index: usize,
    row: Arc<Row>,
}

#[derive(Clone)]
struct SqliteValueWrap(SqliteValue);

impl SqliteValueWrap {
    pub fn to_json(&mut self) -> String {
        match &self.0 {
            SqliteValue::Null => "null".into(),
            SqliteValue::Integer(i) => i.to_string(),
            SqliteValue::Real(f) => f.to_string(),
            SqliteValue::Text(t) => enquote::enquote('"', &t),
            SqliteValue::Blob(b) => hex::encode(b.as_slice()),
        }
    }
}

impl Cell {
    pub fn name(&mut self) -> Result<String, Box<EvalAltResult>> {
        Ok(self
            .row
            .columns
            .get(self.index)
            .ok_or_else(|| Box::new(EvalAltResult::from("cell does not exist")))?
            .to_string())
    }

    pub fn value(&mut self) -> Result<SqliteValueWrap, Box<EvalAltResult>> {
        Ok(SqliteValueWrap(
            self.row
                .cells
                .get(self.index)
                .ok_or_else(|| Box::new(EvalAltResult::from("cell does not exist")))?
                .clone(),
        ))
    }
}

impl Iterator for QueryResponseIter {
    type Item = Result<Row, Box<EvalAltResult>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            let bytes_res = match self.body.write() {
                Ok(mut body) => self.handle.block_on(body.next()),
                Err(e) => {
                    self.done = true;
                    return Some(Err(Box::new(EvalAltResult::from(e.to_string()))));
                }
            };
            match bytes_res {
                Some(Ok(b)) => self.buf.extend_from_slice(&b),
                Some(Err(e)) => {
                    self.done = true;
                    return Some(Err(Box::new(EvalAltResult::from(e.to_string()))));
                }
                None => {
                    self.done = true;
                    return None;
                }
            }
            match self.codec.decode(&mut self.buf) {
                Ok(Some(line)) => match serde_json::from_str(&line) {
                    Ok(res) => match res {
                        QueryResult::Columns(cols) => self.columns = Some(Arc::new(cols)),
                        QueryResult::Row(cells) => match self.columns.as_ref() {
                            Some(columns) => {
                                return Some(Ok(Row {
                                    columns: columns.clone(),
                                    cells,
                                }))
                            }
                            None => {
                                self.done = true;
                                return Some(Err(Box::new(EvalAltResult::from(
                                    "did not receive columns data",
                                ))));
                            }
                        },
                        QueryResult::Error(e) => {
                            self.done = true;
                            return Some(Err(Box::new(EvalAltResult::from(e))));
                        }
                    },
                    Err(e) => {
                        self.done = true;
                        return Some(Err(Box::new(EvalAltResult::from(e.to_string()))));
                    }
                },
                Ok(None) => {
                    continue;
                }
                Err(e) => {
                    self.done = true;
                    return Some(Err(Box::new(EvalAltResult::from(e.to_string()))));
                }
            }
        }
    }
}

pub struct Engine {
    engine: rhai::Engine,
}

impl Engine {
    pub fn new<W: Write + Send + Sync + 'static>(
        writer: Arc<RwLock<W>>,
        client: corro_client::CorrosionApiClient,
    ) -> Self {
        let mut engine = rhai::Engine::new();

        engine.register_fn("__write", {
            let writer = writer.clone();
            move |data: &str| -> Result<(), Box<EvalAltResult>> {
                if data.is_empty() {
                    return Ok(());
                }
                Ok(writer
                    .write()
                    .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?
                    .write_all(data.as_bytes())
                    .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?)
            }
        });

        engine.register_fn("__write", {
            let writer = writer.clone();
            move |data: char| -> Result<(), Box<EvalAltResult>> {
                let mut b = [0; 2];
                let len = data.encode_utf8(&mut b).len();
                Ok(writer
                    .write()
                    .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?
                    .write_all(&b[0..len])
                    .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?)
            }
        });

        engine.register_type_with_name::<QueryResponse>("QueryResponse");
        engine.register_iterator_result::<QueryResponse, Row>();

        engine.register_type_with_name::<Row>("Row");
        engine.register_iterator::<Row>();

        engine.register_type_with_name::<Cell>("Cell");
        engine.register_fn("name", Cell::name);
        engine.register_fn("value", Cell::value);

        engine.register_type_with_name::<SqliteValueWrap>("SqliteValue");
        engine.register_fn("to_json", SqliteValueWrap::to_json);
        engine.register_fn("to_string", SqliteValueWrap::to_json);

        engine.register_fn(
            "sql",
            move |query: &str| -> Result<QueryResponse, Box<EvalAltResult>> {
                let body = tokio::runtime::Handle::current()
                    .block_on(client.query(&Statement::Simple(query.into())))
                    .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?;

                Ok(QueryResponse {
                    body: Arc::new(RwLock::new(body)),
                })
            },
        );

        let json_writer = JsonWriter::new(writer.clone());

        engine.register_fn("write_json", {
            let json_writer = json_writer.clone();
            move |res: QueryResponse| -> Result<(), Box<EvalAltResult>> {
                json_writer.write_json(res)
            }
        });

        engine.register_fn("write_json", {
            let json_writer = json_writer.clone();
            move |res: QueryResponse, options: Map| -> Result<(), Box<EvalAltResult>> {
                json_writer.write_json_w_options(res, options)
            }
        });

        engine.register_fn(
            "write_json",
            move |res: QueryResponse, options: Option<Map>| -> Result<(), Box<EvalAltResult>> {
                let options = match options {
                    None => {
                        println!("no options provided, using default");
                        JsonOutput::default()
                    }
                    Some(map) => {
                        let pretty = map
                            .get("pretty")
                            .and_then(|d| d.as_bool().ok())
                            .unwrap_or(false);
                        let as_array = map
                            .get("as_array")
                            .and_then(|d| d.as_bool().ok())
                            .unwrap_or(false);

                        JsonOutput {
                            pretty,
                            row_values_as_array: as_array,
                        }
                    }
                };

                let mut w = writer
                    .write()
                    .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?;

                let rows = res.into_iter();

                if options.pretty {
                    let ser = serde_json::Serializer::pretty(&mut *w);
                    if options.row_values_as_array {
                        write_json_rows_as_array(ser, rows)
                    } else {
                        write_json_rows_as_object(ser, rows)
                    }
                } else {
                    let ser = serde_json::Serializer::new(&mut *w);
                    if options.row_values_as_array {
                        write_json_rows_as_array(ser, rows)
                    } else {
                        write_json_rows_as_object(ser, rows)
                    }
                }
            },
        );

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
                    program.push_str("__write(`${");
                    program.push_str(content);
                    program.push_str("}`);\n");
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
            program.push_str("__write('\\n');\n");
        } else {
            if !strip_newline && text.starts_with('\n') {
                program.push_str("__write('\\n');\n");
            }

            program.push_str(r#"__write("#);
            program.push_str(&enquote::enquote('`', text));
            program.push_str(");\n");
        }
    }
}

struct JsonWriter<W: Write> {
    writer: Arc<RwLock<W>>,
}

impl<W> Clone for JsonWriter<W>
where
    W: Write,
{
    fn clone(&self) -> Self {
        Self {
            writer: self.writer.clone(),
        }
    }
}

impl<W> JsonWriter<W>
where
    W: Write,
{
    fn new(writer: Arc<RwLock<W>>) -> Self {
        Self { writer }
    }

    fn write_json_w_options(
        &self,
        res: QueryResponse,
        options: Map,
    ) -> Result<(), Box<EvalAltResult>> {
        let mut json_output = {
            let pretty = options
                .get("pretty")
                .and_then(|d| d.as_bool().ok())
                .unwrap_or(false);
            let row_values_as_array = options
                .get("row_values_as_array")
                .and_then(|d| d.as_bool().ok())
                .unwrap_or(false);

            JsonOutput {
                pretty,
                row_values_as_array,
            }
        };

        let mut w = self
            .writer
            .write()
            .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?;

        json_output.write_rows(&mut *w, res.into_iter())
    }

    fn write_json(&self, res: QueryResponse) -> Result<(), Box<EvalAltResult>> {
        let mut json_output = JsonOutput::default();

        let mut w = self
            .writer
            .write()
            .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?;

        json_output.write_rows(&mut *w, res.into_iter())
    }
}

#[derive(Debug, Default, Clone)]
struct JsonOutput {
    pretty: bool,
    row_values_as_array: bool,
}

impl JsonOutput {
    fn write_rows<W: Write>(
        &mut self,
        w: &mut W,
        rows: QueryResponseIter,
    ) -> Result<(), Box<EvalAltResult>> {
        if self.pretty {
            let ser = serde_json::Serializer::pretty(w);
            if self.row_values_as_array {
                write_json_rows_as_array(ser, rows)
            } else {
                write_json_rows_as_object(ser, rows)
            }
        } else {
            let ser = serde_json::Serializer::new(w);
            if self.row_values_as_array {
                write_json_rows_as_array(ser, rows)
            } else {
                write_json_rows_as_object(ser, rows)
            }
        }
    }
}

fn write_json_rows_as_object<W: Write, F: Formatter>(
    mut ser: serde_json::Serializer<W, F>,
    mut rows: QueryResponseIter,
) -> Result<(), Box<EvalAltResult>> {
    let mut seq = ser
        .serialize_seq(None)
        .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?;
    while let Some(row_res) = rows.next() {
        let row = row_res.map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?;

        // we have to collect here due to serde limitations (I think), but it's not a big deal...
        let map = row
            .columns
            .iter()
            .enumerate()
            .filter_map(|(i, col)| row.cells.get(i).map(|value| (col, value)))
            .collect::<IndexMap<&CompactString, &SqliteValue>>();

        seq.serialize_element(&map)
            .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?;
    }
    seq.end()
        .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))
}

fn write_json_rows_as_array<W: Write, F: Formatter>(
    mut ser: serde_json::Serializer<W, F>,
    mut rows: QueryResponseIter,
) -> Result<(), Box<EvalAltResult>> {
    let mut seq = ser
        .serialize_seq(None)
        .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?;

    while let Some(row_res) = rows.next() {
        let row = row_res.map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?;
        seq.serialize_element(&row.cells)
            .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?;
    }

    SerializeSeq::end(seq).map_err(|e| Box::new(EvalAltResult::from(e.to_string())))
}

pub struct Template<'a> {
    ast: rhai::AST,
    engine: &'a Engine,
}

impl<'a> Template<'a> {
    pub fn render(&self) -> Result<(), Box<rhai::EvalAltResult>> {
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
    use corro_tests::launch_test_agent;
    use tokio::{sync::mpsc, task::block_in_place};
    use tripwire::Tripwire;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_basic() {
        _ = tracing_subscriber::fmt::try_init();
        let (tripwire, _, _) = Tripwire::new_simple();

        let tmpdir = tempfile::tempdir().unwrap();
        let filepath = tmpdir.path().join("output");

        let f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&filepath)
            .unwrap();

        let ta = launch_test_agent(|conf| conf.build(), tripwire.clone())
            .await
            .unwrap();

        let client = corro_client::CorrosionApiClient::new(ta.agent.api_addr());

        let engine = Engine::new(Arc::new(RwLock::new(f)), client);

        let input = r#"<%
let a = [42, 123, 999, 0, true, "hello", "world!", 987.6543];

// Loop through the array
for (item, count) in a { %>
Item #<%= count + 1 %> = <%= item %>
<% } %>

<%= hostname() %>

tail"#;

        let tpl = engine.compile(input).unwrap();
        tpl.render().unwrap();

        let output = std::fs::read_to_string(filepath).unwrap();

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_sql() {
        _ = tracing_subscriber::fmt::try_init();
        let (tripwire, _, _) = Tripwire::new_simple();

        let ta = launch_test_agent(|conf| conf.build(), tripwire.clone())
            .await
            .unwrap();

        let client = corro_client::CorrosionApiClient::new(ta.agent.api_addr());

        client
            .schema(&vec![Statement::Simple(corro_tests::TEST_SCHEMA.into())])
            .await
            .unwrap();

        client
            .execute(&vec![
                Statement::WithParams(
                    "insert into tests (id, text) values (?,?)".into(),
                    vec!["service-id".into(), "service-name".into()],
                ),
                Statement::WithParams(
                    "insert into tests (id, text) values (?,?)".into(),
                    vec!["service-id-2".into(), "service-name-2".into()],
                ),
            ])
            .await
            .unwrap();

        let tmpdir = tempfile::tempdir().unwrap();
        let filepath = tmpdir.path().join("output");

        enum Command {
            Render,
        }

        enum Reply {
            Rendered,
        }

        let (cmd_tx, mut cmd_rx) = mpsc::channel(10);
        let (reply_tx, mut reply_rx) = mpsc::channel(10);

        tokio::spawn({
            let filepath = filepath.clone();
            async move {
                let f = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(&filepath)
                    .unwrap();

                let engine = Engine::new(Arc::new(RwLock::new(f)), client);

                // loop {
                let cmd = cmd_rx.recv().await.unwrap();
                assert!(matches!(Command::Render, cmd));

                block_in_place(|| {
                    let input = r#"<%= write_json(sql("select * from tests"), #{pretty: true}) %>"#;

                    let tpl = engine.compile(input).unwrap();
                    tpl.render().unwrap();
                });
                reply_tx.send(Reply::Rendered).await.unwrap();
                // }
            }
        });

        cmd_tx.send(Command::Render).await.unwrap();
        let replied = reply_rx.recv().await.unwrap();
        assert!(matches!(Reply::Rendered, replied));

        let output = std::fs::read_to_string(filepath).unwrap();

        println!("output: {output}");
    }
}
