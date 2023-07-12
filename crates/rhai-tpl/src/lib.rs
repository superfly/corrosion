use std::collections::HashMap;
use std::io::Seek;
use std::io::Write;
use std::sync::Arc;

use bytes::BytesMut;
use compact_str::CompactString;
use corro_client::CorrosionApiClient;
use corro_types::api::RowResult;
use corro_types::api::Statement;
use corro_types::change::SqliteValue;
use futures::StreamExt;
use indexmap::IndexMap;
use logos::Logos;
use parking_lot::RwLock;
use rhai::Scope;
use rhai::{EvalAltResult, Map};
use serde::ser::{SerializeSeq, Serializer};
use serde_json::ser::Formatter;
use tokio_util::codec::{Decoder, LinesCodec};
use tracing::trace;
use uuid::Uuid;

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
    query: Arc<RwLock<QueryHandle>>,
}

impl IntoIterator for QueryResponse {
    type Item = Result<Row, Box<EvalAltResult>>;

    type IntoIter = QueryResponseIter;

    fn into_iter(self) -> Self::IntoIter {
        QueryResponseIter {
            query: self.query.clone(),
            body: None,
            codec: LinesCodec::new(),
            buf: BytesMut::new(),
            handle: tokio::runtime::Handle::current(),
            done: false,
            columns: None,
        }
    }
}

struct QueryResponseIter {
    query: Arc<RwLock<QueryHandle>>,
    body: Option<hyper::Body>,
    buf: BytesMut,
    codec: LinesCodec,
    handle: tokio::runtime::Handle,
    done: bool,
    columns: Option<Arc<Vec<CompactString>>>,
}

#[derive(Clone)]
struct Row {
    id: i64,
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

impl QueryResponseIter {
    pub async fn recv(&mut self) -> Option<Result<Row, Box<EvalAltResult>>> {
        let body = match self.body.as_mut() {
            Some(body) => body,
            None => match self.query.write().body().await {
                Ok(body) => {
                    self.body = Some(body);
                    self.body.as_mut().unwrap()
                }
                Err(e) => {
                    self.done = true;
                    return Some(Err(Box::new(EvalAltResult::from(e.to_string()))));
                }
            },
        };

        if self.done {
            return None;
        }
        loop {
            let bytes_res = body.next().await;
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
                        RowResult::Columns(cols) => self.columns = Some(Arc::new(cols)),
                        RowResult::Row { rowid, cells, .. } => match self.columns.as_ref() {
                            Some(columns) => {
                                return Some(Ok(Row {
                                    id: rowid,
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
                        RowResult::EndOfQuery => {
                            return None;
                        }
                        RowResult::Error(e) => {
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

impl Iterator for QueryResponseIter {
    type Item = Result<Row, Box<EvalAltResult>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        self.handle.clone().block_on(self.recv())
    }
}

pub struct QueryHandle {
    query_id: Uuid,
    body: Option<hyper::Body>,
    client: CorrosionApiClient,
}

impl QueryHandle {
    async fn body(&mut self) -> Result<hyper::Body, corro_client::Error> {
        if let Some(body) = self.body.take() {
            return Ok(body);
        }

        self.client.watched_query(self.query_id).await
    }
}

pub trait WriteSeek: Write + Seek + Send + Sync + 'static {}
impl<T> WriteSeek for T where T: Write + Seek + Send + Sync + 'static {}

#[derive(Clone)]
pub struct TemplateWriter(Arc<RwLock<Box<dyn WriteSeek>>>);

impl TemplateWriter {
    pub fn new<W: WriteSeek>(w: W) -> Self {
        Self(Arc::new(RwLock::new(Box::new(w))))
    }

    fn write_str(&mut self, data: &str) -> Result<(), Box<EvalAltResult>> {
        if data.is_empty() {
            return Ok(());
        }
        Ok(self
            .0
            .write()
            .write_all(data.as_bytes())
            .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?)
    }

    fn write_char(&mut self, data: char) -> Result<(), Box<EvalAltResult>> {
        let mut b = [0; 2];
        let len = data.encode_utf8(&mut b).len();
        Ok(self
            .0
            .write()
            .write_all(&b[0..len])
            .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?)
    }

    fn write_query_to_json(&mut self, res: QueryResponse) -> Result<(), Box<EvalAltResult>> {
        let mut json_output = JsonOutput::default();

        let mut w = self.0.write();

        json_output.write_rows(&mut *w, res.into_iter())
    }

    fn write_query_to_json_w_options(
        &mut self,
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

        let mut w = self.0.write();

        json_output.write_rows(&mut *w, res.into_iter())
    }
}

pub struct Engine {
    engine: rhai::Engine,
}

impl Engine {
    pub fn new(client: corro_client::CorrosionApiClient) -> Self {
        let mut engine = rhai::Engine::new();

        engine.register_type::<TemplateWriter>();
        engine.register_fn("write", TemplateWriter::write_str);
        engine.register_fn("write", TemplateWriter::write_char);
        engine.register_fn("write_json", TemplateWriter::write_query_to_json);
        engine.register_fn("write_json", TemplateWriter::write_query_to_json_w_options);

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

        let query_cache: Arc<RwLock<HashMap<String, Arc<RwLock<QueryHandle>>>>> =
            Default::default();

        engine.register_fn("sql", {
            // let query_cache = query_cache.clone();
            move |query: &str| -> Result<QueryResponse, Box<EvalAltResult>> {
                if let Some(handle) = { query_cache.read().get(query).cloned() } {
                    println!("query already existed...");
                    return Ok(QueryResponse { query: handle });
                }

                let mut w = query_cache.write();
                // double check, for a race (unlikely here, but it is a good idea still)
                if let Some(handle) = { w.get(query).cloned() } {
                    println!("query already existed...");
                    return Ok(QueryResponse { query: handle });
                }

                println!("making a brand new query!");

                let (query_id, body) = tokio::runtime::Handle::current()
                    .block_on(client.query(&corro_types::api::Query::Options {
                        statement: Statement::Simple(query.into()),
                        watch: true,
                    }))
                    .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?;

                let query_id = match query_id {
                    Some(query_id) => query_id,
                    None => {
                        return Err(Box::new(EvalAltResult::from("malformed corrosion response: expected a query id in a response header")));
                    }
                };

                let handle = Arc::new(RwLock::new(QueryHandle {
                    query_id,
                    body: Some(body),
                    client: client.clone(),
                }));

                w.insert(query.to_string(), handle.clone());

                Ok(QueryResponse { query: handle })
            }
        });

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
                    program.push_str("__tpl_writer.write(`${");
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
    pub fn render(&self, w: TemplateWriter) -> Result<(), Box<rhai::EvalAltResult>> {
        let mut scope = Scope::new();
        scope.push("__tpl_writer", w);
        self.engine
            .engine
            .eval_ast_with_scope(&mut scope, &self.ast)
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
        let (tripwire, _trip_worker, _trip_sender) = Tripwire::new_simple();

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

        let engine = Engine::new(client);

        let input = r#"<%
let a = [42, 123, 999, 0, true, "hello", "world!", 987.6543];

// Loop through the array
for (item, count) in a { %>
Item #<%= count + 1 %> = <%= item %>
<% } %>

<%= hostname() %>

tail"#;

        let tpl = engine.compile(input).unwrap();
        tpl.render(TemplateWriter::new(f)).unwrap();

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
        let (tripwire, _trip_worker, _trip_sender) = Tripwire::new_simple();

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

                let engine = Engine::new(client);

                // loop {
                let cmd = cmd_rx.recv().await.unwrap();
                assert!(matches!(Command::Render, cmd));

                block_in_place(|| {
                    let input = r#"<%= write_json(sql("select * from tests"), #{pretty: true}) %>
<%= write_json(sql("select * from tests"), #{pretty: true}) %>"#;

                    let tpl = engine.compile(input).unwrap();
                    tpl.render(TemplateWriter::new(f)).unwrap();
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
