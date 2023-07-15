use std::io::BufWriter;
use std::io::Seek;
use std::io::Write;
use std::sync::Arc;

use bytes::BytesMut;
use compact_str::CompactString;
use corro_client::CorrosionApiClient;
use corro_types::api::QueryEvent;
use corro_types::api::Statement;
use corro_types::change::SqliteValue;
use futures::StreamExt;
use indexmap::IndexMap;
use logos::Logos;
use parking_lot::RwLock;
use rhai::Dynamic;
use rhai::Scope;
use rhai::{EvalAltResult, Map};
use serde::ser::{SerializeSeq, Serializer};
use serde_json::ser::Formatter;
use tokio::sync::{mpsc, RwLock as TokioRwLock};
use tokio_util::codec::{Decoder, LinesCodec};
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::error;
use tracing::trace;
use tracing::warn;
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
    query: Arc<TokioRwLock<QueryHandle>>,
}

impl QueryResponse {
    fn to_json(&mut self) -> SqlToJson {
        SqlToJson {
            json_output: JsonOutput::default(),
            res: self.clone(),
        }
    }

    fn to_json_w_options(&mut self, options: Map) -> SqlToJson {
        let json_output = {
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

        SqlToJson {
            json_output,
            res: self.clone(),
        }
    }
}

#[derive(Clone)]
pub struct SqlToJson {
    json_output: JsonOutput,
    res: QueryResponse,
}

impl IntoIterator for QueryResponse {
    type Item = Result<Row, Box<EvalAltResult>>;

    type IntoIter = QueryResponseIter;

    fn into_iter(self) -> Self::IntoIter {
        QueryResponseIter {
            query: self.query.clone(),
            body: None,
            original: false,
            codec: LinesCodec::new(),
            buf: BytesMut::new(),
            handle: tokio::runtime::Handle::current(),
            done: false,
            columns: None,
        }
    }
}

#[derive(Clone)]
struct Row {
    #[allow(dead_code)]
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

struct QueryResponseIter {
    query: Arc<TokioRwLock<QueryHandle>>,
    body: Option<hyper::Body>,
    original: bool,
    buf: BytesMut,
    codec: LinesCodec,
    handle: tokio::runtime::Handle,
    done: bool,
    columns: Option<Arc<Vec<CompactString>>>,
}

impl QueryResponseIter {
    pub async fn recv(&mut self) -> Option<Result<Row, Box<EvalAltResult>>> {
        let body = match self.body.as_mut() {
            Some(body) => body,
            None => match self.query.write().await.body().await {
                Ok((body, original)) => {
                    self.body = Some(body);
                    self.original = original;
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
                        QueryEvent::Columns(cols) => self.columns = Some(Arc::new(cols)),
                        QueryEvent::Row { rowid, cells, .. } => match self.columns.as_ref() {
                            Some(columns) => {
                                return Some(Ok(Row {
                                    id: rowid,
                                    columns: columns.clone(),
                                    cells,
                                }));
                            }
                            None => {
                                self.done = true;
                                return Some(Err(Box::new(EvalAltResult::from(
                                    "did not receive columns data",
                                ))));
                            }
                        },
                        QueryEvent::EndOfQuery => {
                            return None;
                        }
                        QueryEvent::Error(e) => {
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
    id: Uuid,
    body: Option<hyper::Body>,
    client: CorrosionApiClient,
}

impl QueryHandle {
    async fn body(&mut self) -> Result<(hyper::Body, bool), corro_client::Error> {
        if let Some(body) = self.body.take() {
            return Ok((body, true));
        }

        Ok((self.client.watched_query(self.id).await?, false))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TemplateCommand {
    Render,
}

pub trait WriteSeek: Write + Seek + Send + Sync + 'static {}
impl<T> WriteSeek for T where T: Write + Seek + Send + Sync + 'static {}

#[derive(Clone)]
pub struct TemplateWriter(Arc<TemplateWriterInner>);

pub struct TemplateWriterInner {
    w: RwLock<Box<dyn WriteSeek>>,
    tx: mpsc::Sender<TemplateCommand>,
    cancel: CancellationToken,
}

impl TemplateWriter {
    pub fn new<W: WriteSeek>(w: W, tx: mpsc::Sender<TemplateCommand>) -> Self {
        let cancel = CancellationToken::new();
        Self(Arc::new(TemplateWriterInner {
            w: RwLock::new(Box::new(BufWriter::new(w))),
            tx,
            cancel,
        }))
    }

    pub fn cancel(&self) {
        self.0.cancel.cancel();
    }

    fn write_str(&mut self, data: &str) -> Result<(), Box<EvalAltResult>> {
        if data.is_empty() {
            return Ok(());
        }
        Ok(self
            .0
            .w
            .write()
            .write_all(data.as_bytes())
            .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?)
    }

    fn write_char(&mut self, data: char) -> Result<(), Box<EvalAltResult>> {
        let mut b = [0; 2];
        let len = data.encode_utf8(&mut b).len();
        Ok(self
            .0
            .w
            .write()
            .write_all(&b[0..len])
            .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?)
    }

    fn write_dynamic(&mut self, d: Dynamic) -> Result<(), Box<EvalAltResult>> {
        Ok(self
            .0
            .w
            .write()
            .write_all(d.to_string().as_bytes())
            .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?)
    }

    fn write_sql_to_json(&mut self, mut json: SqlToJson) -> Result<(), Box<EvalAltResult>> {
        debug!("write_sql_to_json");
        let mut rows = json.res.into_iter();
        {
            let mut w = self.0.w.write();
            json.json_output.write_rows(&mut *w, &mut rows)?;
        }

        let tx = self.0.tx.clone();

        let cancel = self.0.cancel.clone();

        tokio::spawn(async move {
            let row_recv = tokio::select! {
                row_recv = rows.recv() => row_recv,
                _ = cancel.cancelled() => {
                    debug!("template cancellation trigger, returning from tokio task");
                    return
                },
            };

            match row_recv {
                Some(Ok(row)) => {
                    trace!("got an updated row! {:?}", row.cells);
                    if let Err(_e) = tx.send(TemplateCommand::Render).await {
                        debug!("could not send back re-render command, channel must be closed!");
                    }
                    return;
                }
                Some(Err(e)) => {
                    // TODO: need to re-render possibly...
                    warn!("error from upstream, returning... {e}");
                    return;
                }
                None => {
                    debug!("sql stream is done");
                    return;
                }
            }
        });

        Ok(())
    }
}

#[derive(Clone)]
pub struct Engine {
    engine: Arc<rhai::Engine>,
}

impl Engine {
    pub fn new(client: corro_client::CorrosionApiClient) -> Self {
        let mut engine = rhai::Engine::new();

        engine.register_type::<TemplateWriter>();
        engine.register_fn("write", TemplateWriter::write_str);
        engine.register_fn("write", TemplateWriter::write_char);
        engine.register_fn("write", TemplateWriter::write_sql_to_json);
        engine.register_fn("write", TemplateWriter::write_dynamic);

        engine.register_type_with_name::<QueryResponse>("QueryResponse");
        engine.register_iterator_result::<QueryResponse, Row>();
        engine.register_fn("to_json", QueryResponse::to_json);
        engine.register_fn("to_json", QueryResponse::to_json_w_options);

        engine.register_type_with_name::<Row>("Row");
        engine.register_iterator::<Row>();

        engine.register_type_with_name::<Cell>("Cell");
        engine.register_fn("name", Cell::name);
        engine.register_fn("value", Cell::value);

        engine.register_type_with_name::<SqliteValueWrap>("SqliteValue");
        engine.register_fn("to_json", SqliteValueWrap::to_json);
        engine.register_fn("to_string", SqliteValueWrap::to_json);

        engine.register_fn("sql", {
            // let query_cache = query_cache.clone();
            move |query: &str| -> Result<QueryResponse, Box<EvalAltResult>> {
                debug!("sql function call {query}");
                let (query_id, body) = tokio::runtime::Handle::current()
                    .block_on(client.watch(&Statement::Simple(query.into())))
                    .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?;

                debug!("got res w/ id: {query_id}");

                let handle = Arc::new(TokioRwLock::new(QueryHandle {
                    id: query_id,
                    body: Some(body),
                    client: client.clone(),
                }));

                Ok(QueryResponse { query: handle })
            }
        });

        engine.register_fn("hostname", || -> Result<String, Box<EvalAltResult>> {
            Ok(hostname::get()
                .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?
                .to_string_lossy()
                .to_string())
        });

        Self {
            engine: Arc::new(engine),
        }
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

#[derive(Debug, Default, Clone)]
struct JsonOutput {
    pretty: bool,
    row_values_as_array: bool,
}

impl JsonOutput {
    fn write_rows<W: Write>(
        &mut self,
        w: &mut W,
        rows: &mut QueryResponseIter,
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
    rows: &mut QueryResponseIter,
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
    rows: &mut QueryResponseIter,
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
        scope.push("__tpl_writer", w.clone());
        self.engine
            .engine
            .eval_ast_with_scope(&mut scope, &self.ast)?;

        w.0.w
            .write()
            .flush()
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

        let (tx, _rx) = mpsc::channel(1);

        let tpl = engine.compile(input).unwrap();
        tpl.render(TemplateWriter::new(f, tx)).unwrap();

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

        // enum Command {
        //     Render,
        // }

        // enum Reply {
        //     Rendered,
        // }

        // let (cmd_tx, mut cmd_rx) = mpsc::channel(10);
        // let (reply_tx, mut reply_rx) = mpsc::channel(10);

        // tokio::spawn({
        //     let filepath = filepath.clone();
        //     async move {
        let f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&filepath)
            .unwrap();

        let engine = Engine::new(client.clone());

        // loop {
        // let cmd = cmd_rx.recv().await.unwrap();
        // assert!(matches!(cmd, Command::Render));

        let (tx, mut rx) = mpsc::channel(1);

        {
            block_in_place(|| {
                let input = r#"<%= sql("select * from tests").to_json(#{pretty: true}) %>
<%= sql("select * from tests").to_json() %>"#;

                let tpl = engine.compile(input).unwrap();
                tpl.render(TemplateWriter::new(f, tx)).unwrap();
            });
            // reply_tx.send(Reply::Rendered).await.unwrap();
            // }
            // }
            // });

            // cmd_tx.send(Command::Render).await.unwrap();
            // let replied = reply_rx.recv().await.unwrap();
            // assert!(matches!(replied, Reply::Rendered));

            let output = std::fs::read_to_string(&filepath).unwrap();

            println!("output: {output}");

            client
                .execute(&vec![Statement::WithParams(
                    "insert into tests (id, text) values (?,?)".into(),
                    vec!["service-id-3".into(), "service-name-3".into()],
                )])
                .await
                .unwrap();

            // we have 2 queries here...
            assert_eq!(rx.recv().await.unwrap(), TemplateCommand::Render);
            assert_eq!(rx.recv().await.unwrap(), TemplateCommand::Render);
            assert!(rx.recv().await.is_none());
        }

        let f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&filepath)
            .unwrap();

        let (tx, _rx) = mpsc::channel(1);

        block_in_place(|| {
            let input = r#"<%= sql("select * from tests").to_json(#{pretty: true}) %>
<%= sql("select * from tests").to_json() %>"#;

            let tpl = engine.compile(input).unwrap();
            tpl.render(TemplateWriter::new(f, tx)).unwrap();
        });

        let output = std::fs::read_to_string(&filepath).unwrap();

        println!("output: {output}");
    }
}
