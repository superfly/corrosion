use std::collections::HashMap;
use std::io;
use std::io::Write;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use compact_str::CompactString;
use compact_str::ToCompactString;
use corro_client::CorrosionApiClient;
use corro_types::api::QueryEvent;
use corro_types::api::Statement;
use corro_types::change::SqliteValue;
use futures::Stream;
use futures::StreamExt;
use indexmap::IndexMap;
use rhai::Dynamic;
use rhai::{EvalAltResult, Map};
use rhai_tpl::TemplateWriter;
use rhai_tpl::Writer;
use serde::ser::{SerializeSeq, Serializer};
use serde_json::ser::Formatter;
use tokio::sync::OnceCell;
use tokio::sync::{mpsc, RwLock as TokioRwLock};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::error;
use tracing::trace;
use tracing::warn;
use uuid::Uuid;

#[derive(Debug, PartialEq, Clone, Copy, Default, thiserror::Error)]
#[error("parse error")]
pub struct ParseError;

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

    fn to_csv(&mut self) -> SqlToCsv {
        SqlToCsv { res: self.clone() }
    }
}

#[derive(Clone)]
pub struct SqlToCsv {
    res: QueryResponse,
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
            body: OnceCell::new(),
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
    fn to_json(&mut self) -> String {
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
    body: OnceCell<BoxedSubscriptionStream>,
    handle: tokio::runtime::Handle,
    done: bool,
    columns: Option<Arc<Vec<CompactString>>>,
}

impl QueryResponseIter {
    pub async fn body(&mut self) -> Result<&mut BoxedSubscriptionStream, Box<EvalAltResult>> {
        self.body
            .get_or_try_init(|| async {
                match self.query.write().await.body().await {
                    Ok((body, _)) => Ok(body),
                    Err(e) => Err(Box::new(EvalAltResult::from(e.to_string()))),
                }
            })
            .await?;
        self.body.get_mut().ok_or_else(|| {
            Box::new(EvalAltResult::from(
                "unexpected error, body is gone from OnceCell",
            ))
        })
    }

    pub async fn recv(&mut self) -> Option<Result<Row, Box<EvalAltResult>>> {
        if self.done {
            return None;
        }

        loop {
            let res = {
                let body = match self.body().await {
                    Ok(body) => body,
                    Err(e) => {
                        self.done = true;
                        return Some(Err(e));
                    }
                };
                body.next().await
            };
            match res {
                Some(Ok(evt)) => match evt {
                    QueryEvent::Columns(cols) => self.columns = Some(Arc::new(cols)),
                    QueryEvent::EndOfQuery { .. } => {
                        return None;
                    }
                    QueryEvent::Row(rowid, cells) | QueryEvent::Change(_, rowid, cells) => {
                        println!("got a row (rowid: {rowid}) or a change...");
                        match self.columns.as_ref() {
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
                        }
                    }
                    QueryEvent::Error(e) => {
                        self.done = true;
                        return Some(Err(Box::new(EvalAltResult::from(e))));
                    }
                },
                Some(Err(e)) => {
                    self.done = true;
                    return Some(Err(Box::new(EvalAltResult::from(e.to_string()))));
                }
                None => {
                    self.done = true;
                    return None;
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

pub type BoxedSubscriptionStream =
    Pin<Box<dyn Stream<Item = io::Result<QueryEvent>> + Send + Sync + 'static>>;

pub struct QueryHandle {
    id: Uuid,
    body: Option<BoxedSubscriptionStream>,
    client: CorrosionApiClient,
}

impl QueryHandle {
    async fn body(&mut self) -> Result<(BoxedSubscriptionStream, bool), corro_client::Error> {
        if let Some(body) = self.body.take() {
            return Ok((body, true));
        }

        Ok((
            Box::pin(self.client.subscriptioned_query(self.id).await?),
            false,
        ))
    }
}

#[derive(Debug, Clone)]
pub struct TemplateState {
    pub cmd_tx: mpsc::Sender<TemplateCommand>,
    pub cancel: CancellationToken,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TemplateCommand {
    Render,
}

fn write_sql_to_csv<W: Write>(
    tw: &mut TemplateWriter<W, TemplateState>,
    csv: SqlToCsv,
) -> Result<QueryResponseIter, Box<EvalAltResult>> {
    let mut rows = csv.res.into_iter();

    let mut wtr = csv::Writer::from_writer(tw);

    let mut wrote_header = false;

    while let Some(row) = rows.next() {
        let row = row?;
        if !wrote_header {
            wtr.write_record(row.columns.as_slice())
                .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?;
            wrote_header = true;
        }
        wtr.serialize(row.cells.as_slice())
            .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?;
    }
    if !wrote_header {
        if let Some(cols) = rows.columns.as_ref() {
            wtr.write_record(cols.as_slice())
                .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?;
        }
    }

    Ok(rows)
}

fn write_sql_to_json<W: Write>(
    tw: &mut TemplateWriter<W, TemplateState>,
    mut json: SqlToJson,
) -> Result<QueryResponseIter, Box<EvalAltResult>> {
    debug!("write_sql_to_json");
    let mut rows = json.res.into_iter();

    json.json_output.write_rows(tw, &mut rows)?;

    Ok(rows)
}

async fn wait_for_rows(
    mut rows: QueryResponseIter,
    tx: mpsc::Sender<TemplateCommand>,
    cancel: CancellationToken,
) {
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

            const DEBOUNCE_DEADLINE: Duration = Duration::from_millis(100);

            let deadline = sleep(DEBOUNCE_DEADLINE);
            tokio::pin!(deadline);
            loop {
                tokio::select! {
                    None = rows.recv() => {
                        debug!("end of iterator");
                        break;
                    },
                    _ = &mut deadline => {
                        debug!("deadline reached");
                        break;
                    }
                }
            }

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
}

#[derive(Clone)]
pub struct Engine {
    engine: Arc<rhai_tpl::Engine>,
}

impl Engine {
    pub fn new<W: Writer>(client: corro_client::CorrosionApiClient) -> Self {
        let mut engine = rhai_tpl::Engine::new::<W, TemplateState>();

        // custom, efficient, write functions
        engine.register_fn("write", {
            move |tw: &mut TemplateWriter<W, TemplateState>,
                  json: SqlToJson|
                  -> Result<(), Box<EvalAltResult>> {
                let rows = write_sql_to_json(tw, json)?;

                tokio::spawn(wait_for_rows(
                    rows,
                    tw.state.cmd_tx.clone(),
                    tw.state.cancel.clone(),
                ));

                Ok(())
            }
        });

        engine.register_fn(
            "write",
            |tw: &mut TemplateWriter<W, TemplateState>,
             csv: SqlToCsv|
             -> Result<(), Box<EvalAltResult>> {
                let rows = write_sql_to_csv(tw, csv)?;

                tokio::spawn(wait_for_rows(
                    rows,
                    tw.state.cmd_tx.clone(),
                    tw.state.cancel.clone(),
                ));

                Ok(())
            },
        );

        engine.register_type_with_name::<QueryResponse>("QueryResponse");
        engine.register_iterator_result::<QueryResponse, Row>();
        engine.register_fn("to_json", QueryResponse::to_json);
        engine.register_fn("to_json", QueryResponse::to_json_w_options);
        engine.register_fn("to_csv", QueryResponse::to_csv);

        engine.register_type_with_name::<Row>("Row");
        engine.register_iterator::<Row>();

        engine.register_type_with_name::<Cell>("Cell");
        engine.register_fn("name", Cell::name);
        engine.register_fn("value", Cell::value);

        engine.register_type_with_name::<SqliteValueWrap>("SqliteValue");
        engine.register_fn("to_json", SqliteValueWrap::to_json);
        engine.register_fn("to_string", SqliteValueWrap::to_json);

        fn sql(
            client: &CorrosionApiClient,
            stmt: Statement,
        ) -> Result<QueryResponse, Box<EvalAltResult>> {
            debug!("sql function call {stmt:?}");
            let (query_id, body) = tokio::runtime::Handle::current()
                .block_on(client.subscription(&stmt))
                .map_err(|e| Box::new(EvalAltResult::from(e.to_string())))?;

            debug!("got res w/ id: {query_id}");

            let handle = Arc::new(TokioRwLock::new(QueryHandle {
                id: query_id,
                body: Some(Box::pin(body)),
                client: client.clone(),
            }));

            Ok(QueryResponse { query: handle })
        }

        engine.register_fn("sql", {
            let client = client.clone();
            move |query: &str| -> Result<QueryResponse, Box<EvalAltResult>> {
                sql(&client, Statement::Simple(query.into()))
            }
        });

        fn dyn_to_sql(v: Dynamic) -> Result<SqliteValue, Box<EvalAltResult>> {
            Ok(match v.type_name() {
                "()" => SqliteValue::Null,
                "i64" => SqliteValue::Integer(
                    v.as_int()
                        .map_err(|_e| Box::new(EvalAltResult::from("could not cast to i64")))?,
                ),
                "f64" => SqliteValue::Real(
                    v.as_float()
                        .map_err(|_e| Box::new(EvalAltResult::from("could not cast to f64")))?,
                ),
                "bool" => {
                    if v.as_bool()
                        .map_err(|_e| Box::new(EvalAltResult::from("could not cast to bool")))?
                    {
                        SqliteValue::Integer(1)
                    } else {
                        SqliteValue::Integer(0)
                    }
                }
                "blob" => SqliteValue::Blob(
                    v.into_blob()
                        .map_err(|_e| Box::new(EvalAltResult::from("could not cast to blob")))?
                        .into(),
                ),
                // convert everything else into a string, including a string
                _ => SqliteValue::Text(v.to_compact_string()),
            })
        }

        engine.register_fn("sql", {
            let client = client.clone();
            move |query: &str, params: Vec<Dynamic>| -> Result<QueryResponse, Box<EvalAltResult>> {
                let params = params
                    .into_iter()
                    .map(|v| dyn_to_sql(v))
                    .collect::<Result<Vec<_>, _>>()?;
                sql(&client, Statement::WithParams(query.into(), params))
            }
        });

        engine.register_fn(
            "sql",
            move |query: &str, params: Map| -> Result<QueryResponse, Box<EvalAltResult>> {
                let params = params
                    .into_iter()
                    .map(|(k, v)| Ok::<_, Box<EvalAltResult>>((k.to_string(), dyn_to_sql(v)?)))
                    .collect::<Result<HashMap<_, _>, _>>()?;
                sql(&client, Statement::WithNamedParams(query.into(), params))
            },
        );

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

    pub fn compile(&self, input: &str) -> Result<rhai_tpl::Template, rhai_tpl::CompileError> {
        self.engine.compile(input)
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

#[cfg(test)]
mod tests {
    use corro_tests::launch_test_agent;
    use tokio::{
        sync::mpsc::{self, error::TryRecvError},
        task::block_in_place,
    };
    use tripwire::Tripwire;

    use super::*;

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

        let f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&filepath)
            .unwrap();

        let (tx, mut rx) = mpsc::channel(1);

        let engine = Engine::new::<std::fs::File>(client.clone());

        {
            let cancel = CancellationToken::new();
            block_in_place(|| {
                let input = r#"<%= sql("select * from tests").to_json(#{pretty: true}) %>
<%= sql("select * from tests").to_json() %>"#;

                let tpl = engine.compile(input).unwrap();
                tpl.render(
                    f,
                    TemplateState {
                        cmd_tx: tx.clone(),
                        cancel: cancel.clone(),
                    },
                )
                .unwrap();
            });

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
            println!("waiting for first render");
            assert_eq!(rx.recv().await.unwrap(), TemplateCommand::Render);
            println!("waiting for second render");
            assert_eq!(rx.recv().await.unwrap(), TemplateCommand::Render);
            cancel.cancel();
            println!("waiting for third (none) render");
            assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
            println!("renders done!");
        }

        let f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&filepath)
            .unwrap();

        block_in_place(|| {
            let input = r#"<%= sql("select * from tests").to_json(#{pretty: true}) %>
<%= sql("select * from tests").to_json() %>"#;

            let tpl = engine.compile(input).unwrap();
            tpl.render(
                f,
                TemplateState {
                    cmd_tx: tx,
                    cancel: CancellationToken::new(),
                },
            )
            .unwrap();
        });

        let output = std::fs::read_to_string(&filepath).unwrap();

        println!("output: {output}");
    }
}
