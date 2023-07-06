use std::{
    collections::{HashMap, HashSet},
    fmt,
    net::SocketAddr,
    ops::Deref,
    str::FromStr,
    sync::Arc,
};

use compact_str::CompactString;
use fallible_iterator::FallibleIterator;
use parking_lot::RwLock;
use rusqlite::Connection;
use serde::{
    de::{self, Visitor},
    Deserialize, Serialize,
};
use speedy::{Context, Readable, Writable};
use sqlite3_parser::{
    ast::{As, Cmd, Expr, Id, Name, OneSelect, Operator, SelectTable, Stmt},
    lexer::sql::Parser,
};
use tokio::sync::mpsc::UnboundedSender;
use tracing::trace;
use uhlc::Timestamp;

use crate::{
    filters::{parse_expr, AggregateChange, OwnedAggregateChange, SupportedExpr},
    schema::{NormalizedSchema, NormalizedTable},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub enum SubscriberId {
    Local { addr: SocketAddr },
    Global,
}

impl fmt::Display for SubscriberId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SubscriberId::Local { addr } => addr.fmt(f),
            SubscriberId::Global => f.write_str("global"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SubscriptionId(pub CompactString);

impl SubscriptionId {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for SubscriptionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<'a, C: Context> Readable<'a, C> for SubscriptionId {
    fn read_from<R: speedy::Reader<'a, C>>(reader: &mut R) -> Result<Self, <C as Context>::Error> {
        let s = <&str as Readable<'a, C>>::read_from(reader)?;
        Ok(Self(s.into()))
    }
}

impl<'a, C: Context> Writable<C> for SubscriptionId {
    fn write_to<T: ?Sized + speedy::Writer<C>>(
        &self,
        writer: &mut T,
    ) -> Result<(), <C as Context>::Error> {
        self.0.as_bytes().write_to(writer)
    }
}

#[derive(Debug)]
pub enum Subscriber {
    Local {
        subscriptions: HashMap<SubscriptionId, SubscriptionInfo>,
        sender: UnboundedSender<SubscriptionMessage>,
    },
    Global {
        subscriptions: HashMap<SubscriptionId, SubscriptionInfo>,
    },
}

impl Subscriber {
    pub fn insert(&mut self, id: SubscriptionId, info: SubscriptionInfo) {
        match self {
            Subscriber::Local { subscriptions, .. } => subscriptions,
            Subscriber::Global { subscriptions } => subscriptions,
        }
        .insert(id, info);
    }

    pub fn remove(&mut self, id: &SubscriptionId) -> Option<SubscriptionInfo> {
        match self {
            Subscriber::Local { subscriptions, .. } => subscriptions,
            Subscriber::Global { subscriptions } => subscriptions,
        }
        .remove(id)
    }

    pub fn as_local(
        &self,
    ) -> Option<(
        &HashMap<SubscriptionId, SubscriptionInfo>,
        &UnboundedSender<SubscriptionMessage>,
    )> {
        match self {
            Subscriber::Local {
                subscriptions,
                sender,
            } => Some((subscriptions, sender)),
            Subscriber::Global { .. } => None,
        }
    }
}

#[derive(Debug)]
pub struct SubscriptionInfo {
    pub filter: Option<SubscriptionFilter>,
    pub updated_at: Timestamp,
}

pub type Subscriptions = Arc<RwLock<Subscriber>>;
pub type Subscribers = Arc<RwLock<HashMap<SubscriberId, Subscriptions>>>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionMessage {
    Event {
        id: SubscriptionId,
        event: SubscriptionEvent,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum SubscriptionEvent {
    Change(OwnedAggregateChange),
    Error { error: String },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Subscription {
    Add {
        id: SubscriptionId,
        where_clause: Option<String>,
        #[serde(default)]
        from_db_version: Option<i64>,
    },
    Remove {
        id: SubscriptionId,
    },
}

#[derive(Debug, Clone)]
pub struct SubscriptionFilter(Arc<String>, Arc<SupportedExpr>);

impl Deref for SubscriptionFilter {
    type Target = SupportedExpr;

    fn deref(&self) -> &Self::Target {
        &self.1
    }
}

impl SubscriptionFilter {
    pub fn new(input: String, expr: SupportedExpr) -> Self {
        Self(Arc::new(input), Arc::new(expr))
    }

    pub fn input(&self) -> &str {
        &self.0
    }
    pub fn expr(&self) -> &SupportedExpr {
        &self.1
    }
}

impl PartialEq for SubscriptionFilter {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for SubscriptionFilter {}

impl Serialize for SubscriptionFilter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for SubscriptionFilter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_string(SubscriptionFilterVisitor)
    }
}

struct SubscriptionFilterVisitor;

impl<'de> Visitor<'de> for SubscriptionFilterVisitor {
    type Value = SubscriptionFilter;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "a string")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_string(s.to_owned())
    }

    fn visit_string<E>(self, s: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        s.parse().map_err(de::Error::custom)
    }
}

impl FromStr for SubscriptionFilter {
    type Err = crate::filters::ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let expr = parse_expr(s)?;
        Ok(SubscriptionFilter::new(s.to_owned(), expr))
    }
}

#[derive(Debug)]
pub struct Matcher {
    pub statements: HashMap<String, String>,
}

impl Matcher {
    pub fn new(schema: &NormalizedSchema, sql: &str) -> Result<Self, MatcherError> {
        let mut parser = Parser::new(sql.as_bytes());

        let mut aliases = HashMap::new();
        let mut tables = HashSet::new();

        let stmt = match parser.next()?.ok_or(MatcherError::StatementRequired)? {
            Cmd::Stmt(stmt) => {
                match stmt {
                    Stmt::Select(ref select) => match select.body.select {
                        OneSelect::Select { ref from, .. } => match from {
                            Some(from) => {
                                match &from.select {
                                    Some(table) => match table.as_ref() {
                                        SelectTable::Table(name, alias, _) => {
                                            if schema.tables.contains_key(name.name.0.as_str()) {
                                                if let Some(As::As(alias) | As::Elided(alias)) =
                                                    alias
                                                {
                                                    aliases.insert(
                                                        name.name.0.clone(),
                                                        alias.0.clone(),
                                                    );
                                                } else if let Some(ref alias) = name.alias {
                                                    aliases.insert(
                                                        name.name.0.clone(),
                                                        alias.0.clone(),
                                                    );
                                                }
                                                tables.insert(name.name.0.clone());
                                            }
                                        }
                                        _ => {}
                                    },
                                    _ => {}
                                }
                                if let Some(ref joins) = from.joins {
                                    for join in joins.iter() {
                                        // let mut tbl_name = None;
                                        match &join.table {
                                            SelectTable::Table(name, alias, _) => {
                                                if let Some(As::As(alias) | As::Elided(alias)) =
                                                    alias
                                                {
                                                    aliases.insert(
                                                        name.name.0.clone(),
                                                        alias.0.clone(),
                                                    );
                                                } else if let Some(ref alias) = name.alias {
                                                    aliases.insert(
                                                        name.name.0.clone(),
                                                        alias.0.clone(),
                                                    );
                                                }
                                                tables.insert(name.name.0.clone());
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                            }
                            _ => {}
                        },
                        _ => {}
                    },
                    _ => return Err(MatcherError::UnsupportedStatement),
                }

                stmt
            }
            _ => return Err(MatcherError::StatementRequired),
        };

        if tables.is_empty() {
            return Err(MatcherError::TableRequired);
        }

        let mut statements = HashMap::new();

        for tbl_name in tables {
            let mut stmt = stmt.clone();

            let expr = table_to_expr(
                &aliases,
                schema
                    .tables
                    .get(&tbl_name)
                    .expect("this should not happen, missing table in schema"),
                &tbl_name,
            )?;

            match &mut stmt {
                Stmt::Select(select) => match &mut select.body.select {
                    OneSelect::Select { where_clause, .. } => {
                        *where_clause = if let Some(prev) = where_clause.take() {
                            Some(Expr::Binary(Box::new(expr), Operator::And, Box::new(prev)))
                        } else {
                            Some(expr)
                        };
                    }
                    _ => {}
                },
                _ => {}
            }

            statements.insert(tbl_name, Cmd::Stmt(stmt).to_string());
        }

        Ok(Self { statements })
    }

    pub fn changed_stmt<'a>(
        &self,
        conn: &'a Connection,
        agg: &AggregateChange,
    ) -> Result<Option<rusqlite::CachedStatement<'a>>, MatcherError> {
        let sql = if let Some(sql) = self.statements.get(agg.table) {
            sql
        } else {
            trace!("irrelevant table!");
            return Ok(None);
        };

        let mut prepped = conn.prepare_cached(sql)?;

        for (i, (_, pk)) in agg.pk.iter().enumerate() {
            prepped.raw_bind_parameter(i + 1, pk.to_owned())?;
        }

        Ok(Some(prepped))
    }
}

fn table_to_expr(
    aliases: &HashMap<String, String>,
    tbl: &NormalizedTable,
    table: &str,
) -> Result<Expr, MatcherError> {
    let tbl_name = aliases
        .get(table)
        .cloned()
        .unwrap_or_else(|| table.to_owned());

    let mut pk_iter = tbl.pk.iter();

    let first = pk_iter
        .next()
        .ok_or_else(|| MatcherError::NoPrimaryKey(tbl_name.clone()))?;

    let mut expr = expr_from_pk(tbl_name.as_str(), first.as_str())
        .ok_or_else(|| MatcherError::AggPrimaryKeyMissing(tbl_name.clone(), first.clone()))?;

    for pk in pk_iter {
        expr = Expr::Binary(
            Box::new(expr),
            Operator::And,
            Box::new(expr_from_pk(tbl_name.as_str(), pk.as_str()).ok_or_else(|| {
                MatcherError::AggPrimaryKeyMissing(tbl_name.clone(), first.clone())
            })?),
        );
    }

    Ok(expr)
}

#[derive(Debug, thiserror::Error)]
pub enum MatcherError {
    #[error(transparent)]
    Lexer(#[from] sqlite3_parser::lexer::sql::Error),
    #[error("one statement is required for matching")]
    StatementRequired,
    #[error("unsupported statement")]
    UnsupportedStatement,
    #[error("at least 1 table is required in FROM / JOIN clause")]
    TableRequired,
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error("table not found in schema: {0}")]
    TableNotFound(String),
    #[error("no primary key for table: {0}")]
    NoPrimaryKey(String),
    #[error("aggregate missing primary key {0}.{1}")]
    AggPrimaryKeyMissing(String, String),
}

fn expr_from_pk(table: &str, pk: &str) -> Option<Expr> {
    Some(Expr::Binary(
        Box::new(Expr::Qualified(Name(table.to_owned()), Name(pk.to_owned()))),
        Operator::Is,
        Box::new(Expr::Id(Id("?".into()))),
    ))
}

#[cfg(test)]
mod tests {
    use crate::{
        actor::ActorId,
        change::{SqliteValue, SqliteValueRef},
        filters::ChangeEvent,
        schema::parse_sql,
    };

    use super::*;

    #[test]
    fn test_diff() {
        let sql = "SELECT json_object('labels',json_object(
            '__metrics_path', JSON_EXTRACT(meta, '$.path'),
            'app_name', app_name,
            'vm_account_id', cs.organization_id,
            'instance', instance_id
          ), 'targets', json_array(address||':'||port)
          ) FROM consul_services AS cs LEFT JOIN machines m ON m.id = cs.instance_id LEFT JOIN machine_versions mv ON m.id = mv.machine_id AND m.machine_version_id = mv.id LEFT JOIN machine_version_statuses mvs ON m.id = mvs.machine_id AND m.machine_version_id = mvs.id WHERE cs.node = 'test-hostname' AND (mvs.status IS NULL OR mvs.status = 'started') AND cs.name == 'app-prometheus'";

        let schema_sql = "
          CREATE TABLE consul_services (
              node TEXT NOT NULL,
              id TEXT NOT NULL,
              name TEXT NOT NULL DEFAULT '',
              tags TEXT NOT NULL DEFAULT '[]',
              meta TEXT NOT NULL DEFAULT '{}',
              port INTEGER NOT NULL DEFAULT 0,
              address TEXT NOT NULL DEFAULT '',
              updated_at INTEGER NOT NULL DEFAULT 0,
              app_id INTEGER AS (CAST(JSON_EXTRACT(meta, '$.app_id') AS INTEGER)), network_id INTEGER AS (
                  CAST(JSON_EXTRACT(meta, '$.network_id') AS INTEGER)
              ), app_name TEXT AS (JSON_EXTRACT(meta, '$.app_name')), instance_id TEXT AS (
                  COALESCE(
                      JSON_EXTRACT(meta, '$.machine_id'),
                      SUBSTR(JSON_EXTRACT(meta, '$.alloc_id'), 1, 8),
                      CASE
                          WHEN INSTR(id, '_nomad-task-') = 1 THEN SUBSTR(id, 13, 8)
                          ELSE NULL
                      END
                  )
              ), organization_id INTEGER AS (
                  CAST(
                      JSON_EXTRACT(meta, '$.organization_id') AS INTEGER
                  )
              ), protocol TEXT
          AS (JSON_EXTRACT(meta, '$.protocol')),
              PRIMARY KEY (node, id)
          );
  
          CREATE TABLE machines (
              id TEXT NOT NULL PRIMARY KEY,
              node TEXT NOT NULL DEFAULT '',
              name TEXT NOT NULL DEFAULT '',
              machine_version_id TEXT NOT NULL DEFAULT '',
              app_id INTEGER NOT NULL DEFAULT 0,
              organization_id INTEGER NOT NULL DEFAULT 0,
              network_id INTEGER NOT NULL DEFAULT 0,
              updated_at INTEGER NOT NULL DEFAULT 0
          );
  
          CREATE TABLE machine_versions (
              machine_id TEXT NOT NULL,
              id TEXT NOT NULL DEFAULT '',
              config TEXT NOT NULL DEFAULT '{}',
              updated_at INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY (machine_id, id)
          );
  
          CREATE TABLE machine_version_statuses (
              machine_id TEXT NOT NULL,
              id TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT '',
              updated_at INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY (machine_id, id)
          );
          ";

        let schema = parse_sql(schema_sql).unwrap();

        let mut conn = rusqlite::Connection::open_in_memory().expect("could not open conn");
        conn.execute_batch(schema_sql)
            .expect("could not exec schema");

        // let's seed some data in there
        {
            let tx = conn.transaction().unwrap();
            tx.execute_batch(r#"
                INSERT INTO consul_services (node, id, name, address, port, meta) VALUES ('test-hostname', 'service-1', 'app-prometheus', '127.0.0.1', 1, '{"path": "/1", "machine_id": "m-1"}');
                INSERT INTO consul_services (node, id, name, address, port, meta) VALUES ('test-hostname', 'service-2', 'not-app-prometheus', '127.0.0.1', 1, '{"path": "/1", "machine_id": "m-2"}');

                INSERT INTO machines (id, machine_version_id) VALUES ('m-1', 'mv-1');
                INSERT INTO machines (id, machine_version_id) VALUES ('m-2', 'mv-2');

                INSERT INTO machine_versions (machine_id, id) VALUES ('m-1', 'mv-1');
                INSERT INTO machine_versions (machine_id, id) VALUES ('m-2', 'mv-2');

                INSERT INTO machine_version_statuses (machine_id, id, status) VALUES ('m-1', 'mv-1', 'started');
                INSERT INTO machine_version_statuses (machine_id, id, status) VALUES ('m-2', 'mv-2', 'started');
            "#).unwrap();
            tx.commit().unwrap();
        }

        let matcher = Matcher::new(&schema, sql).unwrap();

        println!("matcher:\n {matcher:#?}");

        let mut prepped = matcher
            .changed_stmt(
                &conn,
                &AggregateChange {
                    actor_id: ActorId::default(),
                    version: 1,
                    table: "consul_services",
                    pk: vec![
                        ("node", SqliteValueRef::Text("test-hostname")),
                        ("id", SqliteValueRef::Text("service-1")),
                    ]
                    .into_iter()
                    .collect(),
                    evt_type: ChangeEvent::Insert,
                    data: vec![("name", SqliteValueRef::Text("app-prometheus"))]
                        .into_iter()
                        .collect(),
                },
            )
            .unwrap()
            .unwrap();

        let mut rows = prepped.raw_query();

        assert_eq!(rows.next().unwrap().unwrap().get::<_, SqliteValue>(0).unwrap(), SqliteValue::Text(
            "{\"labels\":{\"__metrics_path\":\"/1\",\"app_name\":null,\"vm_account_id\":null,\"instance\":\"m-1\"},\"targets\":[\"127.0.0.1:1\"]}".into(),
        ));

        assert!(rows.next().unwrap().is_none());

        let mut prepped = matcher
            .changed_stmt(
                &conn,
                &AggregateChange {
                    actor_id: ActorId::default(),
                    version: 2,
                    table: "machines",
                    pk: vec![("id", SqliteValueRef::Text("m-1"))]
                        .into_iter()
                        .collect(),
                    evt_type: ChangeEvent::Insert,
                    data: Default::default(),
                },
            )
            .unwrap()
            .unwrap();

        let mut rows = prepped.raw_query();

        assert_eq!(rows.next().unwrap().unwrap().get::<_, SqliteValue>(0).unwrap(), SqliteValue::Text(
                "{\"labels\":{\"__metrics_path\":\"/1\",\"app_name\":null,\"vm_account_id\":null,\"instance\":\"m-1\"},\"targets\":[\"127.0.0.1:1\"]}".into(),
            ));

        assert!(rows.next().unwrap().is_none());

        let mut prepped = matcher
            .changed_stmt(
                &conn,
                &AggregateChange {
                    actor_id: ActorId::default(),
                    version: 3,
                    table: "machines",
                    pk: vec![("id", SqliteValueRef::Text("m-2"))]
                        .into_iter()
                        .collect(),
                    evt_type: ChangeEvent::Insert,
                    data: Default::default(),
                },
            )
            .unwrap()
            .unwrap();

        let mut rows = prepped.raw_query();

        assert!(rows.next().unwrap().is_none());
    }
}
