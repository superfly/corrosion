use std::{collections::HashMap, fmt, net::SocketAddr, str::FromStr, sync::Arc};

use compact_str::CompactString;
use exprt::{parser::ast::Expr, typecheck::schema::Schema};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use serde::{
    de::{self, Visitor},
    Deserialize, Serialize,
};
use tokio::sync::mpsc::UnboundedSender;
use uhlc::Timestamp;

use crate::filters::parse_expr;

type DisplayFromStr = serde_with::As<serde_with::DisplayFromStr>;
type OptionDisplayFromStr = serde_with::As<Option<serde_with::DisplayFromStr>>;

pub static SCHEMA: Lazy<Schema> = Lazy::new(|| {
    exprt::schema! {
        functions: [],
        fields: [
            app_id: Integer,
            organization_id: Integer,
            network_id: Integer,

            app.network_id: Integer,

            ip_assignment.app_id: Integer,

            consul_service.network_id: Integer,
            consul_service.app_id: Integer,
            consul_service.organization_id: Integer,
            consul_service.node: String,
            consul_service.meta.protocol: String,

            consul_check.node: String,

            node.name: String,
        ]
    }
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct SubscriberId(pub SocketAddr);

impl fmt::Display for SubscriberId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SubscriptionId(pub CompactString);

impl fmt::Display for SubscriptionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug)]
pub struct Subscriber {
    pub subscriptions: HashMap<SubscriptionId, SubscriptionInfo>,
    pub sender: UnboundedSender<SubscriptionMessage>,
}

#[derive(Debug)]
pub struct SubscriptionInfo {
    pub filter: Option<SubscriptionFilter>,
    pub broadcast: bool,
    pub updated_at: Timestamp,
}

pub type Subscriptions = Arc<RwLock<Subscriber>>;
pub type Subscribers = Arc<RwLock<HashMap<SubscriberId, Subscriptions>>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SubscriptionMessage {
    Event {
        id: SubscriptionId,
        event: SubscriptionEvent,
    },
    GoingAway,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(untagged)]
pub enum SubscriptionEvent {
    Change {
        // op: Operation,
        #[serde(with = "DisplayFromStr")]
        id: u128,
    },
    Node {
        // node: Node,
    },
    Error {
        error: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Subscription {
    Add {
        id: SubscriptionId,
        filter: Option<String>,
        #[serde(with = "OptionDisplayFromStr")]
        apply_index: Option<u128>,
        #[serde(default)]
        broadcast_interest: bool,
    },
    Remove {
        id: SubscriptionId,
    },
}

#[derive(Debug, Clone)]
pub struct SubscriptionFilter(Arc<String>, Arc<Expr>);

impl SubscriptionFilter {
    pub fn new(input: String, expr: Expr) -> Self {
        Self(Arc::new(input), Arc::new(expr))
    }

    pub fn input(&self) -> &str {
        &self.0
    }
    pub fn expr(&self) -> &Expr {
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
