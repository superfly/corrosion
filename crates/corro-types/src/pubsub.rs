use std::{collections::HashMap, fmt, net::SocketAddr, ops::Deref, str::FromStr, sync::Arc};

use compact_str::CompactString;
use parking_lot::RwLock;
use serde::{
    de::{self, Visitor},
    Deserialize, Serialize,
};
use serde_json::Value;
use speedy::{Context, Readable, Writable};
use tokio::sync::mpsc::UnboundedSender;
use uhlc::Timestamp;

use crate::filters::{parse_expr, SupportedExpr};

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum SubscriptionEvent {
    Change(Value),
    Error { error: String },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Subscription {
    Add {
        id: SubscriptionId,
        filter: Option<String>,
        #[serde(default)]
        from_db_version: Option<i64>,
        #[serde(default)]
        broadcast_interest: bool,
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
