use std::{
    fmt, io, mem,
    num::NonZeroU32,
    ops::{Deref, RangeInclusive},
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use corro_api_types::Change;
use foca::{Identity, Member, Notification, Runtime, Timer};
use metrics::increment_counter;
use rusqlite::{
    types::{FromSql, FromSqlError},
    ToSql,
};
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use time::OffsetDateTime;
use tokio::sync::mpsc::Sender;
use tracing::{error, trace};
use uhlc::{ParseTimestampError, NTP64};

use crate::{
    actor::{Actor, ActorId},
    sync::SyncStateV1,
};

#[derive(Debug, Clone, Readable, Writable)]
pub enum UniPayload {
    V1(UniPayloadV1),
}

#[derive(Debug, Clone, Readable, Writable)]
pub enum UniPayloadV1 {
    Broadcast(BroadcastV1),
}

#[derive(Debug, Clone, Readable, Writable)]
pub enum BiPayload {
    V1(BiPayloadV1),
}

#[derive(Debug, Clone, Readable, Writable)]
pub enum BiPayloadV1 {
    SyncState(SyncStateV1),
}

#[derive(Debug)]
pub enum FocaInput {
    Announce(Actor),
    Data(Bytes),
    ClusterSize(NonZeroU32),
    ApplyMany(Vec<Member<Actor>>),
}

#[derive(Debug, Clone, Readable, Writable)]
pub enum AuthzV1 {
    Token(String),
}

#[derive(Debug, Clone, Readable, Writable)]
pub enum BroadcastV1 {
    Change(ChangeV1),
}

#[derive(Debug, Clone, Readable, Writable)]
pub struct ChangeV1 {
    pub actor_id: ActorId,
    // internal version
    // pub version: i64,
    pub changeset: Changeset,
}

impl Deref for ChangeV1 {
    type Target = Changeset;

    fn deref(&self) -> &Self::Target {
        &self.changeset
    }
}

#[derive(Debug, Clone, Readable, Writable)]
pub enum Changeset {
    Empty {
        versions: RangeInclusive<i64>,
    },
    Full {
        version: i64,
        changes: Vec<Change>,
        seqs: RangeInclusive<i64>,
        last_seq: i64,
        ts: Timestamp,
    },
}

impl Changeset {
    pub fn versions(&self) -> RangeInclusive<i64> {
        match self {
            Changeset::Empty { versions } => versions.clone(),
            Changeset::Full { version, .. } => *version..=*version,
        }
    }

    pub fn max_db_version(&self) -> Option<i64> {
        self.changes().iter().map(|c| c.db_version).max()
    }

    pub fn seqs(&self) -> Option<&RangeInclusive<i64>> {
        match self {
            Changeset::Empty { .. } => None,
            Changeset::Full { seqs, .. } => Some(seqs),
        }
    }

    pub fn last_seq(&self) -> Option<i64> {
        match self {
            Changeset::Empty { .. } => None,
            Changeset::Full { last_seq, .. } => Some(*last_seq),
        }
    }

    pub fn is_complete(&self) -> bool {
        match self {
            Changeset::Empty { .. } => true,
            Changeset::Full { seqs, last_seq, .. } => *seqs.start() == 0 && seqs.end() == last_seq,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Changeset::Empty { .. } => 0,
            Changeset::Full { changes, .. } => changes.len(),
        }
    }

    pub fn ts(&self) -> Option<Timestamp> {
        match self {
            Changeset::Empty { .. } => None,
            Changeset::Full { ts, .. } => Some(*ts),
        }
    }

    pub fn changes(&self) -> &[Change] {
        match self {
            Changeset::Empty { .. } => &[],
            Changeset::Full { changes, .. } => &changes,
        }
    }

    pub fn into_parts(self) -> Option<(i64, Vec<Change>, RangeInclusive<i64>, i64, Timestamp)> {
        match self {
            Changeset::Empty { .. } => None,
            Changeset::Full {
                version,
                changes,
                seqs,
                last_seq,
                ts,
            } => Some((version, changes, seqs, last_seq, ts)),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TimestampParseError {
    #[error("could not parse timestamp: {0:?}")]
    Parse(ParseTimestampError),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct Timestamp(uhlc::Timestamp);

impl Timestamp {
    pub fn to_time(&self) -> OffsetDateTime {
        let t = self.0.get_time();
        OffsetDateTime::from_unix_timestamp(t.as_secs() as i64).unwrap()
            + time::Duration::nanoseconds(t.subsec_nanos() as i64)
    }
}

impl Deref for Timestamp {
    type Target = uhlc::Timestamp;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<uhlc::Timestamp> for Timestamp {
    fn from(ts: uhlc::Timestamp) -> Self {
        Self(ts)
    }
}

const TS_SIZE: usize = mem::size_of::<u64>() + 16;

impl<'a, C> Readable<'a, C> for Timestamp
where
    C: speedy::Context,
{
    #[inline]
    fn read_from<R: speedy::Reader<'a, C>>(
        reader: &mut R,
    ) -> Result<Self, <C as speedy::Context>::Error> {
        let time: u64 = Readable::read_from(reader)?;
        let id: [u8; 16] = Readable::read_from(reader)?;
        Ok(uhlc::Timestamp::new(
            NTP64(time),
            id.try_into().map_err(|e| speedy::Error::custom(e))?,
        )
        .into())
    }

    #[inline]
    fn minimum_bytes_needed() -> usize {
        TS_SIZE
    }
}

impl<C> Writable<C> for Timestamp
where
    C: speedy::Context,
{
    #[inline]
    fn write_to<T: ?Sized + speedy::Writer<C>>(
        &self,
        writer: &mut T,
    ) -> Result<(), <C as speedy::Context>::Error> {
        self.get_time().0.write_to(writer)?;
        self.get_id().to_le_bytes().write_to(writer)
    }

    #[inline]
    fn bytes_needed(&self) -> Result<usize, C::Error> {
        Ok(TS_SIZE)
    }
}

impl FromSql for Timestamp {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        match value {
            rusqlite::types::ValueRef::Text(b) => match std::str::from_utf8(b) {
                Ok(s) => match s.parse::<uhlc::Timestamp>() {
                    Ok(ts) => Ok(Timestamp(ts)),
                    Err(e) => Err(FromSqlError::Other(Box::new(TimestampParseError::Parse(e)))),
                },
                Err(e) => Err(FromSqlError::Other(Box::new(e))),
            },
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

impl ToSql for Timestamp {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(rusqlite::types::ToSqlOutput::Owned(
            rusqlite::types::Value::Text(self.0.to_string()),
        ))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BroadcastEncodeError {
    #[error(transparent)]
    Encode(#[from] speedy::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum BroadcastDecodeError {
    #[error(transparent)]
    Decode(#[from] speedy::Error),
    #[error("corrupted message, crc mismatch (got: {0}, expected {1})")]
    Corrupted(u32, u32),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("insufficient length received to decode message: {0}")]
    InsufficientLength(usize),
}

#[derive(Debug)]
pub enum BroadcastInput {
    Rebroadcast(BroadcastV1),
    AddBroadcast(BroadcastV1),
}

pub struct DispatchRuntime<T> {
    pub to_send: Sender<(T, Bytes)>,
    pub to_schedule: Sender<(Duration, Timer<T>)>,
    pub notifications: Sender<Notification<T>>,
    pub active: bool,
    pub buf: BytesMut,
}

impl<T: Identity> Runtime<T> for DispatchRuntime<T> {
    fn notify(&mut self, notification: Notification<T>) {
        match &notification {
            Notification::Active => {
                self.active = true;
            }
            Notification::Idle | Notification::Defunct => {
                self.active = false;
            }
            _ => {}
        };
        if let Err(e) = self.notifications.try_send(notification) {
            increment_counter!("corro.channel.error", "type" => "full", "name" => "dispatch.notifications");
            error!("error dispatching notification: {e}");
        }
    }

    fn send_to(&mut self, to: T, data: &[u8]) {
        trace!("cluster send_to {to:?}");
        self.buf.extend_from_slice(data);

        if let Err(e) = self.to_send.try_send((to, self.buf.split().freeze())) {
            increment_counter!("corro.channel.error", "type" => "full", "name" => "dispatch.to_send");
            error!("error dispatching broadcast packet: {e}");
        }
    }

    fn submit_after(&mut self, event: Timer<T>, after: Duration) {
        if let Err(e) = self.to_schedule.try_send((after, event)) {
            increment_counter!("corro.channel.error", "type" => "full", "name" => "dispatch.to_schedule");
            error!("error dispatching scheduled event: {e}");
        }
    }
}

impl<T> DispatchRuntime<T> {
    pub fn new(
        to_send: Sender<(T, Bytes)>,
        to_schedule: Sender<(Duration, Timer<T>)>,
        notifications: Sender<Notification<T>>,
    ) -> Self {
        Self {
            to_send,
            to_schedule,
            notifications,
            active: false,
            buf: BytesMut::new(),
        }
    }
}
