use std::{
    fmt, io,
    num::NonZeroU32,
    ops::{Deref, RangeInclusive},
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use corro_api_types::Change;
use foca::{Identity, Member, Notification, Runtime, Timer};
use metrics::counter;
use rusqlite::{
    types::{FromSql, FromSqlError},
    ToSql,
};
use serde::{Deserialize, Serialize};
use speedy::{Context, Readable, Reader, Writable, Writer};
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, trace};
use uhlc::{ParseNTP64Error, NTP64};

use crate::{
    actor::{Actor, ActorId, ClusterId},
    base::{CrsqlDbVersion, CrsqlSeq, Version},
    channel::CorroSender,
    sync::SyncTraceContextV1,
};

#[derive(Debug, Clone, Readable, Writable)]
pub enum UniPayload {
    V1 {
        data: UniPayloadV1,
        #[speedy(default_on_eof)]
        cluster_id: ClusterId,
    },
}

#[derive(Debug, Clone, Readable, Writable)]
pub enum UniPayloadV1 {
    Broadcast(BroadcastV1),
}

#[derive(Debug, Clone, Readable, Writable)]
pub enum BiPayload {
    V1 {
        data: BiPayloadV1,
        #[speedy(default_on_eof)]
        cluster_id: ClusterId,
    },
}

#[derive(Debug, Clone, Readable, Writable)]
pub enum BiPayloadV1 {
    SyncStart {
        actor_id: ActorId,
        #[speedy(default_on_eof)]
        trace_ctx: SyncTraceContextV1,
    },
}

#[derive(Debug)]
pub enum FocaInput {
    Announce(Actor),
    Data(Bytes),
    ClusterSize(NonZeroU32),
    ApplyMany(Vec<Member<Actor>>),
    Cmd(FocaCmd),
}

#[derive(Debug)]
pub enum FocaCmd {
    Rejoin(oneshot::Sender<Result<(), foca::Error>>),
    MembershipStates(mpsc::Sender<foca::Member<Actor>>),
    ChangeIdentity(Actor, oneshot::Sender<Result<(), foca::Error>>),
}

#[derive(Debug, Clone, Readable, Writable)]
pub enum AuthzV1 {
    Token(String),
}

#[derive(Clone, Debug, Readable, Writable)]
pub enum BroadcastV1 {
    Change(ChangeV1),
}

#[derive(Debug, Clone, Copy, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum ChangeSource {
    Broadcast,
    Sync,
}

// TODO: shrink this by mapping primary keys to integers instead of repeating them
#[derive(Debug, Clone, PartialEq, Readable, Writable)]
pub struct ChangeV1 {
    pub actor_id: ActorId,
    pub changeset: Changeset,
}

impl Deref for ChangeV1 {
    type Target = Changeset;

    fn deref(&self) -> &Self::Target {
        &self.changeset
    }
}

#[derive(Debug, Clone, PartialEq, Readable, Writable)]
pub enum Changeset {
    Empty {
        versions: RangeInclusive<Version>,
    },
    Full {
        version: Version,
        changes: Vec<Change>,
        // cr-sqlite sequences contained in this changeset
        seqs: RangeInclusive<CrsqlSeq>,
        // last cr-sqlite sequence for the complete changeset
        last_seq: CrsqlSeq,
        ts: Timestamp,
    },
}

impl From<ChangesetParts> for Changeset {
    fn from(value: ChangesetParts) -> Self {
        Changeset::Full {
            version: value.version,
            changes: value.changes,
            seqs: value.seqs,
            last_seq: value.last_seq,
            ts: value.ts,
        }
    }
}

pub struct ChangesetParts {
    pub version: Version,
    pub changes: Vec<Change>,
    pub seqs: RangeInclusive<CrsqlSeq>,
    pub last_seq: CrsqlSeq,
    pub ts: Timestamp,
}

impl Changeset {
    pub fn versions(&self) -> RangeInclusive<Version> {
        match self {
            Changeset::Empty { versions } => versions.clone(),
            Changeset::Full { version, .. } => *version..=*version,
        }
    }

    pub fn max_db_version(&self) -> Option<CrsqlDbVersion> {
        self.changes().iter().map(|c| c.db_version).max()
    }

    pub fn seqs(&self) -> Option<&RangeInclusive<CrsqlSeq>> {
        match self {
            Changeset::Empty { .. } => None,
            Changeset::Full { seqs, .. } => Some(seqs),
        }
    }

    pub fn last_seq(&self) -> Option<CrsqlSeq> {
        match self {
            Changeset::Empty { .. } => None,
            Changeset::Full { last_seq, .. } => Some(*last_seq),
        }
    }

    pub fn is_complete(&self) -> bool {
        match self {
            Changeset::Empty { .. } => true,
            Changeset::Full { seqs, last_seq, .. } => {
                *seqs.start() == CrsqlSeq(0) && seqs.end() == last_seq
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Changeset::Empty { .. } => 0,
            Changeset::Full { changes, .. } => changes.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Changeset::Empty { .. } => true,
            Changeset::Full { changes, .. } => changes.is_empty(),
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
            Changeset::Full { changes, .. } => changes,
        }
    }

    pub fn into_parts(self) -> Option<ChangesetParts> {
        match self {
            Changeset::Empty { .. } => None,
            Changeset::Full {
                version,
                changes,
                seqs,
                last_seq,
                ts,
            } => Some(ChangesetParts {
                version,
                changes,
                seqs,
                last_seq,
                ts,
            }),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TimestampParseError {
    #[error("could not parse timestamp: {0:?}")]
    Parse(ParseNTP64Error),
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd)]
#[serde(transparent)]
pub struct Timestamp(pub NTP64);

impl Timestamp {
    pub fn to_time(&self) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(self.0.as_secs() as i64).unwrap()
            + time::Duration::nanoseconds(self.0.subsec_nanos() as i64)
    }

    pub fn to_ntp64(&self) -> NTP64 {
        self.0
    }

    pub fn zero() -> Self {
        Timestamp(NTP64(0))
    }
}

impl Deref for Timestamp {
    type Target = NTP64;

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
        Self(*ts.get_time())
    }
}

impl From<NTP64> for Timestamp {
    fn from(ntp64: NTP64) -> Self {
        Self(ntp64)
    }
}

impl FromSql for Timestamp {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        match value {
            rusqlite::types::ValueRef::Text(b) => match std::str::from_utf8(b) {
                Ok(s) => match s.parse::<NTP64>() {
                    Ok(ntp) => Ok(Timestamp(ntp)),
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

impl<'a, C> Readable<'a, C> for Timestamp
where
    C: Context,
{
    #[inline]
    fn read_from<R: Reader<'a, C>>(reader: &mut R) -> Result<Self, C::Error> {
        Ok(Timestamp(NTP64(u64::read_from(reader)?)))
    }

    #[inline]
    fn minimum_bytes_needed() -> usize {
        std::mem::size_of::<u64>()
    }
}

impl<C> Writable<C> for Timestamp
where
    C: Context,
{
    #[inline]
    fn write_to<T: ?Sized + Writer<C>>(&self, writer: &mut T) -> Result<(), C::Error> {
        self.0 .0.write_to(writer)
    }

    #[inline]
    fn bytes_needed(&self) -> Result<usize, C::Error> {
        <u64 as speedy::Writable<C>>::bytes_needed(&self.0 .0)
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
    pub to_send: CorroSender<(T, Bytes)>,
    pub to_schedule: CorroSender<(Duration, Timer<T>)>,
    pub notifications: CorroSender<Notification<T>>,
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
            counter!("corro.channel.error", "type" => "full", "name" => "dispatch.notifications")
                .increment(1);
            error!("error dispatching notification: {e}");
        }
    }

    fn send_to(&mut self, to: T, data: &[u8]) {
        trace!("cluster send_to {to:?}");
        self.buf.extend_from_slice(data);

        if let Err(e) = self.to_send.try_send((to, self.buf.split().freeze())) {
            counter!("corro.channel.error", "type" => "full", "name" => "dispatch.to_send")
                .increment(1);
            error!("error dispatching broadcast packet: {e}");
        }
    }

    fn submit_after(&mut self, event: Timer<T>, after: Duration) {
        if let Err(e) = self.to_schedule.try_send((after, event)) {
            counter!("corro.channel.error", "type" => "full", "name" => "dispatch.to_schedule")
                .increment(1);
            error!("error dispatching scheduled event: {e}");
        }
    }
}

impl<T> DispatchRuntime<T> {
    pub fn new(
        to_send: CorroSender<(T, Bytes)>,
        to_schedule: CorroSender<(Duration, Timer<T>)>,
        notifications: CorroSender<Notification<T>>,
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
