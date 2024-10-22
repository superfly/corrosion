use std::{
    cmp, fmt, io,
    num::NonZeroU32,
    ops::{Deref, RangeInclusive},
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use corro_api_types::{row_to_change, Change};
use foca::{Identity, Member, Notification, Runtime, Timer};
use itertools::Itertools;
use metrics::counter;
use rusqlite::{
    types::{FromSql, FromSqlError},
    ToSql,
};
use serde::{Deserialize, Serialize};
use speedy::{Context, Readable, Reader, Writable, Writer};
use time::OffsetDateTime;
use tokio::{
    sync::{mpsc, oneshot},
    task::block_in_place,
};
use tracing::{error, trace};
use uhlc::{ParseNTP64Error, NTP64};

use crate::{
    actor::{Actor, ActorId, ClusterId},
    agent::Agent,
    base::{CrsqlDbVersion, CrsqlSeq, Version},
    change::{ChunkedChanges, MAX_CHANGES_BYTE_SIZE},
    channel::CorroSender,
    sqlite::SqlitePoolError,
    sync::SyncTraceContextV1,
    updates::match_changes,
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
        #[speedy(default_on_eof)]
        ts: Option<Timestamp>,
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
    EmptySet {
        versions: Vec<RangeInclusive<Version>>,
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
            Changeset::Empty { versions, .. } => versions.clone(),
            // todo: this returns dummy version because empty set has an array of versions.
            // probably shouldn't be doing this
            Changeset::EmptySet { .. } => Version(0)..=Version(0),
            Changeset::Full { version, .. } => *version..=*version,
        }
    }

    // determine the estimated resource cost of processing a change
    pub fn processing_cost(&self) -> usize {
        match self {
            Changeset::Empty { versions, .. } => {
                cmp::min((versions.end().0 - versions.start().0) as usize + 1, 20)
            }
            Changeset::EmptySet { versions, .. } => versions
                .iter()
                .map(|versions| cmp::min((versions.end().0 - versions.start().0) as usize + 1, 20))
                .sum::<usize>(),
            Changeset::Full { changes, .. } => changes.len(),
        }
    }

    pub fn max_db_version(&self) -> Option<CrsqlDbVersion> {
        self.changes().iter().map(|c| c.db_version).max()
    }

    pub fn seqs(&self) -> Option<&RangeInclusive<CrsqlSeq>> {
        match self {
            Changeset::Empty { .. } => None,
            Changeset::EmptySet { .. } => None,
            Changeset::Full { seqs, .. } => Some(seqs),
        }
    }

    pub fn last_seq(&self) -> Option<CrsqlSeq> {
        match self {
            Changeset::Empty { .. } => None,
            Changeset::EmptySet { .. } => None,
            Changeset::Full { last_seq, .. } => Some(*last_seq),
        }
    }

    pub fn is_complete(&self) -> bool {
        match self {
            Changeset::Empty { .. } => true,
            Changeset::EmptySet { .. } => true,
            Changeset::Full { seqs, last_seq, .. } => {
                *seqs.start() == CrsqlSeq(0) && seqs.end() == last_seq
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Changeset::Empty { .. } => 0, //(versions.end().0 - versions.start().0 + 1) as usize,
            Changeset::EmptySet { versions, .. } => versions.len(),
            Changeset::Full { changes, .. } => changes.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Changeset::Empty { .. } => true,
            Changeset::EmptySet { .. } => true,
            Changeset::Full { changes, .. } => changes.is_empty(),
        }
    }

    pub fn is_empty_set(&self) -> bool {
        match self {
            Changeset::Empty { .. } => false,
            Changeset::EmptySet { .. } => true,
            Changeset::Full { .. } => false,
        }
    }

    pub fn ts(&self) -> Option<Timestamp> {
        match self {
            Changeset::Empty { ts, .. } => *ts,
            Changeset::EmptySet { ts, .. } => Some(*ts),
            Changeset::Full { ts, .. } => Some(*ts),
        }
    }

    pub fn changes(&self) -> &[Change] {
        match self {
            Changeset::Empty { .. } => &[],
            Changeset::EmptySet { .. } => &[],
            Changeset::Full { changes, .. } => changes,
        }
    }

    pub fn into_parts(self) -> Option<ChangesetParts> {
        match self {
            Changeset::Empty { .. } => None,
            Changeset::EmptySet { .. } => None,
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

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, Eq, PartialOrd, Ord)]
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

// formatting to humantime and then parsing again incurs oddness, so lets compare secs and subsec_nanos
impl PartialEq for Timestamp {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_secs() == other.0.as_secs() && self.0.subsec_nanos() == other.0.subsec_nanos()
    }
}

impl std::hash::Hash for Timestamp {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.as_secs().hash(state);
        self.0.subsec_nanos().hash(state);
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

#[derive(Debug, thiserror::Error)]
pub enum BroadcastError {
    #[error(transparent)]
    Pool(#[from] SqlitePoolError),
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
}

pub async fn broadcast_changes(
    agent: Agent,
    db_version: CrsqlDbVersion,
    last_seq: CrsqlSeq,
    version: Version,
    ts: Timestamp,
) -> Result<(), BroadcastError> {
    let actor_id = agent.actor_id();
    let conn = agent.pool().read().await?;

    block_in_place(|| {
        // TODO: make this more generic so both sync and local changes can use it.
        let mut prepped = conn.prepare_cached(
            r#"
                SELECT "table", pk, cid, val, col_version, db_version, seq, site_id, cl
                    FROM crsql_changes
                    WHERE db_version = ?
                    ORDER BY seq ASC
            "#,
        )?;
        let rows = prepped.query_map([db_version], row_to_change)?;
        let chunked = ChunkedChanges::new(rows, CrsqlSeq(0), last_seq, MAX_CHANGES_BYTE_SIZE);
        for changes_seqs in chunked {
            match changes_seqs {
                Ok((changes, seqs)) => {
                    for (table_name, count) in changes.iter().counts_by(|change| &change.table) {
                        counter!("corro.changes.committed", "table" => table_name.to_string(), "source" => "local").increment(count as u64);
                    }

                    trace!("broadcasting changes: {changes:?} for seq: {seqs:?}");

                    match_changes(agent.subs_manager(), &changes, db_version);
                    match_changes(agent.updates_manager(), &changes, db_version);

                    let tx_bcast = agent.tx_bcast().clone();
                    tokio::spawn(async move {
                        if let Err(e) = tx_bcast
                            .send(BroadcastInput::AddBroadcast(BroadcastV1::Change(
                                ChangeV1 {
                                    actor_id,
                                    changeset: Changeset::Full {
                                        version,
                                        changes,
                                        seqs,
                                        last_seq,
                                        ts,
                                    },
                                },
                            )))
                            .await
                        {
                            error!("could not send change message for broadcast: {e}");
                        }
                    });
                }
                Err(e) => {
                    error!("could not process crsql change (db_version: {db_version}) for broadcast: {e}");
                    break;
                }
            }
        }
        Ok::<_, rusqlite::Error>(())
    })?;

    Ok(())
}
