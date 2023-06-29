use std::{
    fmt, io,
    net::SocketAddr,
    num::NonZeroU32,
    ops::{Deref, RangeInclusive},
    time::Duration,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
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
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};
use tracing::{error, trace};
use uhlc::{ParseNTP64Error, NTP64};

use crate::{
    actor::{Actor, ActorId},
    change::Change,
};

pub const FRAGMENTS_AT: usize = 1420 // wg0 MTU
                              - 40 // 40 bytes IPv6 header
                              - 8; // UDP header bytes
pub const EFFECTIVE_CAP: usize = FRAGMENTS_AT - 1; // fragmentation cap - 1 for the message type byte
pub const HTTP_BROADCAST_SIZE: usize = 64 * 1024;
pub const EFFECTIVE_HTTP_BROADCAST_SIZE: usize = HTTP_BROADCAST_SIZE - 1;

#[derive(Debug)]
pub enum BroadcastSrc {
    Http(SocketAddr),
    Udp(SocketAddr),
}

#[derive(Debug)]
pub enum FocaInput {
    Announce(Actor),
    Data(Bytes, BroadcastSrc),
    ClusterSize(NonZeroU32),
    ApplyMany(Vec<Member<Actor>>),
}

#[derive(Debug, Clone, Readable, Writable)]
pub enum Message {
    V1(MessageV1),
}

#[derive(Debug, Clone, Readable, Writable)]
pub enum MessageV1 {
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
    Parse(ParseNTP64Error),
}

#[derive(
    Debug, Default, Clone, Copy, Serialize, Deserialize, Readable, Writable, PartialEq, Eq,
)]
#[serde(transparent)]
pub struct Timestamp(pub u64);

impl Timestamp {
    pub fn to_time(&self) -> OffsetDateTime {
        let t = NTP64(self.0);
        OffsetDateTime::from_unix_timestamp(t.as_secs() as i64).unwrap()
            + time::Duration::nanoseconds(t.subsec_nanos() as i64)
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        NTP64(self.0).fmt(f)
    }
}

impl From<uhlc::Timestamp> for Timestamp {
    fn from(ts: uhlc::Timestamp) -> Self {
        Self(ts.get_time().as_u64())
    }
}

impl FromSql for Timestamp {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        match value {
            rusqlite::types::ValueRef::Text(b) => match std::str::from_utf8(b) {
                Ok(s) => match s.parse::<NTP64>() {
                    Ok(ntp) => Ok(Timestamp(ntp.as_u64())),
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
            rusqlite::types::Value::Text(NTP64(self.0).to_string()),
        ))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MessageEncodeError {
    #[error(transparent)]
    Encode(#[from] speedy::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum MessageDecodeError {
    #[error(transparent)]
    Decode(#[from] speedy::Error),
    #[error("corrupted message, crc mismatch (got: {0}, expected {1})")]
    Corrupted(u32, u32),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("insufficient length received to decode message: {0}")]
    InsufficientLength(usize),
}

impl Message {
    pub fn from_slice<S: AsRef<[u8]>>(slice: S) -> Result<Self, speedy::Error> {
        Self::read_from_buffer(slice.as_ref())
    }

    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), MessageEncodeError> {
        self.write_to_stream(buf.writer())?;
        let mut bytes = buf.split();
        let hash = crc32fast::hash(&bytes);
        bytes.put_u32(hash);

        let mut codec = LengthDelimitedCodec::builder()
            .length_field_type::<u32>()
            .new_codec();
        codec.encode(bytes.split().freeze(), buf)?;

        Ok(())
    }

    pub fn from_buf(buf: &mut BytesMut) -> Result<Message, MessageDecodeError> {
        let len = buf.len();
        trace!("successfully decoded a frame, len: {len}");

        if len < 4 {
            return Err(MessageDecodeError::InsufficientLength(len));
        }

        let mut crc_bytes = buf.split_off(len - 4);

        let crc = crc_bytes.get_u32();
        let new_crc = crc32fast::hash(&buf);
        if crc != new_crc {
            return Err(MessageDecodeError::Corrupted(crc, new_crc));
        }

        Ok(Message::from_slice(&buf)?)
    }

    pub fn decode(
        codec: &mut LengthDelimitedCodec,
        buf: &mut BytesMut,
    ) -> Result<Option<Self>, MessageDecodeError> {
        Ok(match codec.decode(buf)? {
            Some(mut buf) => Some(Self::from_buf(&mut buf)?),
            None => None,
        })
    }
}

#[derive(Debug)]
pub enum BroadcastInput {
    Rebroadcast(Message),
    AddBroadcast(Message),
}

pub struct DispatchRuntime<T> {
    pub to_send: Sender<(T, Bytes)>,
    pub to_schedule: Sender<(Duration, Timer<T>)>,
    pub notifications: Sender<Notification<T>>,
    pub active: bool,
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
        let packet = data.to_vec();

        if let Err(e) = self.to_send.try_send((to, packet.into())) {
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
        }
    }
}
