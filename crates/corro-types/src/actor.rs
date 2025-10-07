use std::{
    fmt,
    hash::Hash,
    net::SocketAddr,
    ops::Deref,
    time::{Duration, SystemTime},
};

use corro_api_types::SqliteValue;
use foca::Identity;
use rusqlite::{
    types::{FromSql, ToSqlOutput},
    ToSql,
};
use serde::{Deserialize, Serialize};
use speedy::{Context, Readable, Reader, Writable, Writer};
use uhlc::NTP64;
use uuid::Uuid;

use crate::broadcast::Timestamp;
use crate::members::MemberState;

#[derive(
    Debug, Default, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize,
)]
#[serde(transparent)]
pub struct ActorId(pub Uuid);

impl ActorId {
    pub fn to_bytes(&self) -> [u8; 16] {
        self.0.into_bytes()
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }
}

impl TryFrom<ActorId> for uhlc::ID {
    type Error = uhlc::SizeError;

    fn try_from(value: ActorId) -> Result<Self, Self::Error> {
        value.as_bytes().try_into()
    }
}

impl Deref for ActorId {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.as_simple().fmt(f)
    }
}

const UUID_SIZE: usize = 16;

#[derive(Debug, thiserror::Error)]
pub enum SqliteValueToActorIdError {
    #[error("sqlite value had wrong type")]
    WrongType,
    #[error("wrong number of bytes, requires exactly 16 bytes")]
    WrongNumberOfBytes,
}

impl TryFrom<&SqliteValue> for ActorId {
    type Error = SqliteValueToActorIdError;

    fn try_from(value: &SqliteValue) -> Result<Self, Self::Error> {
        match value.as_blob() {
            Some(v) => {
                if v.len() != UUID_SIZE {
                    Err(SqliteValueToActorIdError::WrongNumberOfBytes)
                } else {
                    Ok(ActorId::from_bytes(v.try_into().unwrap()))
                }
            }
            None => Err(SqliteValueToActorIdError::WrongType),
        }
    }
}

impl<'a, C> Readable<'a, C> for ActorId
where
    C: Context,
{
    #[inline]
    fn read_from<R: Reader<'a, C>>(reader: &mut R) -> Result<Self, C::Error> {
        Ok(ActorId(Uuid::from_bytes(reader.read_value()?)))
    }

    #[inline]
    fn minimum_bytes_needed() -> usize {
        UUID_SIZE
    }
}

impl<C> Writable<C> for ActorId
where
    C: Context,
{
    #[inline]
    fn write_to<T: ?Sized + Writer<C>>(&self, writer: &mut T) -> Result<(), C::Error> {
        writer.write_bytes(self.0.as_bytes())
    }

    #[inline]
    fn bytes_needed(&self) -> Result<usize, C::Error> {
        Ok(UUID_SIZE)
    }
}

impl ToSql for ActorId {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        self.0.to_sql()
    }
}

impl FromSql for ActorId {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        Ok(Self(FromSql::column_result(value)?))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Actor {
    id: ActorId,
    addr: SocketAddr,
    ts: Timestamp,
    #[serde(default)]
    cluster_id: ClusterId,
    #[serde(default)]
    membership_id: Option<MembershipId>,
}

impl Hash for Actor {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.addr.hash(state);
    }
}

impl Actor {
    pub fn new(
        id: ActorId,
        addr: SocketAddr,
        ts: Timestamp,
        cluster_id: ClusterId,
        membership_id: Option<MembershipId>,
    ) -> Self {
        Self {
            id,
            addr,
            ts,
            cluster_id,
            membership_id,
        }
    }

    pub fn id(&self) -> ActorId {
        self.id
    }
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
    pub fn ts(&self) -> Timestamp {
        self.ts
    }
    pub fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }
    pub fn membership_id(&self) -> Option<MembershipId> {
        self.membership_id
    }

    pub fn to_state(&self) -> MemberState {
        MemberState::new(self.addr, self.ts, self.cluster_id)
    }
}

impl From<SocketAddr> for Actor {
    fn from(value: SocketAddr) -> Self {
        Self::new(
            ActorId(Uuid::nil()),
            value,
            Timestamp::zero(),
            Default::default(),
            None,
        )
    }
}

pub struct ActorAddr {
    id: ActorId,
    addr: SocketAddr,
}

impl PartialEq for ActorAddr {
    fn eq(&self, other: &Self) -> bool {
        // this happens if we're announcing ourselves to another node
        // we don't yet have any info about them, except their gossip addr
        if self.id.is_nil() || other.id.is_nil() {
            self.addr.eq(&other.addr)
        } else {
            self.id.eq(&other.id)
        }
    }
}

impl Identity for Actor {
    type Addr = ActorAddr;

    // Since a client outside the cluster will not be aware of our
    // `bump` field, we implement the optional trait method
    // `has_same_prefix` to allow anyone that knows our `addr`
    // to join our cluster.
    fn addr(&self) -> Self::Addr {
        ActorAddr {
            id: self.id,
            addr: self.addr,
        }
    }

    // And by implementing `renew` we enable automatic rejoining:
    // when another member declares us as down, Foca immediatelly
    // switches to this new identity and rejoins the cluster for us
    fn renew(&self) -> Option<Self> {
        Some(Self {
            id: self.id,
            addr: self.addr,
            ts: NTP64::from(duration_since_epoch()).into(),
            cluster_id: self.cluster_id,
            membership_id: self.membership_id,
        })
    }

    fn win_addr_conflict(&self, adversary: &Self) -> bool {
        self.ts > adversary.ts
    }
}

fn duration_since_epoch() -> Duration {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("could not generate duration since unix epoch")
}

#[derive(
    Debug, Default, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize,
)]
#[serde(transparent)]
pub struct ClusterId(pub u16);

impl fmt::Display for ClusterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<'a, C> Readable<'a, C> for ClusterId
where
    C: Context,
{
    #[inline]
    fn read_from<R: Reader<'a, C>>(reader: &mut R) -> Result<Self, C::Error> {
        Ok(ClusterId(u16::read_from(reader)?))
    }

    #[inline]
    fn minimum_bytes_needed() -> usize {
        2
    }
}

impl<C> Writable<C> for ClusterId
where
    C: Context,
{
    #[inline]
    fn write_to<T: ?Sized + Writer<C>>(&self, writer: &mut T) -> Result<(), C::Error> {
        self.0.write_to(writer)
    }

    #[inline]
    fn bytes_needed(&self) -> Result<usize, C::Error> {
        Writable::<C>::bytes_needed(&self.0)
    }
}

impl ToSql for ClusterId {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        self.0.to_sql()
    }
}

impl FromSql for ClusterId {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        Ok(Self(FromSql::column_result(value)?))
    }
}

#[derive(
    Debug, Default, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize,
)]
#[serde(transparent)]
pub struct MembershipId(pub u16);

impl fmt::Display for MembershipId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<'a, C> Readable<'a, C> for MembershipId
where
    C: Context,
{
    #[inline]
    fn read_from<R: Reader<'a, C>>(reader: &mut R) -> Result<Self, C::Error> {
        Ok(MembershipId(u16::read_from(reader)?))
    }

    #[inline]
    fn minimum_bytes_needed() -> usize {
        2
    }
}

impl<C> Writable<C> for MembershipId
where
    C: Context,
{
    #[inline]
    fn write_to<T: ?Sized + Writer<C>>(&self, writer: &mut T) -> Result<(), C::Error> {
        self.0.write_to(writer)
    }

    #[inline]
    fn bytes_needed(&self) -> Result<usize, C::Error> {
        Writable::<C>::bytes_needed(&self.0)
    }
}

impl ToSql for MembershipId {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        self.0.to_sql()
    }
}

impl FromSql for MembershipId {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        Ok(Self(FromSql::column_result(value)?))
    }
}
