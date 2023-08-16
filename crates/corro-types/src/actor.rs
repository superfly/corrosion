use std::{fmt, net::SocketAddr, ops::Deref};

use foca::Identity;
use rusqlite::{
    types::{FromSql, FromSqlError, ToSqlOutput},
    ToSql,
};
use serde::{Deserialize, Serialize};
use speedy::{Context, Readable, Reader, Writable, Writer};
use uuid::Uuid;

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, Hash, Deserialize, Serialize)]
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

impl Deref for ActorId {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

const UUID_SIZE: usize = 16;

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
        Ok(ToSqlOutput::Owned(rusqlite::types::Value::Text(
            self.0.to_string(),
        )))
    }
}

impl FromSql for ActorId {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        match value {
            rusqlite::types::ValueRef::Text(s) => Ok(ActorId(
                String::from_utf8_lossy(s)
                    .parse()
                    .map_err(|e| FromSqlError::Other(Box::new(e)))?,
            )),
            rusqlite::types::ValueRef::Blob(b) => Ok(ActorId(
                Uuid::from_slice(b).map_err(|e| FromSqlError::Other(Box::new(e)))?,
            )),
            _ => Err(rusqlite::types::FromSqlError::InvalidType),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Actor {
    id: ActorId,
    addr: SocketAddr,
    // An extra field to allow fast rejoin
    bump: u16,
}

impl Actor {
    pub fn new(id: ActorId, addr: SocketAddr) -> Self {
        Self {
            id,
            addr,
            bump: rand::random(),
        }
    }

    pub fn id(&self) -> ActorId {
        self.id
    }
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl From<SocketAddr> for Actor {
    fn from(value: SocketAddr) -> Self {
        Self::new(ActorId(Uuid::nil()), value)
    }
}

impl Identity for Actor {
    // Since a client outside the cluster will not be aware of our
    // `bump` field, we implement the optional trait method
    // `has_same_prefix` to allow anyone that knows our `addr`
    // to join our cluster.
    fn has_same_prefix(&self, other: &Self) -> bool {
        // this happens if we're announcing ourselves to another node
        // we don't yet have any info about them, except their gossip addr
        if other.id.is_nil() || self.id.is_nil() {
            self.addr.eq(&other.addr)
        } else {
            self.id.eq(&other.id)
        }
    }

    // And by implementing `renew` we enable automatic rejoining:
    // when another member declares us as down, Foca immediatelly
    // switches to this new identity and rejoins the cluster for us
    fn renew(&self) -> Option<Self> {
        Some(Self {
            id: self.id,
            addr: self.addr,
            bump: self.bump.wrapping_add(1),
        })
    }
}
