use std::{fmt, net::SocketAddr, ops::Deref};

use foca::Identity;
use rusqlite::{
    types::{FromSql, FromSqlError, ToSqlOutput},
    ToSql,
};
use serde::{Deserialize, Serialize};
use speedy::{Context, Readable, Reader, Writable, Writer};
use ulid::Ulid;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Deserialize, Serialize)]
#[serde(transparent)]
pub struct ActorId(pub Ulid);

impl ActorId {
    pub fn to_bytes(&self) -> [u8; 16] {
        self.0 .0.to_be_bytes()
    }
}

impl Deref for ActorId {
    type Target = Ulid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

const ULID_SIZE: usize = 16;

impl<'a, C> Readable<'a, C> for ActorId
where
    C: Context,
{
    #[inline]
    fn read_from<R: Reader<'a, C>>(reader: &mut R) -> Result<Self, C::Error> {
        Ok(ActorId(Ulid::from(reader.read_u128()?)))
    }

    #[inline]
    fn minimum_bytes_needed() -> usize {
        ULID_SIZE
    }
}

impl<C> Writable<C> for ActorId
where
    C: Context,
{
    #[inline]
    fn write_to<T: ?Sized + Writer<C>>(&self, writer: &mut T) -> Result<(), C::Error> {
        writer.write_u128(self.0 .0)
    }

    #[inline]
    fn bytes_needed(&self) -> Result<usize, C::Error> {
        Ok(ULID_SIZE)
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
            rusqlite::types::ValueRef::Blob(v) => Ok(ActorId(
                u128::from_be_bytes(v.try_into().map_err(|e| FromSqlError::Other(Box::new(e)))?)
                    .into(),
            )),
            _ => Err(rusqlite::types::FromSqlError::InvalidType),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
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
        Self::new(ActorId(Ulid::nil()), value)
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
