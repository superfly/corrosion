use std::{
    collections::HashMap,
    fmt::{self, Write},
    hash::Hash,
    ops::Deref,
};

use compact_str::{CompactString, ToCompactString};
use rusqlite::{
    types::{FromSql, FromSqlError, ToSqlOutput, Value, ValueRef},
    Row, ToSql,
};
use serde::{Deserialize, Serialize};
use smallvec::{SmallVec, ToSmallVec};
use speedy::{Context, Readable, Reader, Writable, Writer};
use sqlite::ChangeType;

pub mod sqlite;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum QueryEvent {
    Columns(Vec<CompactString>),
    Row(i64, Vec<SqliteValue>),
    #[serde(rename = "eoq")]
    EndOfQuery {
        time: f64,
    },
    Change(ChangeType, i64, Vec<SqliteValue>),
    Error(CompactString),
}

pub type RowIdCells = (i64, Vec<SqliteValue>);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Statement {
    Simple(String),
    WithParams(String, Vec<SqliteValue>),
    WithNamedParams(String, HashMap<String, SqliteValue>),
}

impl From<&str> for Statement {
    fn from(value: &str) -> Self {
        Statement::Simple(value.into())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExecResponse {
    pub results: Vec<ExecResult>,
    pub time: f64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ExecResult {
    Execute { rows_affected: usize, time: f64 },
    Error { error: String },
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, Readable, Writable, PartialEq)]
pub struct Change {
    pub table: TableName,
    pub pk: Vec<u8>,
    pub cid: ColumnName,
    pub val: SqliteValue,
    pub col_version: i64,
    pub db_version: i64,
    pub seq: i64,
    pub site_id: [u8; 16],
    pub cl: i64,
}

impl Change {
    // this is an ESTIMATE, it should give a rough idea of how many bytes will
    // be required on the wire
    pub fn estimated_byte_size(&self) -> usize {
        self.table.len() + self.pk.len() + self.cid.len() + self.val.estimated_byte_size() +
        // col_version
        8 +
        // db_version
        8 +
        // seq
        8 +
        // site_id
        16 +
        // cl
        8
    }
}

pub fn row_to_change(row: &Row) -> Result<Change, rusqlite::Error> {
    Ok(Change {
        table: row.get(0)?,
        pk: row.get(1)?,
        cid: row.get(2)?,
        val: row.get(3)?,
        col_version: row.get(4)?,
        db_version: row.get(5)?,
        seq: row.get(6)?,
        site_id: row.get(7)?,
        cl: row.get(8)?,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum SqliteValueRef<'a> {
    Null,
    Integer(i64),
    Real(f64),
    Text(&'a str),
    Blob(&'a [u8]),
}

impl<'a> SqliteValueRef<'a> {
    pub fn is_null(&self) -> bool {
        matches!(self, SqliteValueRef::Null)
    }

    pub fn as_integer(&self) -> Option<&i64> {
        match self {
            SqliteValueRef::Integer(i) => Some(i),
            _ => None,
        }
    }

    pub fn as_real(&self) -> Option<&f64> {
        match self {
            SqliteValueRef::Real(f) => Some(f),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            SqliteValueRef::Text(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_blob(&self) -> Option<&[u8]> {
        match self {
            SqliteValueRef::Blob(b) => Some(b),
            _ => None,
        }
    }

    pub fn to_owned(&self) -> SqliteValue {
        match self {
            SqliteValueRef::Null => SqliteValue::Null,
            SqliteValueRef::Integer(v) => SqliteValue::Integer(*v),
            SqliteValueRef::Real(v) => SqliteValue::Real(Real(*v)),
            SqliteValueRef::Text(v) => SqliteValue::Text((*v).to_compact_string()),
            SqliteValueRef::Blob(v) => SqliteValue::Blob(v.to_smallvec()),
        }
    }
}

#[derive(PartialEq, Debug)]
pub enum ColumnType {
    Integer = 1,
    Float = 2,
    Text = 3,
    Blob = 4,
    Null = 5,
}

impl ColumnType {
    pub fn from_u8(u: u8) -> Option<Self> {
        Some(match u {
            1 => Self::Integer,
            2 => Self::Float,
            3 => Self::Text,
            4 => Self::Blob,
            5 => Self::Null,
            _ => return None,
        })
    }

    pub fn from_sqlite_name(s: &str) -> Option<Self> {
        Some(match s {
            "INTEGER" => Self::Integer,
            "REAL" => Self::Float,
            "TEXT" => Self::Text,
            "BLOB" => Self::Blob,
            _ => return None,
        })
    }
}

impl FromSql for ColumnType {
    fn column_result(value: ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        match value {
            ValueRef::Text(s) => Ok(match String::from_utf8_lossy(s).as_ref() {
                "INTEGER" => Self::Integer,
                "REAL" => Self::Float,
                "TEXT" => Self::Text,
                "BLOB" => Self::Blob,
                _ => {
                    return Err(FromSqlError::InvalidType);
                }
            }),
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Hash)]
#[serde(untagged)]
pub enum SqliteValue {
    #[default]
    Null,
    Integer(i64),
    Real(Real),
    Text(CompactString),
    Blob(SmallVec<[u8; 512]>),
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(transparent)]
pub struct Real(pub f64);

impl Deref for Real {
    type Target = f64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Hash for Real {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        integer_decode(self.0).hash(state)
    }
}

fn integer_decode(val: f64) -> (u64, i16, i8) {
    let bits: u64 = val.to_bits();
    let sign: i8 = if bits >> 63 == 0 { 1 } else { -1 };
    let mut exponent: i16 = ((bits >> 52) & 0x7ff) as i16;
    let mantissa = if exponent == 0 {
        (bits & 0xfffffffffffff) << 1
    } else {
        (bits & 0xfffffffffffff) | 0x10000000000000
    };

    exponent -= 1023 + 52;
    (mantissa, exponent, sign)
}

impl SqliteValue {
    pub fn column_type(&self) -> ColumnType {
        match self {
            SqliteValue::Null => ColumnType::Null,
            SqliteValue::Integer(_) => ColumnType::Integer,
            SqliteValue::Real(_) => ColumnType::Float,
            SqliteValue::Text(_) => ColumnType::Text,
            SqliteValue::Blob(_) => ColumnType::Blob,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        if let Self::Text(ref s) = self {
            Some(s)
        } else {
            None
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, SqliteValue::Null)
    }

    pub fn as_integer(&self) -> Option<&i64> {
        match self {
            SqliteValue::Integer(i) => Some(i),
            _ => None,
        }
    }

    pub fn as_real(&self) -> Option<&f64> {
        match self {
            SqliteValue::Real(f) => Some(f),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            SqliteValue::Text(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_blob(&self) -> Option<&[u8]> {
        match self {
            SqliteValue::Blob(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_ref(&self) -> SqliteValueRef {
        match self {
            SqliteValue::Null => SqliteValueRef::Null,
            SqliteValue::Integer(i) => SqliteValueRef::Integer(*i),
            SqliteValue::Real(r) => SqliteValueRef::Real(r.0),
            SqliteValue::Text(s) => SqliteValueRef::Text(s.as_str()),
            SqliteValue::Blob(v) => SqliteValueRef::Blob(v.as_slice()),
        }
    }

    pub fn estimated_byte_size(&self) -> usize {
        1 + match self {
            SqliteValue::Null => 1,
            SqliteValue::Integer(_) => 8,
            SqliteValue::Real(_) => 8,
            SqliteValue::Text(t) => 4 + t.len(),
            SqliteValue::Blob(v) => 4 + v.len(),
        }
    }
}

impl From<&str> for SqliteValue {
    fn from(value: &str) -> Self {
        Self::Text(value.into())
    }
}

impl From<Vec<u8>> for SqliteValue {
    fn from(value: Vec<u8>) -> Self {
        Self::Blob(value.into())
    }
}

impl From<String> for SqliteValue {
    fn from(value: String) -> Self {
        Self::Text(value.into())
    }
}

impl From<u16> for SqliteValue {
    fn from(value: u16) -> Self {
        Self::Integer(value as i64)
    }
}

impl From<i64> for SqliteValue {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl FromSql for SqliteValue {
    fn column_result(value: ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        Ok(match value {
            ValueRef::Null => SqliteValue::Null,
            ValueRef::Integer(i) => SqliteValue::Integer(i),
            ValueRef::Real(f) => SqliteValue::Real(Real(f)),
            ValueRef::Text(t) => SqliteValue::Text(
                std::str::from_utf8(t)
                    .map_err(|e| FromSqlError::Other(Box::new(e)))?
                    .into(),
            ),
            ValueRef::Blob(b) => SqliteValue::Blob(b.into()),
        })
    }
}

impl ToSql for SqliteValue {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(match self {
            SqliteValue::Null => ToSqlOutput::Owned(Value::Null),
            SqliteValue::Integer(i) => ToSqlOutput::Owned(Value::Integer(*i)),
            SqliteValue::Real(f) => ToSqlOutput::Owned(Value::Real(f.0)),
            SqliteValue::Text(t) => ToSqlOutput::Borrowed(ValueRef::Text(t.as_bytes())),
            SqliteValue::Blob(b) => ToSqlOutput::Borrowed(ValueRef::Blob(b.as_slice())),
        })
    }
}

impl fmt::Display for SqliteValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqliteValue::Null => f.write_str(""),
            SqliteValue::Integer(v) => v.fmt(f),
            SqliteValue::Real(v) => v.fmt(f),
            SqliteValue::Text(v) => v.fmt(f),
            SqliteValue::Blob(v) => {
                f.write_str("x'")?;
                f.write_str(&hex::encode(v))?;
                f.write_char('\'')
            }
        }
    }
}

impl<'a, C> Readable<'a, C> for SqliteValue
where
    C: Context,
{
    #[inline]
    fn read_from<R: Reader<'a, C>>(reader: &mut R) -> Result<Self, C::Error> {
        Ok(match u8::read_from(reader)? {
            0 => SqliteValue::Null,
            1 => SqliteValue::Integer(i64::read_from(reader)?),
            2 => SqliteValue::Real(Real(f64::read_from(reader)?)),
            3 => {
                let len = reader.read_u32()? as usize;

                SqliteValue::Text(unsafe {
                    CompactString::from_utf8_unchecked(reader.read_vec(len)?)
                })
            }
            4 => {
                let len = reader.read_u32()? as usize;
                let mut vec = SmallVec::with_capacity(len);

                reader.read_bytes(&mut vec)?;

                SqliteValue::Blob(vec)
            }
            _ => return Err(speedy::Error::custom("unknown SqliteValue variant").into()),
        })
    }

    #[inline]
    fn minimum_bytes_needed() -> usize {
        1
    }
}

impl<C> Writable<C> for SqliteValue
where
    C: Context,
{
    #[inline]
    fn write_to<T: ?Sized + Writer<C>>(&self, writer: &mut T) -> Result<(), C::Error> {
        match self {
            SqliteValue::Null => writer.write_u8(0),
            SqliteValue::Integer(i) => {
                1u8.write_to(writer)?;
                i.write_to(writer)
            }
            SqliteValue::Real(f) => {
                2u8.write_to(writer)?;
                f.write_to(writer)
            }
            SqliteValue::Text(s) => {
                3u8.write_to(writer)?;
                s.as_bytes().write_to(writer)
            }
            SqliteValue::Blob(b) => {
                4u8.write_to(writer)?;
                b.as_slice().write_to(writer)
            }
        }
    }

    #[inline]
    fn bytes_needed(&self) -> Result<usize, C::Error> {
        Ok(1 + match self {
            SqliteValue::Null => 0,
            SqliteValue::Integer(i) => <i64 as Writable<C>>::bytes_needed(i)?,
            SqliteValue::Real(f) => <f64 as Writable<C>>::bytes_needed(f)?,
            SqliteValue::Text(s) => <[u8] as Writable<C>>::bytes_needed(s.as_bytes())?,
            SqliteValue::Blob(b) => <[u8] as Writable<C>>::bytes_needed(b.as_slice())?,
        })
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct TableName(pub CompactString);

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct ColumnName(pub CompactString);

impl Deref for TableName {
    type Target = CompactString;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<C> Writable<C> for TableName
where
    C: Context,
{
    #[inline]
    fn write_to<T: ?Sized + Writer<C>>(&self, writer: &mut T) -> Result<(), <C as Context>::Error> {
        self.0.as_str().write_to(writer)
    }

    #[inline]
    fn bytes_needed(&self) -> Result<usize, <C as Context>::Error> {
        Writable::<C>::bytes_needed(self.0.as_str())
    }
}

impl<'a, C> Readable<'a, C> for TableName
where
    C: Context,
{
    #[inline]
    fn read_from<R: Reader<'a, C>>(reader: &mut R) -> Result<Self, <C as Context>::Error> {
        let s: &'a str = Readable::<'a, C>::read_from(reader)?;
        Ok(Self(CompactString::new(s)))
    }
}

impl FromSql for TableName {
    fn column_result(value: ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        Ok(Self(CompactString::new(value.as_str()?)))
    }
}

impl ToSql for TableName {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        self.0.as_str().to_sql()
    }
}

impl Deref for ColumnName {
    type Target = CompactString;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<C> Writable<C> for ColumnName
where
    C: Context,
{
    #[inline]
    fn write_to<T: ?Sized + Writer<C>>(&self, writer: &mut T) -> Result<(), <C as Context>::Error> {
        self.0.as_str().write_to(writer)
    }

    #[inline]
    fn bytes_needed(&self) -> Result<usize, <C as Context>::Error> {
        Writable::<C>::bytes_needed(self.0.as_str())
    }
}

impl<'a, C> Readable<'a, C> for ColumnName
where
    C: Context,
{
    #[inline]
    fn read_from<R: Reader<'a, C>>(reader: &mut R) -> Result<Self, <C as Context>::Error> {
        let s: &'a str = Readable::<'a, C>::read_from(reader)?;
        Ok(Self(CompactString::new(s)))
    }
}

impl FromSql for ColumnName {
    fn column_result(value: ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        Ok(Self(CompactString::new(value.as_str()?)))
    }
}

impl ToSql for ColumnName {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        self.0.as_str().to_sql()
    }
}
