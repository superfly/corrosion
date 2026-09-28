use std::{
    fmt,
    ops::{Add, RangeInclusive, Sub},
};

use rangemap::StepLite;
use rusqlite::{types::FromSql, ToSql};
use serde::{Deserialize, Serialize};
use speedy::{Context, Readable, Writable};

#[macro_export]
macro_rules! dbvr {
    ($start:literal, $end:literal) => {
        $crate::CrsqlDbVersionRange::new(
            $crate::CrsqlDbVersion($start),
            $crate::CrsqlDbVersion($end),
        )
    };
}

#[macro_export]
macro_rules! dbvri {
    ($start:literal, $end:literal) => {
        $crate::CrsqlDbVersion($start)..=$crate::CrsqlDbVersion($end)
    };
}

#[macro_export]
macro_rules! dbsr {
    ($start:literal, $end:literal) => {
        $crate::CrsqlSeqRange::new($crate::CrsqlSeq($start), $crate::CrsqlSeq($end))
    };
}

#[macro_export]
macro_rules! dbsri {
    ($start:literal, $end:literal) => {
        $crate::CrsqlSeq($start)..=$crate::CrsqlSeq($end)
    };
}

#[derive(
    Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct CrsqlDbVersion(pub u64);

impl CrsqlDbVersion {
    #[inline]
    pub fn chunked_iter(
        range: CrsqlDbVersionRange,
        chunk_size: usize,
    ) -> ChunkedCrsqlDbVersionIterator {
        ChunkedCrsqlDbVersionIterator {
            current: range.start_int(),
            end: range.end_int(),
            chunk_size: chunk_size as u64,
        }
    }
}

pub struct ChunkedCrsqlDbVersionIterator {
    current: u64,
    end: u64,
    chunk_size: u64,
}

impl Iterator for ChunkedCrsqlDbVersionIterator {
    type Item = CrsqlDbVersionRange;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.current > self.end {
            return None;
        }

        // Avoid overflows if the end of the range happens to be within chunk_size
        // of u64::MAX
        let next_step = self.current.saturating_add(self.chunk_size);

        let next = CrsqlDbVersionRange::new(
            CrsqlDbVersion(self.current),
            CrsqlDbVersion(next_step.min(self.end)),
        );

        self.current = next_step;

        Some(next)
    }
}

impl StepLite for CrsqlDbVersion {
    fn add_one(&self) -> Self {
        Self(self.0 + 1)
    }

    fn sub_one(&self) -> Self {
        Self(self.0 - 1)
    }
}

impl From<CrsqlDbVersion> for u64 {
    fn from(v: CrsqlDbVersion) -> Self {
        v.0
    }
}

impl ToSql for CrsqlDbVersion {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        self.0.to_sql()
    }
}

impl FromSql for CrsqlDbVersion {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        u64::column_result(value).map(Self)
    }
}

impl Add<u64> for CrsqlDbVersion {
    type Output = Self;

    fn add(self, rhs: u64) -> Self::Output {
        Self(self.0.add(rhs))
    }
}

impl Sub<u64> for CrsqlDbVersion {
    type Output = Self;

    fn sub(self, rhs: u64) -> Self::Output {
        Self(self.0.sub(rhs))
    }
}

impl<'a, C> Readable<'a, C> for CrsqlDbVersion
where
    C: Context,
{
    fn read_from<R: speedy::Reader<'a, C>>(reader: &mut R) -> Result<Self, <C as Context>::Error> {
        u64::read_from(reader).map(Self)
    }
}

impl<C> Writable<C> for CrsqlDbVersion
where
    C: Context,
{
    fn write_to<T: ?Sized + speedy::Writer<C>>(
        &self,
        writer: &mut T,
    ) -> Result<(), <C as Context>::Error> {
        self.0.write_to(writer)
    }
}

impl fmt::Display for CrsqlDbVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(
    Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct CrsqlSeq(pub u64);

impl StepLite for CrsqlSeq {
    fn add_one(&self) -> Self {
        Self(self.0 + 1)
    }

    fn sub_one(&self) -> Self {
        Self(self.0 - 1)
    }
}

impl From<CrsqlSeq> for u64 {
    fn from(v: CrsqlSeq) -> Self {
        v.0
    }
}

impl ToSql for CrsqlSeq {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        self.0.to_sql()
    }
}

impl FromSql for CrsqlSeq {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        u64::column_result(value).map(Self)
    }
}

impl Add<u64> for CrsqlSeq {
    type Output = Self;

    fn add(self, rhs: u64) -> Self::Output {
        Self(self.0.add(rhs))
    }
}

impl Sub<u64> for CrsqlSeq {
    type Output = Self;

    fn sub(self, rhs: u64) -> Self::Output {
        Self(self.0.sub(rhs))
    }
}

impl<'a, C> Readable<'a, C> for CrsqlSeq
where
    C: Context,
{
    fn read_from<R: speedy::Reader<'a, C>>(reader: &mut R) -> Result<Self, <C as Context>::Error> {
        u64::read_from(reader).map(Self)
    }
}

impl<C> Writable<C> for CrsqlSeq
where
    C: Context,
{
    fn write_to<T: ?Sized + speedy::Writer<C>>(
        &self,
        writer: &mut T,
    ) -> Result<(), <C as Context>::Error> {
        self.0.write_to(writer)
    }
}

impl fmt::Display for CrsqlSeq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

macro_rules! range {
    ($inner:ident, $name:ident, $iter:ident) => {
        #[derive(Copy, Clone, PartialEq, Eq, Hash)]
        pub struct $name {
            start: u64,
            end: u64,
            exhausted: bool,
        }

        impl $name {
            /// Equivalent of `(<start>..=<end>).into()`
            #[inline]
            pub fn new(start: $inner, end: $inner) -> Self {
                Self {
                    start: start.0,
                    end: end.0,
                    exhausted: false,
                }
            }

            /// Creates a range that starts and ends at the specified value
            #[inline]
            pub fn single(single: $inner) -> Self {
                Self {
                    start: single.0,
                    end: single.0,
                    exhausted: false,
                }
            }

            /// The typed start of the range
            #[inline]
            pub fn start(&self) -> $inner {
                $inner(self.start)
            }

            /// The typed end of the range
            #[inline]
            pub fn end(&self) -> $inner {
                $inner(self.end)
            }

            /// The u64 at the beginning of the range
            #[inline]
            pub fn start_int(&self) -> u64 {
                self.start
            }

            /// The u64 at the end of the range
            #[inline]
            pub fn end_int(&self) -> u64 {
                self.end
            }

            /// The length of this range, ie the number of values
            /// that would be yielded on iteration
            #[inline]
            pub fn len(&self) -> usize {
                let end = self.end;
                if end >= self.start && !self.exhausted {
                    (end - self.start) as usize + 1
                } else {
                    0
                }
            }

            #[inline]
            pub fn is_empty(&self) -> bool {
                self.len() == 0
            }
        }

        impl From<RangeInclusive<$inner>> for $name {
            #[inline]
            fn from(r: RangeInclusive<$inner>) -> Self {
                let (start, end) = r.into_inner();
                Self {
                    start: start.0,
                    end: end.0,
                    exhausted: false,
                }
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct(stringify!($name))
                    .field("start", &self.start())
                    .field("end", &self.end())
                    .finish()
            }
        }

        impl<'s> From<&'s RangeInclusive<$inner>> for $name {
            #[inline]
            fn from(r: &'s RangeInclusive<$inner>) -> Self {
                Self {
                    start: r.start().0,
                    end: r.end().0,
                    exhausted: false,
                }
            }
        }

        impl From<$name> for RangeInclusive<$inner> {
            #[inline]
            fn from(r: $name) -> Self {
                Self::new(r.start(), r.end())
            }
        }

        impl<'s> From<&'s $name> for RangeInclusive<$inner> {
            #[inline]
            fn from(r: &'s $name) -> Self {
                Self::new(r.start(), r.end())
            }
        }

        impl Iterator for $name {
            type Item = $inner;

            #[inline]
            fn next(&mut self) -> Option<Self::Item> {
                if self.exhausted || self.start > self.end {
                    return None;
                }

                let next = self.start;
                if self.start < self.end {
                    self.start += 1;
                } else {
                    self.exhausted = true;
                }
                Some($inner(next))
            }

            #[inline]
            fn size_hint(&self) -> (usize, Option<usize>) {
                let len = self.len();
                (len, Some(len))
            }
        }

        impl<'a, C: speedy::Context> Readable<'a, C> for $name {
            #[inline]
            fn read_from<R: speedy::Reader<'a, C>>(reader: &mut R) -> Result<Self, C::Error> {
                let start = reader.read_value()?;
                let end: u64 = reader.read_value()?;
                Ok(Self {
                    start,
                    end,
                    exhausted: false,
                })
            }

            #[inline]
            fn minimum_bytes_needed() -> usize {
                16
            }
        }

        impl<C: speedy::Context> Writable<C> for $name {
            #[inline]
            fn write_to<W: ?Sized + speedy::Writer<C>>(
                &self,
                writer: &mut W,
            ) -> Result<(), C::Error> {
                self.start.write_to(writer)?;
                self.end.write_to(writer)
            }

            #[inline]
            fn bytes_needed(&self) -> Result<usize, C::Error> {
                Ok(16)
            }
        }
    };
}

range!(CrsqlDbVersion, CrsqlDbVersionRange, CrsqlDbVersionIter);
range!(CrsqlSeq, CrsqlSeqRange, CrsqlSeqIter);

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn chunked_iter() {
        /// This was the original implementation in corro-agent::api::peer, reproduced
        /// here in stable form to validate the range implementation matches
        fn chunk_range(
            range: RangeInclusive<u64>,
            chunk_size: usize,
        ) -> impl Iterator<Item = RangeInclusive<u64>> {
            let end = *range.end();
            range.step_by(chunk_size).map(move |block_start| {
                let block_end = (block_start + chunk_size as u64).min(end);
                block_start..=block_end
            })
        }

        #[track_caller]
        fn matches_step_by(range: RangeInclusive<u64>, chunk_size: usize) {
            let mut ours = CrsqlDbVersion::chunked_iter(
                CrsqlDbVersionRange::new(
                    CrsqlDbVersion(*range.start()),
                    CrsqlDbVersion(*range.end()),
                ),
                chunk_size,
            );
            let mut stds = chunk_range(range, chunk_size);

            loop {
                match (ours.next(), stds.next()) {
                    (Some(o), Some(s)) => {
                        assert_eq!(o.start_int(), *s.start());
                        assert_eq!(o.end_int(), *s.end());
                    }
                    (None, None) => {
                        break;
                    }
                    (Some(o), None) => {
                        panic!("step_by was done but we still had {o:?}");
                    }
                    (None, Some(s)) => {
                        panic!("we were done but step_by still had {s:?}");
                    }
                }
            }
        }

        // a range is always emitted, even on empty ranges
        {
            matches_step_by(0..=0, 10);
            matches_step_by(1987..=1987, 1);

            // ...unless the start > end
            #[allow(clippy::reversed_empty_ranges)]
            matches_step_by(2..=0, 1);
        }

        // exact
        {
            matches_step_by(1..=10, 11);
            matches_step_by(0..=1, 1);
            matches_step_by(1..=13, 14);
        }

        // remainder
        {
            matches_step_by(2001..=11000, 13);
        }
    }

    #[test]
    fn ranges() {
        let r = dbsr!(1, 1000);
        assert_eq!(r.len(), 1000);

        let ri = RangeInclusive::from(r);
        assert_eq!(ri.start(), &r.start());
        assert_eq!(ri.end(), &r.end());

        for (i, (ours, theirs)) in r.zip(r.start_int()..=r.end_int()).enumerate() {
            assert_eq!(ours.0, theirs, "oops {i}");
        }

        let max =
            CrsqlDbVersionRange::from(CrsqlDbVersion(u64::MAX - 1)..=CrsqlDbVersion(u64::MAX));
        assert_eq!(max.len(), 2);
        assert_eq!(max.into_iter().count(), 2);
    }

    #[test]
    fn ranges_iterator() {
        // if start > end, the range is empty
        let empty = CrsqlDbVersionRange::new(CrsqlDbVersion(1), CrsqlDbVersion(0));
        assert!(empty.into_iter().collect::<Vec<_>>().is_empty());

        let zero = CrsqlDbVersionRange::single(CrsqlDbVersion(0));
        assert_eq!(
            zero.into_iter().collect::<Vec<_>>(),
            vec![CrsqlDbVersion(0)]
        );

        let multiple = CrsqlDbVersionRange::new(CrsqlDbVersion(0), CrsqlDbVersion(3));
        assert_eq!(
            multiple.into_iter().collect::<Vec<_>>(),
            vec![
                CrsqlDbVersion(0),
                CrsqlDbVersion(1),
                CrsqlDbVersion(2),
                CrsqlDbVersion(3)
            ]
        );
    }

    #[test]
    fn serialization() {
        #[track_caller]
        fn speedy(input: CrsqlDbVersionRange) {
            let ser = input.write_to_vec().unwrap();
            let deser = CrsqlDbVersionRange::read_from_buffer(&ser).unwrap();

            assert_eq!(input, deser);
        }

        speedy(CrsqlDbVersionRange::single(CrsqlDbVersion(0)));
        speedy(CrsqlDbVersionRange::new(
            CrsqlDbVersion(0),
            CrsqlDbVersion(u64::MAX),
        ));
        speedy(CrsqlDbVersionRange::new(
            CrsqlDbVersion(110),
            CrsqlDbVersion(0),
        ));
    }
}

/// SQLite varint encoding/decoding, copied from cr-sqlite's `pack_columns.rs`
/// (gorbak/new-clock-tables branch, cr-sqlite 0.18).
///
/// cr-sqlite 0.18 V2 packed format packs `seq` (and other integer columns) as
/// SQLite varints inside a BLOB. To do seq accounting on the Corrosion side
/// we need to decode these varints back into `u64`/`i64` values.
///
/// SQLite varint format (MSB-first / big-endian):
///   1-8 bytes: each byte has 7 data bits (high bit = continuation).
///             The first byte has the most significant bits.
///             The last byte has the least significant 7 bits (no continuation).
///   9 bytes:  p[0..7] have 7 data bits each (all with continuation bit set),
///             p[8] has 8 data bits (the least significant byte, no continuation).
/// Total capacity: 7*8 + 8 = 64 bits.
pub mod varint {
    /// Decode a single SQLite varint from the buffer.
    /// Returns the value and the number of bytes consumed.
    pub fn get(buf: &[u8]) -> Result<(u64, usize), VarintError> {
        if buf.is_empty() {
            return Err(VarintError::Empty);
        }
        let mut result: u64 = 0;
        let mut i = 0;
        while i < buf.len() && i < 9 {
            let byte = buf[i];
            if i == 8 {
                // 9th byte uses all 8 bits
                result = (result << 8) | byte as u64;
                i += 1;
                return Ok((result, i));
            }
            result = (result << 7) | (byte & 0x7F) as u64;
            i += 1;
            if byte & 0x80 == 0 {
                return Ok((result, i));
            }
        }
        // Buffer exhausted before a non-continuation byte was found.
        Err(VarintError::Truncated)
    }

    /// Unpack a blob produced by `crsql_pack_varint_agg` into a `Vec<i64>`.
    /// Format: `[count:varint, ...varint(value_i)]`. Values are reinterpreted
    /// from `u64` to `i64` to recover negative numbers (matching
    /// `crsql_pack_varint_agg_step` which encodes `int64 as u64`).
    pub fn unpack_i64_vec(data: &[u8]) -> Result<Vec<i64>, VarintError> {
        let (count, header_len) = get(data)?;
        let mut buf = &data[header_len..];
        // Cap allocation against remaining buffer — each varint is at least 1 byte,
        // so count can never legitimately exceed buf.len().
        let cap = (count as usize).min(buf.len());
        let mut out = Vec::with_capacity(cap);
        for _ in 0..count {
            let (val, n) = get(buf)?;
            out.push(val as i64);
            buf = &buf[n..];
        }
        Ok(out)
    }

    /// Unpack a blob produced by `crsql_pack_varint_agg` into a `Vec<u64>`.
    /// Format: `[count:varint, ...varint(value_i)]`.
    pub fn unpack_u64_vec(data: &[u8]) -> Result<Vec<u64>, VarintError> {
        let (count, header_len) = get(data)?;
        let mut buf = &data[header_len..];
        let cap = (count as usize).min(buf.len());
        let mut out = Vec::with_capacity(cap);
        for _ in 0..count {
            let (val, n) = get(buf)?;
            out.push(val);
            buf = &buf[n..];
        }
        Ok(out)
    }

    #[derive(Debug, thiserror::Error)]
    pub enum VarintError {
        #[error("empty buffer")]
        Empty,
        #[error("truncated varint (no terminating byte)")]
        Truncated,
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_varint_single_byte() {
            for v in 0..0x80u64 {
                // single-byte varints are just the value itself
                let buf = [v as u8];
                let (decoded, n) = get(&buf).unwrap();
                assert_eq!(decoded, v);
                assert_eq!(n, 1);
            }
        }

        #[test]
        fn test_varint_two_bytes() {
            // 128 = 0x81 0x00, 200 = 0x81 0x48, 16383 = 0xFF 0x7F
            let cases: &[(u64, &[u8])] = &[
                (128, &[0x81, 0x00]),
                (200, &[0x81, 0x48]),
                (16383, &[0xFF, 0x7F]),
            ];
            for &(val, expected) in cases {
                let (decoded, n) = get(expected).unwrap();
                assert_eq!(decoded, val);
                assert_eq!(n, expected.len());
            }
        }

        #[test]
        fn test_varint_unpack_vec() {
            // pack 3 varints: count=3, then 0, 127, 200
            let mut data = vec![3u8]; // count
            data.extend_from_slice(&[0]); // 0
            data.extend_from_slice(&[127]); // 127
            data.extend_from_slice(&[0x81, 0x48]); // 200
            let vals = unpack_u64_vec(&data).unwrap();
            assert_eq!(vals, vec![0, 127, 200]);
        }

        #[test]
        fn test_varint_empty() {
            assert!(matches!(get(&[]), Err(VarintError::Empty)));
        }
    }
}
