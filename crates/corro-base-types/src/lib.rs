use std::{
    fmt,
};

use speedy::{Context, Readable, Writable};

pub type CrsqlDbVersion = u64;
pub type CrsqlDbVersionRange = InclusiveRange;

pub type CrsqlSeq = u64;

#[derive(Copy, Clone, PartialEq)]
pub struct InclusiveRange {
    pub start: u64,
    pub end: u64,
}

impl InclusiveRange {
    /// Returns an iterator of ranges with a maximum `chunk_size`
    #[inline]
    pub fn chunked(self, chunk_size: usize) -> InclusiveRangeChunkIter {
        InclusiveRangeChunkIter {
            current: self.start,
            end: self.end,
            chunk_size: chunk_size as u64,
        }
    }

    #[inline]
    pub fn iter(self) -> impl Iterator<Item = u64> {
        std::ops::RangeInclusive::new(self.start, self.end)
    }
}

pub struct InclusiveRangeChunkIter {
    current: u64,
    end: u64,
    chunk_size: u64,
}

impl Iterator for InclusiveRangeChunkIter {
    type Item = InclusiveRange;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current > self.end {
            return None;
        }

        let next_step = self.current + self.chunk_size;

        let next = InclusiveRange {
            start: self.current,
            end: next_step.min(self.end),
        };

        self.current = next_step;

        Some(next)
    }
}

impl From<std::ops::RangeInclusive<u64>> for InclusiveRange {
    #[inline]
    fn from(value: std::ops::RangeInclusive<u64>) -> Self {
        let (start, end) = value.into_inner();
        Self {
            start,
            end,
        }
    }
}

impl From<InclusiveRange> for std::ops::RangeInclusive<u64> {
    #[inline]
    fn from(value: InclusiveRange) -> Self {
        Self::new(value.start, value.end)
    }
}

impl PartialEq<std::ops::RangeInclusive<u64>> for InclusiveRange {
    fn eq(&self, other: &std::ops::RangeInclusive<u64>) -> bool {
        self.start == *other.start() && self.end == *other.end()
    }
}

impl fmt::Debug for InclusiveRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}..={}", self.start, self.end)
    }
}

impl< C: Context> Writable< C > for InclusiveRange {
    #[inline]
    fn write_to< W: ?Sized + speedy::Writer< C > >( &self, writer: &mut W ) -> Result< (), C::Error > {
        self.start.write_to( writer )?;
        self.end.write_to( writer )
    }

    #[inline]
    fn bytes_needed( &self ) -> Result< usize, C::Error > {
        Ok( Writable::< C >::bytes_needed( &self.start )? + Writable::< C >::bytes_needed( &self.end )? )
    }
}

impl< 'a, C: Context > Readable< 'a, C > for InclusiveRange {
    #[inline]
    fn read_from< R: speedy::Reader< 'a, C > >( reader: &mut R ) -> Result< Self, C::Error > {
        let start = reader.read_value()?;
        let end = reader.read_value()?;
        Ok( Self { start, end} )
    }

    #[inline]
    fn minimum_bytes_needed() -> usize {
        <u64 as Readable< 'a, C >>::minimum_bytes_needed() * 2
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn chunked_iter() {
        // a range is always emitted, even on empty ranges
        {
            let r = InclusiveRange {start: 0, end: 0};
            assert_eq!(r.chunked(10).next().unwrap(), 0..=0);

            let r = InclusiveRange {start: 1987, end: 1987};
            assert_eq!(r.chunked(2).next().unwrap(), 1987..=1987);

            // ...unless the start > end
            let r = InclusiveRange {start: 2, end: 0};
            assert!(r.chunked(1).next().is_none());
        }

        // exact
        {
            let r = InclusiveRange {start: 1, end: 10};
            let mut iter = r.chunked(10);
            assert_eq!(iter.next().unwrap(), 1..=10);
            assert!(iter.next().is_none());
        }

        // remainder
        {
            let r = InclusiveRange {start: 2001, end: 11000};
            let mut iter = r.chunked(13);

            let mut full_chunks = (r.end - r.start) / 13;

            while full_chunks > 0 {
                let range = iter.next().unwrap();
                assert_eq!(13, range.end - range.start);
                full_chunks -= 1;
            }

            let last = iter.next().unwrap();
            assert!(last.end - last.start < 13);
        }
    }
}