use eyre::{bail, Result};
use parking_lot::RwLock;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

pub const DEFAULT_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// A simple bucket histogram modeled after Prometheus's classic histogram.
/// See: https://github.com/prometheus/prometheus/blob/main/promql/quantile.go
pub struct BucketHistogram {
    /// Sorted upper bounds, not including +Inf.
    upper_bounds: Vec<f64>,

    /// Non-cumulative counts. Length = upper_bounds.len() + 1.
    /// buckets[i] counts observations where upper_bounds[i-1] < v <= upper_bounds[i].
    /// buckets[last] is the +Inf bucket (v > upper_bounds[last]).
    buckets: Vec<u64>,

    /// Total number of observations.
    count: u64,

    /// Sum of all observed values.
    sum: f64,
}

impl BucketHistogram {
    pub fn new(upper_bounds: &[f64]) -> Self {
        Self {
            upper_bounds: upper_bounds.to_vec(),
            buckets: vec![0; upper_bounds.len() + 1],
            count: 0,
            sum: 0.0,
        }
    }

    pub fn with_default_buckets() -> Self {
        Self::new(DEFAULT_BUCKETS)
    }

    pub fn observe(&mut self, v: f64) {
        let bucket = self.find_bucket(v);
        self.buckets[bucket] += 1;
        self.count += 1;
        self.sum += v;
    }

    fn find_bucket(&self, v: f64) -> usize {
        for (i, bound) in self.upper_bounds.iter().enumerate() {
            if v <= *bound {
                return i;
            }
        }
        // +Inf bucket
        self.upper_bounds.len()
    }

    pub fn reset(&mut self) {
        self.buckets.iter_mut().for_each(|c| *c = 0);
        self.count = 0;
        self.sum = 0.0;
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn sum(&self) -> f64 {
        self.sum
    }

    #[allow(clippy::manual_range_contains)]
    pub fn quantile(&self, q: f64) -> Option<f64> {
        if q < 0.0 || q > 1.0 {
            return None;
        }

        if self.count == 0 {
            return None;
        }

        let mut cumulative: Vec<f64> = Vec::with_capacity(self.buckets.len());
        let mut running = 0u64;
        for &c in &self.buckets {
            running += c;
            cumulative.push(running as f64);
        }

        let observations = cumulative[cumulative.len() - 1];
        if observations == 0.0 {
            return None;
        }

        let rank = q * observations;

        let b = cumulative
            .iter()
            .position(|&c| c >= rank)
            .unwrap_or(cumulative.len() - 1);

        // Linear interpolation within the bucket.
        let bucket_end = if b < self.upper_bounds.len() {
            self.upper_bounds[b]
        } else {
            return self.upper_bounds.last().copied();
        };

        if b == 0 {
            return Some(self.upper_bounds[0]);
        }

        // Linear interpolation, assuming uniform distribution within the bucket.
        let bucket_start = self.upper_bounds[b - 1];
        let prev_count = cumulative[b - 1];
        let count_in_bucket = cumulative[b] - prev_count;
        let rank_in_bucket = rank - prev_count;

        if count_in_bucket == 0.0 {
            return Some(bucket_start);
        }

        Some(bucket_start + (bucket_end - bucket_start) * (rank_in_bucket / count_in_bucket))
    }

    pub fn merge(histograms: &[&BucketHistogram]) -> Option<BucketHistogram> {
        let first = histograms.first()?;
        let mut merged = BucketHistogram::new(&first.upper_bounds);

        for h in histograms {
            for (i, &c) in h.buckets.iter().enumerate() {
                merged.buckets[i] += c;
            }
            merged.count += h.count;
            merged.sum += h.sum;
        }

        Some(merged)
    }
}

pub struct RollingHistogram {
    /// The histogram chunks. Each covers `chunk_duration` of time.
    chunks: Vec<Chunk>,

    /// Duration each chunk covers (= window / num_chunks).
    chunk_duration: Duration,

    /// The bucket boundaries shared by all chunks.
    _upper_bounds: Vec<f64>,

    /// When this rolling histogram was created. Used to calculate
    /// which chunk index is current.
    created_at: Instant,
}

struct Chunk {
    histogram: BucketHistogram,
    expires_at: Instant,
}

impl RollingHistogram {
    /// Create a new rolling histogram.
    ///
    /// `window` is the total time window (e.g. 5 minutes).
    /// `num_chunks` is how many chunks to split it into (e.g. 5 = one chunk per minute).
    /// `upper_bounds` are the bucket boundaries for each underlying histogram.
    pub fn new(window: Duration, num_chunks: usize, upper_bounds: &[f64]) -> Result<Self> {
        assert!(num_chunks > 0, "num_chunks must be > 0");

        if upper_bounds.is_empty() {
            bail!("upper bounds must be non-empty");
        }

        let mut prev = upper_bounds[0];
        for bound in upper_bounds.iter().skip(1) {
            if *bound <= prev {
                bail!("upper bounds must be in ascending order");
            }
            prev = *bound;
        }

        let chunk_duration = window / num_chunks as u32;
        let now = Instant::now();

        let chunks = (0..num_chunks)
            .map(|i| Chunk {
                histogram: BucketHistogram::new(upper_bounds),
                expires_at: now + chunk_duration * (num_chunks + i) as u32,
            })
            .collect();

        Ok(Self {
            chunks,
            chunk_duration,
            _upper_bounds: upper_bounds.to_vec(),
            created_at: now,
        })
    }

    pub fn observe(&mut self, v: f64) {
        let now = Instant::now();
        let chunk = self.current_chunk_mut(now);
        chunk.histogram.observe(v);
    }

    fn current_chunk_mut(&mut self, now: Instant) -> &mut Chunk {
        let elapsed = now.duration_since(self.created_at);
        let intervals = elapsed.as_millis() / self.chunk_duration.as_millis().max(1);
        let index = (intervals as usize) % self.chunks.len();
        let chunk_len = self.chunks.len();
        let chunk = &mut self.chunks[index];

        // If the chunk has expired, reset it and set a new expiration.
        if now >= chunk.expires_at {
            chunk.histogram.reset();
            chunk.expires_at =
                self.created_at + self.chunk_duration * (intervals as u32 + chunk_len as u32);
        }

        chunk
    }

    pub fn quantile(&self, q: f64) -> Option<f64> {
        let now = Instant::now();
        let active: Vec<&BucketHistogram> = self
            .chunks
            .iter()
            .filter(|c| now < c.expires_at)
            .map(|c| &c.histogram)
            .collect();

        if active.is_empty() {
            return None;
        }

        let refs: Vec<&BucketHistogram> = active.into_iter().collect();
        let merged = BucketHistogram::merge(&refs)?;
        merged.quantile(q)
    }

    pub fn count(&self) -> u64 {
        let now = Instant::now();

        self.chunks
            .iter()
            .filter(|c| now < c.expires_at)
            .map(|c| c.histogram.count())
            .sum()
    }
}

pub struct MetricsTracker(Arc<MetricsTrackerInner>);

pub struct MetricsTrackerInner {
    rolling_histogram: RwLock<RollingHistogram>,
    queue_size: AtomicU64,
}

impl MetricsTracker {
    pub fn new(window: Duration, num_chunks: usize) -> eyre::Result<Self> {
        let rolling_his = RollingHistogram::new(window, num_chunks, DEFAULT_BUCKETS)?;
        Ok(Self(Arc::new(MetricsTrackerInner {
            rolling_histogram: RwLock::new(rolling_his),
            queue_size: AtomicU64::new(0),
        })))
    }

    pub fn observe_lag(&self, lag: f64) {
        self.0.rolling_histogram.write().observe(lag);
    }

    pub fn observe_queue_size(&self, size: u64) {
        self.0.queue_size.store(size, Ordering::Relaxed);
    }

    pub fn quantile_lag(&self, q: f64) -> Option<f64> {
        self.0.rolling_histogram.read().quantile(q)
    }

    pub fn count_lag(&self) -> u64 {
        self.0.rolling_histogram.read().count()
    }

    pub fn queue_size(&self) -> u64 {
        self.0.queue_size.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_observe_and_count() {
        let mut h = BucketHistogram::new(&[0.1, 0.5, 1.0, 5.0, 10.0]);
        for i in [0.05, 0.3, 0.7, 2.0, 15.0] {
            h.observe(i);
        }
        assert_eq!(h.count(), 5);
        assert_eq!(h.buckets, vec![1, 1, 1, 1, 0, 1]);
    }

    #[test]
    fn test_quantile_uniform() {
        // 100 observations evenly spread: 0.01, 0.02, ..., 1.00
        let mut h = BucketHistogram::new(&[0.1, 0.25, 0.5, 1.0]);
        for i in 1..=100 {
            h.observe(i as f64 / 100.0);
        }

        let p50 = h.quantile(0.5).unwrap();
        let p99 = h.quantile(0.99).unwrap();

        // p50 should be around 0.5, p99 should be around 0.99
        // Exact values depend on bucket interpolation
        assert!(p50 > 0.4 && p50 < 0.6, "p50 was {}", p50);
        assert!(p99 > 0.9 && p99 <= 1.0, "p99 was {}", p99);
    }

    #[test]
    fn test_quantile_empty() {
        let h = BucketHistogram::new(&[1.0, 5.0, 10.0]);
        assert_eq!(h.quantile(0.99), None);
    }

    #[test]
    fn test_reset() {
        let mut h = BucketHistogram::new(&[1.0, 5.0]);
        h.observe(0.5);
        h.observe(3.0);
        assert_eq!(h.count(), 2);

        h.reset();
        assert_eq!(h.count(), 0);
        assert_eq!(h.sum(), 0.0);
        assert_eq!(h.buckets, vec![0, 0, 0]);
    }

    #[test]
    fn test_merge() {
        let mut h1 = BucketHistogram::new(&[1.0, 5.0, 10.0]);
        let mut h2 = BucketHistogram::new(&[1.0, 5.0, 10.0]);
        h1.observe(0.5);
        h1.observe(3.0);
        h2.observe(0.7);
        h2.observe(7.0);

        let merged = BucketHistogram::merge(&[&h1, &h2]).unwrap();
        assert_eq!(merged.count(), 4);
        assert_eq!(merged.buckets, vec![2, 1, 1, 0]);
    }
}
