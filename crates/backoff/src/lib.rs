use std::{iter, time};

use rand::{thread_rng, Rng};

/// Exponential backoff.
#[derive(Debug, Clone)]
pub struct Backoff {
    retries: u32,
    min: time::Duration,
    max: time::Duration,
    jitter: f32,
    factor: u32,
}

impl Backoff {
    /// Create a new instance.
    #[inline]
    pub fn new(retries: u32) -> Self {
        Self {
            retries,
            min: time::Duration::from_millis(100),
            max: time::Duration::from_secs(10),
            jitter: 0.3,
            factor: 2,
        }
    }

    /// Set the min and max durations.
    #[inline]
    pub fn timeout_range(mut self, min: time::Duration, max: time::Duration) -> Self {
        self.min = min;
        self.max = max;
        self
    }

    /// Set the amount of jitter per backoff.
    ///
    /// ## Panics
    /// This method panics if a number smaller than `0` or larger than `1` is
    /// provided.
    #[inline]
    pub fn jitter(mut self, jitter: f32) -> Self {
        assert!(
            jitter >= 0f32 && jitter <= 1f32,
            "<exponential-backoff>: jitter must be between 0 and 1."
        );
        self.jitter = jitter;
        self
    }

    /// Set the growth factor for each iteration of the backoff.
    #[inline]
    pub fn factor(mut self, factor: u32) -> Self {
        self.factor = factor;
        self
    }

    /// Create an iterator.
    #[inline]
    pub fn iter(self) -> Iter {
        Iter::new(self)
    }
}

/// Immutable iterator.
#[derive(Debug, Clone)]
pub struct Iter {
    inner: Backoff,
    retry_count: u32,
    last_exponent: u32,
}

impl Iter {
    #[inline]
    pub fn new(inner: Backoff) -> Self {
        Self {
            inner,
            retry_count: 0,
            last_exponent: 0,
        }
    }

    pub fn retry_count(&self) -> u32 {
        self.retry_count
    }
}

impl iter::Iterator for Iter {
    type Item = time::Duration;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        // if  {
        if self.inner.retries != 0 && self.retry_count >= self.inner.retries {
            return None;
        }

        // Create exponential duration.
        // Prevent multiply overflow by calling pow every time.
        if self.inner.min * self.last_exponent < self.inner.max {
            self.last_exponent = self.inner.factor.pow(self.retry_count);
        }

        let mut duration = self.inner.min * self.last_exponent;

        self.retry_count += 1;

        // Apply jitter. Uses multiples of 100 to prevent relying on floats.
        let jitter_factor = (self.inner.jitter * 100f32) as u32;
        let random: u32 = thread_rng().gen_range(0..(jitter_factor * 2));
        duration *= 100;
        if random < jitter_factor {
            let jitter = (duration * random) / 100;
            duration -= jitter;
        } else {
            let jitter = (duration * (random / 2)) / 100;
            duration += jitter;
        };
        duration /= 100;

        // Make sure it doesn't exceed upper / lower bounds.
        duration = duration.min(self.inner.max);
        duration = duration.max(self.inner.min);

        Some(duration)
    }
}

impl iter::FusedIterator for Iter {}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_backoff() {
        let boff = Backoff::new(15)
            .timeout_range(Duration::from_millis(10), Duration::from_secs(1))
            .iter();

        let mut total = Duration::default();
        for dur in boff {
            println!("{dur:?}");
            total += dur;
        }
        println!("total: {total:?}");
    }
}
