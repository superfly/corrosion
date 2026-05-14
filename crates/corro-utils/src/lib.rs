use std::collections::HashMap;
use std::hash::Hash;
use std::path::Path;
use std::time::{Duration, Instant};

use tracing::warn;

pub async fn read_files_from_paths<P: AsRef<Path>>(
    schema_paths: &[P],
) -> eyre::Result<Vec<String>> {
    let mut contents = vec![];

    for schema_path in schema_paths.iter() {
        match tokio::fs::metadata(schema_path).await {
            Ok(meta) => {
                if meta.is_dir() {
                    match tokio::fs::read_dir(schema_path).await {
                        Ok(mut dir) => {
                            let mut entries = vec![];

                            while let Ok(Some(entry)) = dir.next_entry().await {
                                entries.push(entry);
                            }

                            let mut entries: Vec<_> = entries
                                .into_iter()
                                .filter_map(|entry| {
                                    entry.path().extension().and_then(|ext| {
                                        if ext == "sql" {
                                            Some(entry)
                                        } else {
                                            None
                                        }
                                    })
                                })
                                .collect();

                            entries.sort_by_key(|entry| entry.path());

                            for entry in entries.iter() {
                                match tokio::fs::read_to_string(entry.path()).await {
                                    Ok(s) => {
                                        contents.push(s);
                                    }
                                    Err(e) => {
                                        warn!(
                                            "could not read schema file '{}', error: {e}",
                                            entry.path().display()
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            warn!(
                                "could not read dir '{}', error: {e}",
                                schema_path.as_ref().display()
                            );
                        }
                    }
                } else if meta.is_file() {
                    match tokio::fs::read_to_string(schema_path).await {
                        Ok(s) => {
                            contents.push(s);
                            // pushed.push(schema_path.clone());
                        }
                        Err(e) => {
                            warn!(
                                "could not read schema file '{}', error: {e}",
                                schema_path.as_ref().display()
                            );
                        }
                    }
                }
            }

            Err(e) => {
                warn!(
                    "could not read schema file meta '{}', error: {e}",
                    schema_path.as_ref().display()
                );
            }
        }
    }

    Ok(contents)
}

#[derive(Debug, Clone)]
struct ThrottleEntry {
    blocked_until: Instant,
    throttle_count: u32,
}

/// Per-key exponential throttle: each [`Self:.throttle`] increases the wait from
/// `throttle_min` by powers of two until `throttle_max`. [`Self::retries`] count the number of
/// times the key has been throttled).
#[derive(Debug)]
pub struct ThrottleMap<K> {
    inner: HashMap<K, ThrottleEntry>,
    throttle_min: Duration,
    throttle_max: Duration,

    max_pow: u32,
}

#[inline]
fn exponential_increase(pow: u32, throttle_min: Duration, throttle_max: Duration) -> Duration {
    let mult = 1u32.checked_shl(pow).unwrap_or(u32::MAX);
    (throttle_min * mult).min(throttle_max)
}

#[inline]
fn max_pow(throttle_min: Duration, throttle_max: Duration) -> u32 {
    if throttle_min > throttle_max {
        return 0;
    }
    let ratio = if throttle_min.is_zero() {
        throttle_max.as_nanos()
    } else {
        throttle_max.as_nanos() / throttle_min.as_nanos()
    };
    ratio.ilog2()
}

impl<K> ThrottleMap<K>
where
    K: Eq + Hash,
{
    pub fn new(throttle_min: Duration, throttle_max: Duration) -> Self {
        Self {
            inner: HashMap::new(),
            throttle_min,
            throttle_max,
            max_pow: max_pow(throttle_min, throttle_max),
        }
    }

    pub fn is_throttled(&self, key: &K) -> Option<Instant> {
        if let Some(e) = self.inner.get(key) {
            if Instant::now() < e.blocked_until {
                return Some(e.blocked_until);
            }
        }
    
        None
    }

    pub fn throttle_count(&self, key: &K) -> u64 {
        self.inner
            .get(key)
            .map(|e| e.throttle_count as u64)
            .unwrap_or(0)
    }

    pub fn throttle(&mut self, key: K) {
        let now = Instant::now();
        let entry = self.inner.entry(key).or_insert(ThrottleEntry {
            blocked_until: now,
            throttle_count: 0,
        });

        // we calculate max pow so we can clamp things and not worry about overflow
        let pow = entry.throttle_count.min(self.max_pow);
        let wait = exponential_increase(pow, self.throttle_min, self.throttle_max);

        entry.blocked_until = now + wait;
        entry.throttle_count = entry.throttle_count.saturating_add(1);
    }

    pub fn clear_expired(&mut self) {
        let now = Instant::now();
        self.inner.retain(|_, entry| entry.blocked_until > now);
    }

    pub fn remove(&mut self, key: &K) {
        self.inner.remove(key);
    }

    pub fn contains(&self, key: &K) -> bool {
        self.inner.contains_key(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exponential_increase_doubles_until_cap() {
        let min = Duration::from_secs(5);
        let max = Duration::from_secs(60);

        let max_pow = max_pow(min, max);
        assert_eq!(max_pow, 3);

        assert_eq!(exponential_increase(0, min, max), min);
        assert_eq!(exponential_increase(1, min, max), Duration::from_secs(10));
        assert_eq!(exponential_increase(2, min, max), Duration::from_secs(20));
        assert_eq!(exponential_increase(3, min, max), Duration::from_secs(40));
        assert_eq!(exponential_increase(4, min, max), max);
        assert_eq!(exponential_increase(99, min, max), max);
    }

    #[test]
    fn throttle_map_exponential_backoff_and_counts() {
        let min = Duration::from_millis(40);
        let max = Duration::from_secs(3600);
        let mut m = ThrottleMap::new(min, max);

        m.throttle("a");
        assert_eq!(m.throttle_count(&"a"), 1);
        assert!(m.is_throttled(&"a").is_some());
        assert!(m.is_throttled(&"b").is_none());

        std::thread::sleep(Duration::from_millis(55));
        assert!(m.is_throttled(&"a").is_none());
        assert_eq!(
            m.throttle_count(&"a"),
            1,
            "failure count survives after backoff elapses"
        );

        m.throttle("a");
        assert_eq!(m.throttle_count(&"a"), 2);
        assert!(m.is_throttled(&"a").is_some());
        std::thread::sleep(Duration::from_millis(95));
        assert!(m.is_throttled(&"a").is_none());

        m.remove(&"a");
        assert_eq!(m.throttle_count(&"a"), 0);
        assert!(!m.contains(&"a"));
    }
}
