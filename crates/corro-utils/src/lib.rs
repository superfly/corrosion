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

pub struct ThrottleMap<K> {
    inner: HashMap<K, Instant>,
    ttl: Duration,
}

impl<K> ThrottleMap<K>
where
    K: Eq + Hash,
{
    pub fn new(ttl: Duration) -> Self {
        Self {
            inner: HashMap::new(),
            ttl,
        }
    }

    pub fn is_throttled(&self, key: &K) -> bool {
        self.inner
            .get(key)
            .map(|t| Instant::now() < *t)
            .unwrap_or(false)
    }

    pub fn throttle(&mut self, key: K) {
        self.inner.insert(key, Instant::now() + self.ttl);
    }

    pub fn clear_expired(&mut self) {
        let now = Instant::now();
        self.inner.retain(|_, next_retry_at| now > *next_retry_at);
    }

    pub fn contains(&self, key: &K) -> bool {
        self.inner.contains_key(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_throttle_map() {
        let mut throttle_map = ThrottleMap::new(Duration::from_secs(3));
        throttle_map.throttle("throttle-key");
        assert!(throttle_map.is_throttled(&"throttle-key"));
        // sleep for three secs to expire timeout
        std::thread::sleep(Duration::from_secs(3));
        // key should no longer be throttled
        assert!(!throttle_map.is_throttled(&"throttle-key"));
        throttle_map.throttle("new-key");

        // clear expired clears old expired entries
        throttle_map.clear_expired();
        // the old key should be cleared
        assert!(!throttle_map.contains(&"throttle-key"));
        // new key should be present
        assert!(throttle_map.contains(&"new-key"));

        // unknown key should not be throtthled
        assert!(!throttle_map.is_throttled(&"unknown-key"));
    }
}
