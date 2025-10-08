use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use lazy_static::lazy_static;
use metrics::Gauge;
use tokio::time::sleep;

lazy_static! {
    pub static ref PERSISTENT_GAUGE_REGISTRY: GaugeRegistry = GaugeRegistry {
        gauges: Arc::new(Mutex::new(HashMap::new())),
    };
}

struct StoredGauge {
    gauge: Arc<Gauge>,
    persistent: bool,
}

#[derive(Clone)]
pub struct GaugeRegistry {
    gauges: Arc<Mutex<HashMap<u64, StoredGauge>>>,
}

#[derive(Clone)]
pub struct PersistentGauge {
    gauge: Arc<Gauge>,
}

impl PersistentGauge {
    pub fn increment(&self, value: f64) {
        self.gauge.increment(value);
    }

    pub fn decrement(&self, value: f64) {
        self.gauge.decrement(value);
    }

    pub fn set(&self, value: f64) {
        self.gauge.set(value);
    }
}

impl GaugeRegistry {
    pub fn register(&self, gauge: Gauge, key: u64, persistent: bool) -> PersistentGauge {
        let mut guard = self.gauges.lock().unwrap();
        let stored_gauge = guard.entry(key).or_insert(StoredGauge {
            persistent,
            gauge: Arc::new(gauge),
        });
        PersistentGauge {
            gauge: stored_gauge.gauge.clone(),
        }
    }

    // If an metric doesn't get updated for idle_timeout
    // It will stop getting send and old references to gauges will stop working
    // https://github.com/superfly/corrosion/blob/dbb3fc41a0295eb79a207d2938ec56e26c389410/crates/corrosion/src/command/agent.rs#L117
    pub fn spawn_handle_update(self) {
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(30)).await;
                let mut guard = self.gauges.lock().unwrap();
                let mut to_remove = vec![];
                for (key, stored_gauge) in guard.iter_mut() {
                    if !stored_gauge.persistent && Arc::strong_count(&stored_gauge.gauge) == 1 {
                        to_remove.push(*key)
                    }
                    stored_gauge.gauge.increment(0);
                }
                for key in to_remove {
                    guard.remove(&key);
                }
                drop(guard);
            }
        });
    }
}

/// Helper function to calculate a hash key from gauge name and labels
#[doc(hidden)]
pub fn calculate_gauge_key(name: &str, labels: &[(&str, &str)]) -> u64 {
    let mut hasher = DefaultHasher::new();
    name.hash(&mut hasher);
    for (key, value) in labels {
        key.hash(&mut hasher);
        value.hash(&mut hasher);
    }
    hasher.finish()
}

/// Wrapper macro for metrics::gauge! that ensures the given gauge will always send data
/// Every persistent gauge will be updated every 30 seconds to prevent it from being removed
/// Use exactly the same as you would gauge!
#[macro_export]
macro_rules! persistent_gauge {
    (target: $target:expr, level: $level:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {{
        let gauge = ::metrics::gauge!(target: $target, level: $level, $name $(, $label_key $(=> $label_value)?)*);
        let labels: Vec<(&str, String)> = vec![
            $(
                ($label_key, {
                    $(
                        $label_value.to_string()
                    )?
                }),
            )*
        ];
        let labels_ref: Vec<(&str, &str)> = labels.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let key = $crate::gauge::calculate_gauge_key($name, &labels_ref);
        $crate::gauge::PERSISTENT_GAUGE_REGISTRY.register(gauge.clone(), key, true)
    }};
    (target: $target:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {{
        let gauge = ::metrics::gauge!(target: $target, $name $(, $label_key $(=> $label_value)?)*);
        let labels: Vec<(&str, String)> = vec![
            $(
                ($label_key, {
                    $(
                        $label_value.to_string()
                    )?
                }),
            )*
        ];
        let labels_ref: Vec<(&str, &str)> = labels.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let key = $crate::gauge::calculate_gauge_key($name, &labels_ref);
        $crate::gauge::PERSISTENT_GAUGE_REGISTRY.register(gauge.clone(), key, true)
    }};
    (level: $level:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {{
        let gauge = ::metrics::gauge!(level: $level, $name $(, $label_key $(=> $label_value)?)*);
        let labels: Vec<(&str, String)> = vec![
            $(
                ($label_key, {
                    $(
                        $label_value.to_string()
                    )?
                }),
            )*
        ];
        let labels_ref: Vec<(&str, &str)> = labels.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let key = $crate::gauge::calculate_gauge_key($name, &labels_ref);
        $crate::gauge::PERSISTENT_GAUGE_REGISTRY.register(gauge.clone(), key, true)
    }};
    ($name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {{
        let gauge = ::metrics::gauge!($name $(, $label_key $(=> $label_value)?)*);
        let labels: Vec<(&str, String)> = vec![
            $(
                ($label_key, {
                    $(
                        $label_value.to_string()
                    )?
                }),
            )*
        ];
        let labels_ref: Vec<(&str, &str)> = labels.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let key = $crate::gauge::calculate_gauge_key($name, &labels_ref);
        $crate::gauge::PERSISTENT_GAUGE_REGISTRY.register(gauge.clone(), key, true)
    }};
}
