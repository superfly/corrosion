use circular_buffer::CircularBuffer;
use parking_lot::RwLock;
use std::sync::Arc;

pub struct LagTracker(Arc<RwLock<InnerLagTracker>>);

struct InnerLagTracker {
    buffer: CircularBuffer<1000, f64>,
}

impl Default for LagTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl LagTracker {
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(InnerLagTracker {
            buffer: CircularBuffer::<1000, f64>::new(),
        })))
    }

    pub fn add(&self, lag: f64) {
        self.0.write().buffer.push_back(lag);
    }

    pub fn p99(&self) -> Option<f64> {
        let mut values: Vec<_> = self
            .0
            .read()
            .buffer
            .iter()
            .filter(|x| x.is_finite())
            .copied()
            .collect();
        // we haven't collected enough data yet
        if values.len() < 1000 {
            return None;
        }
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let idx = (values.len() * 99 / 100)
            .saturating_sub(1)
            .min(values.len() - 1);
        Some(values[idx])
    }
}
