use metrics::{counter, gauge, histogram, Counter, Histogram};
use std::{
    fmt::{self, Debug, Formatter},
    time::{Duration, Instant},
};
use tokio::{
    sync::mpsc::{
        channel,
        error::{SendError, SendTimeoutError, TryRecvError, TrySendError},
        Receiver, Sender,
    },
    time::interval,
};

use crate::persistent_gauge;

/// A tokio channel wrapper sender that tracks various metrics
pub struct CorroSender<T> {
    send_count: Counter,
    failed_sends: Counter,
    send_time: Histogram,
    inner: Sender<T>,
}

impl<T> Clone for CorroSender<T> {
    fn clone(&self) -> Self {
        Self {
            send_count: self.send_count.clone(),
            failed_sends: self.failed_sends.clone(),
            send_time: self.send_time.clone(),
            inner: self.inner.clone(),
        }
    }
}

impl<T: Debug> Debug for CorroSender<T> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        self.inner.fmt(f)
    }
}

/// A tokio channel wrapper receiver that tracks various metrics
pub struct CorroReceiver<T> {
    recv_count: Counter,
    inner: Receiver<T>,
}

impl<T: Debug> Debug for CorroReceiver<T> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        self.inner.fmt(f)
    }
}

/// Create a bounded channel which tracks capacity with a label
pub fn bounded<T: Send + 'static>(
    capacity: usize,
    label: &'static str,
) -> (CorroSender<T>, CorroReceiver<T>) {
    persistent_gauge!("corro.runtime.channel.max_capacity", "channel_name" => label)
        .set(capacity as f64);

    // Count the number of sends and receives going through the channel
    let send_count = counter!("corro.runtime.channel.send_count", "channel_name" => label);
    let recv_count = counter!("corro.runtime.channel.recv_count", "channel_name" => label);

    // How many times did we fail to send
    let failed_sends = counter!("corro.runtime.channel.failed_send_count", "channel_name" => label);

    // Track current capacity and send time over time
    let capacity_gauge = gauge!("corro.runtime.channel.capacity", "channel_name" => label);
    let send_time = histogram!("corro.runtime.channel.send_delay", "channel_name" => label);

    let (tx, rx) = channel(capacity);

    let threshold = (capacity as f64 * 0.9) as usize;
    let inner_channel = tx.clone();
    tokio::spawn(async move {
        let mut ticks_since_report = 0;
        let mut tick = interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut prev = inner_channel.capacity();
        loop {
            tick.tick().await;
            if inner_channel.is_closed() {
                break;
            }
            let current = inner_channel.capacity();
            if prev != current
                && (current < threshold || prev < threshold || ticks_since_report >= 30)
            {
                capacity_gauge.set(current as f64);
                ticks_since_report = 0;
            } else {
                ticks_since_report += 1;
            }
            prev = current;
        }
    });

    (
        CorroSender {
            send_count,
            failed_sends,
            send_time,
            inner: tx,
        },
        CorroReceiver {
            recv_count,
            inner: rx,
        },
    )
}

impl<T> CorroSender<T> {
    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
        let before = Instant::now();
        self.inner
            .send(value)
            .await
            .inspect(|_r| {
                self.send_time.record(before.elapsed().as_secs_f64());
                self.send_count.increment(1);
            })
            .inspect_err(|_e| {
                self.failed_sends.increment(1);
            })
    }

    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        self.inner
            .try_send(value)
            .inspect(|_r| {
                self.send_count.increment(1);
            })
            .inspect_err(|_e| {
                self.failed_sends.increment(1);
            })
    }

    pub fn blocking_send(&self, value: T) -> Result<(), SendError<T>> {
        let before = Instant::now();
        self.inner
            .blocking_send(value)
            .inspect(|_r| {
                self.send_time.record(before.elapsed().as_secs_f64());
                self.send_count.increment(1);
            })
            .inspect_err(|_e| {
                self.failed_sends.increment(1);
            })
    }

    pub async fn send_timeout(
        &self,
        value: T,
        timeout: Duration,
    ) -> Result<(), SendTimeoutError<T>> {
        let before = Instant::now();
        self.inner
            .send_timeout(value, timeout)
            .await
            .inspect(|_r| {
                self.send_time.record(before.elapsed().as_secs_f64());
                self.send_count.increment(1);
            })
            .inspect_err(|_e| {
                self.failed_sends.increment(1);
            })
    }
}

impl<T> CorroReceiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        self.inner.recv().await.inspect(|_r| {
            self.recv_count.increment(1);
        })
    }

    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.inner.try_recv().inspect(|_r| {
            self.recv_count.increment(1);
        })
    }

    pub fn blocking_recv(&mut self) -> Option<T> {
        self.inner.blocking_recv().inspect(|_r| {
            self.recv_count.increment(1);
        })
    }
}
