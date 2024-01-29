use metrics::{gauge, Gauge};
use std::{
    fmt::{self, Debug, Formatter},
    time::Duration,
};
use tokio::sync::mpsc::{
    channel,
    error::{SendError, SendTimeoutError, TryRecvError, TrySendError},
    Receiver, Sender,
};

/// A tokio channel wrapper sender that tracks occupancy
pub struct CorroSender<T> {
    gauge: Gauge,
    inner: Sender<T>,
}

impl<T> Clone for CorroSender<T> {
    fn clone(&self) -> Self {
        Self {
            gauge: self.gauge.clone(),
            inner: self.inner.clone(),
        }
    }
}

impl<T: Debug> Debug for CorroSender<T> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        self.inner.fmt(f)
    }
}

/// A tokio channel wrapper receiver that tracks occupancy
pub struct CorroReceiver<T> {
    gauge: Gauge,
    inner: Receiver<T>,
}

impl<T: Debug> Debug for CorroReceiver<T> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        self.inner.fmt(f)
    }
}

/// Create a bounded channel which tracks occupancy with a label
pub fn bounded<T>(max: usize, label: &'static str) -> (CorroSender<T>, CorroReceiver<T>) {
    let gauge = gauge!("corro.runtime.channel_occupancy", "max_capacity" => max.to_string(), "label" => label);
    let (tx, rx) = channel(max);

    (
        CorroSender {
            gauge: gauge.clone(),
            inner: tx,
        },
        CorroReceiver { gauge, inner: rx },
    )
}

impl<T> CorroSender<T> {
    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
        self.inner.send(value).await.map(|r| {
            self.gauge.increment(1.0);
            r
        })
    }

    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        self.inner.try_send(value).map(|r| {
            self.gauge.increment(1.0);
            r
        })
    }

    pub async fn send_timeout(
        &self,
        value: T,
        timeout: Duration,
    ) -> Result<(), SendTimeoutError<T>> {
        self.inner.send_timeout(value, timeout).await.map(|r| {
            self.gauge.increment(1.0);
            r
        })
    }
}

impl<T> CorroReceiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        self.inner.recv().await.map(|r| {
            self.gauge.decrement(1.0);
            r
        })
    }

    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.inner.try_recv().map(|r| {
            self.gauge.decrement(1.0);
            r
        })
    }

    pub fn blocking_recv(&mut self) -> Option<T> {
        self.inner.blocking_recv().map(|r| {
            self.gauge.decrement(1.0);
            r
        })
    }
}
