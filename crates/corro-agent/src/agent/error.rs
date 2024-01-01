use crate::{api::peer::SyncError, transport::TransportError};
use corro_types::{
    agent::ChangeError,
    sqlite::SqlitePoolError,
    sync::{SyncMessageDecodeError, SyncMessageEncodeError},
};
use tokio::time::error::Elapsed;
use hyper::StatusCode;

#[derive(Debug, thiserror::Error)]
pub enum SyncClientError {
    #[error("bad status code: {0}")]
    Status(StatusCode),
    #[error("service unavailable right now")]
    Unavailable,
    #[error(transparent)]
    Connect(#[from] TransportError),
    #[error("request timed out")]
    RequestTimedOut,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Pool(#[from] SqlitePoolError),
    #[error("could not decode message: {0}")]
    Decoded(#[from] SyncMessageDecodeError),
    #[error("could not encode message: {0}")]
    Encoded(#[from] SyncMessageEncodeError),

    #[error(transparent)]
    Sync(#[from] SyncError),
}

impl SyncClientError {
    pub fn is_unavailable(&self) -> bool {
        matches!(self, SyncClientError::Unavailable)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SyncRecvError {
    #[error("could not decode message: {0}")]
    Decoded(#[from] SyncMessageDecodeError),
    #[error(transparent)]
    Change(#[from] ChangeError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("expected sync state message, received something else")]
    ExpectedSyncState,
    #[error("unexpected end of stream")]
    UnexpectedEndOfStream,
    #[error("expected sync clock message, received something else")]
    ExpectedClockMessage,
    #[error("timed out waiting for sync message")]
    TimedOut(#[from] Elapsed),
    #[error("changes channel is closed")]
    ChangesChannelClosed,
    #[error("requests channel is closed")]
    RequestsChannelClosed,
}
