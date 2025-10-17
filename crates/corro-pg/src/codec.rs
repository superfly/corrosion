//! Reimpl of PgWireMessageServerCodec from <https://github.com/sunng87/pgwire/blob/a12c5f28aef8ada3e13a387da9f9dd5a70cfea41/src/tokio/server.rs>,
//! the implementation was public, but was made (accidentally?) private in <https://github.com/sunng87/pgwire/pull/214>

use pgwire::{
    api::{self, ClientInfo},
    error::PgWireError,
    messages as msg,
};
use std::{collections::HashMap, io};
use tokio_util::codec;

pub struct Client {
    pub socket_addr: std::net::SocketAddr,
    pub is_secure: bool,
    pub protocol_version: msg::ProtocolVersion,
    pub pid_secret_key: (i32, msg::startup::SecretKey),
    pub state: api::PgWireConnectionState,
    pub transaction_status: msg::response::TransactionStatus,
    pub metadata: HashMap<String, String>,
}

impl Client {
    pub fn new(socket_addr: std::net::SocketAddr, is_secure: bool) -> Self {
        Self {
            socket_addr,
            is_secure,
            protocol_version: Default::default(),
            pid_secret_key: Default::default(),
            state: Default::default(),
            transaction_status: msg::response::TransactionStatus::Idle,
            metadata: Default::default(),
        }
    }
}

impl ClientInfo for Client {
    #[inline]
    fn socket_addr(&self) -> std::net::SocketAddr {
        self.socket_addr
    }

    #[inline]
    fn is_secure(&self) -> bool {
        self.is_secure
    }

    #[inline]
    fn state(&self) -> api::PgWireConnectionState {
        self.state
    }

    #[inline]
    fn set_state(&mut self, new_state: api::PgWireConnectionState) {
        self.state = new_state;
    }

    #[inline]
    fn transaction_status(&self) -> msg::response::TransactionStatus {
        self.transaction_status
    }

    #[inline]
    fn set_transaction_status(&mut self, new_status: msg::response::TransactionStatus) {
        self.transaction_status = new_status;
    }

    #[inline]
    fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    #[inline]
    fn metadata_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.metadata
    }

    #[inline]
    fn pid_and_secret_key(&self) -> (i32, msg::startup::SecretKey) {
        self.pid_secret_key.clone()
    }

    #[inline]
    fn set_pid_and_secret_key(&mut self, pid: i32, secret_key: msg::startup::SecretKey) {
        self.pid_secret_key = (pid, secret_key);
    }

    #[inline]
    fn protocol_version(&self) -> msg::ProtocolVersion {
        self.protocol_version
    }

    #[inline]
    fn set_protocol_version(&mut self, version: msg::ProtocolVersion) {
        self.protocol_version = version;
    }

    #[inline]
    fn client_certificates<'a>(&self) -> Option<&[rustls::pki_types::CertificateDer<'a>]> {
        None
    }
}

pub struct PgWireMessageServerCodec {
    pub client_info: Client,
    decode_context: msg::DecodeContext,
}

impl PgWireMessageServerCodec {
    pub fn new(client: Client) -> Self {
        Self {
            client_info: client,
            decode_context: msg::DecodeContext::new(msg::ProtocolVersion::PROTOCOL3_0),
        }
    }
}

impl codec::Decoder for PgWireMessageServerCodec {
    type Item = msg::PgWireFrontendMessage;
    type Error = PgWireError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.decode_context.protocol_version = self.client_info.protocol_version;

        match self.client_info.state() {
            api::PgWireConnectionState::AwaitingSslRequest => {}
            api::PgWireConnectionState::AwaitingStartup => {
                self.decode_context.awaiting_ssl = false;
            }
            _ => {
                self.decode_context.awaiting_startup = false;
            }
        }

        msg::PgWireFrontendMessage::decode(src, &self.decode_context)
    }
}

impl codec::Encoder<msg::PgWireBackendMessage> for PgWireMessageServerCodec {
    type Error = io::Error;

    fn encode(
        &mut self,
        item: msg::PgWireBackendMessage,
        dst: &mut bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        item.encode(dst).map_err(Into::into)
    }
}

pub(super) trait SetState {
    fn set_state(&mut self, state: pgwire::api::PgWireConnectionState);
}

impl<T: 'static> SetState for codec::Framed<T, PgWireMessageServerCodec> {
    fn set_state(&mut self, state: pgwire::api::PgWireConnectionState) {
        self.codec_mut().client_info.set_state(state);
    }
}

impl<L, R> SetState for tokio_util::either::Either<L, R>
where
    L: SetState,
    R: SetState,
{
    fn set_state(&mut self, state: pgwire::api::PgWireConnectionState) {
        match self {
            Self::Left(l) => l.set_state(state),
            Self::Right(r) => r.set_state(state),
        }
    }
}
