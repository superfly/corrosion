//! Reimpl of PgWireMessageServerCodec from <https://github.com/sunng87/pgwire/blob/a12c5f28aef8ada3e13a387da9f9dd5a70cfea41/src/tokio/server.rs>,
//! the implementation was public, but was made (accidentally?) private in <https://github.com/sunng87/pgwire/pull/214>

use pgwire::{
    api::{self, ClientInfo},
    error::PgWireError,
    messages as msg,
    types::FromSqlText,
};
use postgres_types::Type;
use std::{collections::HashMap, io};
use tokio_util::codec;
use tracing::debug;

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

pub trait VecFromSqlText: Sized {
    fn from_vec_sql_text(
        ty: &Type,
        input: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>>;
}

// Re-implementation of the ToSqlText trait from pg_wire to make it generic over different types.
// Implemented as a macro in pgwire
// https://github.com/sunng87/pgwire/blob/6cbce9d444cc86a01d992f6b35f84c024f10ceda/src/types/from_sql_text.rs#L402
impl<T: FromSqlText> VecFromSqlText for Vec<T> {
    fn from_vec_sql_text(
        ty: &Type,
        input: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        // PostgreSQL array text format: {elem1,elem2,elem3}
        // Remove the outer braces
        let input_str = std::str::from_utf8(input)?;

        if input_str.is_empty() {
            return Ok(Vec::new());
        }

        // Check if it's an array format
        if !input_str.starts_with('{') || !input_str.ends_with('}') {
            return Err("Invalid array format: must start with '{' and end with '}'".into());
        }

        let inner = &input_str[1..input_str.len() - 1];

        if inner.is_empty() {
            return Ok(Vec::new());
        }

        let elements = extract_array_elements(inner)?;
        let mut result = Vec::new();

        for element_str in elements {
            let element = T::from_sql_text(ty, element_str.as_bytes())?;
            result.push(element);
        }

        Ok(result)
    }
}

// Helper function to extract array elements
// https://github.com/sunng87/pgwire/blob/6cbce9d444cc86a01d992f6b35f84c024f10ceda/src/types/from_sql_text.rs#L402
fn extract_array_elements(
    input: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error + Sync + Send>> {
    if input.is_empty() {
        return Ok(Vec::new());
    }

    let mut elements = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut escape_next = false;
    let mut depth = 0; // For nested arrays
    let mut seen_content = false; // helpful for tracking when the last element is an empty string

    for ch in input.chars() {
        match ch {
            '\\' if !escape_next => {
                escape_next = true;
            }
            '"' if !escape_next => {
                in_quotes = !in_quotes;
                // we have seen a new element surrounded by quotes
                if !in_quotes {
                    seen_content = true;
                }
                // Don't include the quotes in the output
            }
            '{' if !in_quotes && !escape_next => {
                depth += 1;
                current.push(ch);
            }
            '}' if !in_quotes && !escape_next => {
                depth -= 1;
                current.push(ch);
            }
            ',' if !in_quotes && depth == 0 && !escape_next => {
                // End of current element
                if !current.trim().eq_ignore_ascii_case("NULL") {
                    elements.push(std::mem::take(&mut current));
                    seen_content = false;
                }
            }
            _ => {
                current.push(ch);
                escape_next = false;
                seen_content = true;
            }
        }
    }

    // Process the last element
    if seen_content && !current.trim().eq_ignore_ascii_case("NULL") {
        elements.push(current);
    }

    debug!(
        "extracted elements: {elements:?} from input: {input}, lenght: {}",
        elements.len()
    );
    Ok(elements)
}
