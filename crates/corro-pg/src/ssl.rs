use futures::{SinkExt, StreamExt};
use pgwire::messages::{
    response::{GssEncResponse, SslResponse},
    PgWireBackendMessage, PgWireFrontendMessage,
};
use tokio::io::{AsyncBufReadExt, BufStream};
use tokio_util::codec::Framed;

use crate::utils::CountedTcpStream;

pub(super) enum SslNegotiationType {
    Postgres,
    Direct,
    None(Option<PgWireFrontendMessage>),
}

pub(super) async fn negotiate_ssl(
    socket: &mut Framed<BufStream<CountedTcpStream>, crate::PgWireMessageServerCodec>,
    ssl_supported: bool,
) -> std::io::Result<SslNegotiationType> {
    let buf = socket.get_mut().fill_buf().await?;
    if !buf.is_empty() && buf[0] == 0x16 {
        return Ok(SslNegotiationType::Direct);
    }

    let mut ssl_done = false;
    let mut gss_done = false;

    loop {
        match socket.next().await {
            Some(msg) => {
                let msg = msg?;
                match msg {
                    PgWireFrontendMessage::SslRequest(_) => {
                        if ssl_supported {
                            socket
                                .send(PgWireBackendMessage::SslResponse(SslResponse::Accept))
                                .await?;
                            return Ok(SslNegotiationType::Postgres);
                        } else {
                            socket
                                .send(PgWireBackendMessage::SslResponse(SslResponse::Refuse))
                                .await?;
                            ssl_done = true;

                            if gss_done {
                                return Ok(SslNegotiationType::None(None));
                            } else {
                                // Continue to check for more requests (e.g., GssEncRequest after SSL refuse)
                                continue;
                            }
                        }
                    }
                    PgWireFrontendMessage::GssEncRequest(_) => {
                        let _ = socket.next().await;
                        socket
                            .send(PgWireBackendMessage::GssEncResponse(GssEncResponse::Refuse))
                            .await?;
                        gss_done = true;

                        if ssl_done {
                            return Ok(SslNegotiationType::None(None));
                        } else {
                            // Continue to check for more requests (e.g., SSL request after GSSAPI refuse)
                            continue;
                        }
                    }
                    msg => {
                        return Ok(SslNegotiationType::None(Some(msg)));
                    }
                }
            }
            None => {
                return Ok(SslNegotiationType::None(None));
            }
        }
    }
}

#[inline]
pub(super) fn check_alpn_for_direct_ssl<IO>(
    tls_socket: &tokio_rustls::server::TlsStream<IO>,
) -> std::io::Result<()> {
    if tls_socket.get_ref().1.alpn_protocol() != Some(b"postgresql") {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "received direct SSL connection request without ALPN protocol negotiation extension",
        ))
    } else {
        Ok(())
    }
}
