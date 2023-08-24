use std::cmp;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use corro_types::agent::{Agent, KnownDbVersion, SplitPool};
use corro_types::broadcast::{ChangeV1, Changeset};
use corro_types::change::row_to_change;
use corro_types::config::{GossipConfig, TlsClientConfig};
use corro_types::sync::{SyncMessage, SyncMessageEncodeError, SyncMessageV1, SyncStateV1};
use futures::{SinkExt, StreamExt, TryFutureExt};
use metrics::counter;
use quinn::{RecvStream, SendStream};
use rusqlite::{params, Connection};
use speedy::Writable;
use tokio::sync::mpsc::{channel, Sender};
use tokio::task::block_in_place;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::{debug, error, info, trace, warn};

use crate::agent::{process_single_version, SyncRecvError};
use crate::api::client::{ChunkedChanges, MAX_CHANGES_PER_MESSAGE};

use corro_types::{
    actor::ActorId,
    agent::{Booked, Bookie},
};

#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error(transparent)]
    Send(#[from] SyncSendError),
    #[error(transparent)]
    Recv(#[from] SyncRecvError),
}

#[derive(Debug, thiserror::Error)]
pub enum SyncSendError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Rusqlite(#[from] rusqlite::Error),

    #[error("expected a column, there were none")]
    NoMoreColumns,

    #[error("expected column '{0}', got none")]
    MissingColumn(&'static str),

    #[error("expected column '{0}', got '{1}'")]
    WrongColumn(&'static str, String),

    #[error("unexpected column '{0}'")]
    UnexpectedColumn(String),

    #[error("unknown resource state")]
    UnknownResourceState,

    #[error("unknown resource type")]
    UnknownResourceType,

    #[error("wrong ip byte len: {0}")]
    WrongIpByteLength(usize),

    #[error("could not encode message: {0}")]
    Encode(#[from] SyncMessageEncodeError),

    #[error("sync send channel is closed")]
    ChannelClosed,
}

fn build_quinn_transport_config(config: &GossipConfig) -> quinn::TransportConfig {
    let mut transport_config = quinn::TransportConfig::default();

    // max idle timeout
    transport_config.max_idle_timeout(Some(
        Duration::from_secs(std::cmp::min(config.idle_timeout_secs as u64, 10))
            .try_into()
            .unwrap(),
    ));

    // max concurrent bidirectional streams
    transport_config.max_concurrent_bidi_streams(32u32.into());

    // max concurrent unidirectional streams
    transport_config.max_concurrent_uni_streams(512u32.into());

    if let Some(max_mtu) = config.max_mtu {
        info!("Setting maximum MTU for QUIC at {max_mtu}");
        transport_config.initial_mtu(max_mtu);
        // disable discovery
        transport_config.mtu_discovery_config(None);
    }

    if config.disable_gso {
        transport_config.enable_segmentation_offload(false);
    }

    transport_config
}

async fn build_quinn_server_config(config: &GossipConfig) -> eyre::Result<quinn::ServerConfig> {
    let mut server_config = if config.plaintext {
        quinn_plaintext::server_config()
    } else {
        let tls = config
            .tls
            .as_ref()
            .ok_or_else(|| eyre::eyre!("either plaintext or a tls config is required"))?;

        let key = tokio::fs::read(&tls.key_file).await?;
        let key = if tls.key_file.extension().map_or(false, |x| x == "der") {
            rustls::PrivateKey(key)
        } else {
            let pkcs8 = rustls_pemfile::pkcs8_private_keys(&mut &*key)?;
            match pkcs8.into_iter().next() {
                Some(x) => rustls::PrivateKey(x),
                None => {
                    let rsa = rustls_pemfile::rsa_private_keys(&mut &*key)?;
                    match rsa.into_iter().next() {
                        Some(x) => rustls::PrivateKey(x),
                        None => {
                            eyre::bail!("no private keys found");
                        }
                    }
                }
            }
        };

        let certs = tokio::fs::read(&tls.cert_file).await?;
        let certs = if tls.cert_file.extension().map_or(false, |x| x == "der") {
            vec![rustls::Certificate(certs)]
        } else {
            rustls_pemfile::certs(&mut &*certs)?
                .into_iter()
                .map(rustls::Certificate)
                .collect()
        };

        let server_crypto = rustls::ServerConfig::builder().with_safe_defaults();

        let server_crypto = if tls.client.is_some() {
            let ca_file = match &tls.ca_file {
                None => {
                    eyre::bail!(
                        "ca_file required in tls config for server client cert auth verification"
                    );
                }
                Some(ca_file) => ca_file,
            };

            let ca_certs = tokio::fs::read(&ca_file).await?;
            let ca_certs = if ca_file.extension().map_or(false, |x| x == "der") {
                vec![rustls::Certificate(ca_certs)]
            } else {
                rustls_pemfile::certs(&mut &*ca_certs)?
                    .into_iter()
                    .map(rustls::Certificate)
                    .collect()
            };

            let mut root_store = rustls::RootCertStore::empty();

            for cert in ca_certs {
                root_store.add(&cert)?;
            }

            server_crypto.with_client_cert_verifier(Arc::new(
                rustls::server::AllowAnyAuthenticatedClient::new(root_store),
            ))
        } else {
            server_crypto.with_no_client_auth()
        };

        quinn::ServerConfig::with_crypto(Arc::new(server_crypto.with_single_cert(certs, key)?))
    };

    let transport_config = build_quinn_transport_config(config);

    server_config.transport_config(Arc::new(transport_config));

    Ok(server_config)
}

pub async fn gossip_server_endpoint(config: &GossipConfig) -> eyre::Result<quinn::Endpoint> {
    let server_config = build_quinn_server_config(config).await?;

    Ok(quinn::Endpoint::server(server_config, config.bind_addr)?)
}

fn client_cert_auth(
    config: &TlsClientConfig,
) -> eyre::Result<(Vec<rustls::Certificate>, rustls::PrivateKey)> {
    let mut cert_file = std::io::BufReader::new(
        std::fs::OpenOptions::new()
            .read(true)
            .open(&config.cert_file)?,
    );
    let certs = rustls_pemfile::certs(&mut cert_file)?
        .into_iter()
        .map(rustls::Certificate)
        .collect();

    let mut key_file = std::io::BufReader::new(
        std::fs::OpenOptions::new()
            .read(true)
            .open(&config.key_file)?,
    );
    let key = rustls_pemfile::pkcs8_private_keys(&mut key_file)?
        .into_iter()
        .map(rustls::PrivateKey)
        .next()
        .ok_or_else(|| eyre::eyre!("could not find client tls key"))?;

    Ok((certs, key))
}

async fn build_quinn_client_config(config: &GossipConfig) -> eyre::Result<quinn::ClientConfig> {
    let mut client_config = if config.plaintext {
        quinn_plaintext::client_config()
    } else {
        let tls = config
            .tls
            .as_ref()
            .ok_or_else(|| eyre::eyre!("tls config required"))?;

        let client_crypto = rustls::ClientConfig::builder().with_safe_defaults();

        let client_crypto = if let Some(ca_file) = &tls.ca_file {
            let ca_certs = tokio::fs::read(&ca_file).await?;
            let ca_certs = if ca_file.extension().map_or(false, |x| x == "der") {
                vec![rustls::Certificate(ca_certs)]
            } else {
                rustls_pemfile::certs(&mut &*ca_certs)?
                    .into_iter()
                    .map(rustls::Certificate)
                    .collect()
            };

            let mut root_store = rustls::RootCertStore::empty();

            for cert in ca_certs {
                root_store.add(&cert)?;
            }

            let client_crypto = client_crypto.with_root_certificates(root_store);

            if let Some(client_config) = &tls.client {
                let (certs, key) = client_cert_auth(client_config)?;
                client_crypto.with_client_auth_cert(certs, key)?
            } else {
                client_crypto.with_no_client_auth()
            }
        } else {
            if !tls.insecure {
                eyre::bail!(
                    "insecure setting needs to be explicitly true if no ca_file is provided"
                );
            }
            let client_crypto =
                client_crypto.with_custom_certificate_verifier(SkipServerVerification::new());
            if let Some(client_config) = &tls.client {
                let (certs, key) = client_cert_auth(client_config)?;
                client_crypto.with_client_auth_cert(certs, key)?
            } else {
                client_crypto.with_no_client_auth()
            }
        };

        quinn::ClientConfig::new(Arc::new(client_crypto))
    };

    client_config.transport_config(Arc::new(build_quinn_transport_config(config)));

    Ok(client_config)
}

pub async fn gossip_client_endpoint(config: &GossipConfig) -> eyre::Result<quinn::Endpoint> {
    let client_config = build_quinn_client_config(config).await?;

    let client_bind_addr = match config.bind_addr {
        SocketAddr::V4(_) => "0.0.0.0:0".parse()?,
        SocketAddr::V6(_) => "[::]:0".parse()?,
    };
    let mut client = quinn::Endpoint::client(client_bind_addr)?;

    client.set_default_client_config(client_config);
    Ok(client)
}

/// Dummy certificate verifier that treats any certificate as valid.
/// NOTE, such verification is vulnerable to MITM attacks, but convenient for testing.
struct SkipServerVerification;

impl SkipServerVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

impl rustls::client::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}

fn process_range(
    booked: &Booked,
    conn: &Connection,
    range: &RangeInclusive<i64>,
    actor_id: ActorId,
    is_local: bool,
    sender: &Sender<SyncMessage>,
) -> eyre::Result<()> {
    let (start, end) = (range.start(), range.end());
    trace!("processing range {start}..={end} for {}", actor_id);

    let overlapping: Vec<(_, KnownDbVersion)> = {
        booked
            .read()
            .overlapping(range)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    };

    for (versions, known_version) in overlapping {
        block_in_place(|| {
            if let KnownDbVersion::Cleared = &known_version {
                sender.blocking_send(SyncMessage::V1(SyncMessageV1::Changeset(ChangeV1 {
                    actor_id,
                    changeset: Changeset::Empty { versions },
                })))?;
                return Ok(());
            }

            for version in versions {
                process_version(
                    &conn,
                    actor_id,
                    is_local,
                    version,
                    &known_version,
                    vec![],
                    &sender,
                )?;
            }
            Ok::<_, eyre::Report>(())
        })?;
    }

    Ok(())
}

fn process_version(
    conn: &Connection,
    actor_id: ActorId,
    is_local: bool,
    version: i64,
    known_version: &KnownDbVersion,
    mut seqs_needed: Vec<RangeInclusive<i64>>,
    sender: &Sender<SyncMessage>,
) -> eyre::Result<()> {
    match known_version {
        KnownDbVersion::Current {
            db_version,
            last_seq,
            ts,
        } => {
            let mut prepped = conn.prepare_cached(r#"SELECT "table", pk, cid, val, col_version, db_version, seq, COALESCE(site_id, crsql_site_id()), cl FROM crsql_changes WHERE site_id IS ? AND db_version = ? ORDER BY seq ASC"#)?;
            let site_id: Option<[u8; 16]> = (!is_local)
                .then_some(actor_id)
                .map(|actor_id| actor_id.to_bytes());

            let rows = prepped.query_map(params![site_id, db_version], row_to_change)?;

            let mut chunked = ChunkedChanges::new(rows, 0, *last_seq, MAX_CHANGES_PER_MESSAGE);
            while let Some(changes_seqs) = chunked.next() {
                match changes_seqs {
                    Ok((changes, seqs)) => {
                        if let Err(_) = sender.blocking_send(SyncMessage::V1(
                            SyncMessageV1::Changeset(ChangeV1 {
                                actor_id,
                                changeset: Changeset::Full {
                                    version,
                                    changes,
                                    seqs,
                                    last_seq: *last_seq,
                                    ts: *ts,
                                },
                            }),
                        )) {
                            eyre::bail!("sync message sender channel is closed");
                        }
                    }
                    Err(e) => {
                        error!("could not process crsql change (db_version: {db_version}) for broadcast: {e}");
                        break;
                    }
                }
            }
        }
        KnownDbVersion::Partial { seqs, last_seq, ts } => {
            debug!("seqs needed: {seqs_needed:?}");
            debug!("seqs we got: {seqs:?}");
            if seqs_needed.is_empty() {
                seqs_needed = vec![(0..=*last_seq)];
            }

            for range_needed in seqs_needed {
                for range in seqs.overlapping(&range_needed) {
                    let start_seq = cmp::max(range.start(), range_needed.start());
                    debug!("start seq: {start_seq}");
                    let end_seq = cmp::min(range.end(), range_needed.end());
                    debug!("end seq: {end_seq}");

                    let mut prepped = conn.prepare_cached(r#"SELECT "table", pk, cid, val, col_version, db_version, seq, site_id, cl FROM __corro_buffered_changes WHERE site_id = ? AND version = ? AND seq >= ? AND seq <= ?"#)?;

                    let site_id: [u8; 16] = actor_id.to_bytes();

                    let rows = prepped
                        .query_map(params![site_id, version, start_seq, end_seq], row_to_change)?;

                    let mut chunked =
                        ChunkedChanges::new(rows, *start_seq, *end_seq, MAX_CHANGES_PER_MESSAGE);
                    while let Some(changes_seqs) = chunked.next() {
                        match changes_seqs {
                            Ok((changes, seqs)) => {
                                if let Err(_e) = sender.blocking_send(SyncMessage::V1(
                                    SyncMessageV1::Changeset(ChangeV1 {
                                        actor_id,
                                        changeset: Changeset::Full {
                                            version,
                                            changes,
                                            seqs,
                                            last_seq: *last_seq,
                                            ts: *ts,
                                        },
                                    }),
                                )) {
                                    eyre::bail!("sync message sender channel is closed");
                                }
                            }
                            Err(e) => {
                                error!("could not process buffered crsql change (version: {version}) for broadcast: {e}");
                                break;
                            }
                        }
                    }
                }
            }
        }
        _ => {
            warn!("not supposed to happen");
        }
    }

    trace!("done processing version: {version} for actor_id: {actor_id}");

    Ok(())
}

async fn process_sync(
    local_actor_id: ActorId,
    pool: SplitPool,
    bookie: Bookie,
    sync_state: SyncStateV1,
    sender: Sender<SyncMessage>,
) -> eyre::Result<()> {
    let conn = pool.read().await?;

    let bookie: HashMap<ActorId, Booked> =
        { bookie.read().iter().map(|(k, v)| (*k, v.clone())).collect() };

    for (actor_id, booked) in bookie {
        if actor_id == sync_state.actor_id {
            trace!("skipping itself!");
            // don't send the requester's data
            continue;
        }
        trace!(actor_id = %local_actor_id, "processing sync for {actor_id}, last: {:?}", booked.last());

        let is_local = actor_id == local_actor_id;

        // 1. process needed versions
        if let Some(needed) = sync_state.need.get(&actor_id) {
            for range in needed {
                process_range(&booked, &conn, range, actor_id, is_local, &sender)?;
            }
        }

        // 2. process partial needs
        if let Some(partially_needed) = sync_state.partial_need.get(&actor_id) {
            for (version, seqs_needed) in partially_needed.iter() {
                let known = { booked.read().get(version).cloned() };
                if let Some(known) = known {
                    block_in_place(|| {
                        process_version(
                            &conn,
                            actor_id,
                            is_local,
                            *version,
                            &known,
                            seqs_needed.clone(),
                            &sender,
                        )
                    })?;
                }
            }
        }

        // 3. process newer-than-heads
        let their_last_version = sync_state.heads.get(&actor_id).copied().unwrap_or(0);
        let our_last_version = booked.last().unwrap_or(0);

        debug!(actor_id = %local_actor_id, "their last version: {their_last_version} vs ours: {our_last_version}");

        if their_last_version >= our_last_version {
            // nothing to teach the other node!
            continue;
        }

        process_range(
            &booked,
            &conn,
            &((their_last_version + 1)..=our_last_version),
            actor_id,
            is_local,
            &sender,
        )?;
    }

    debug!("done processing sync state");

    Ok(())
}

pub async fn bidirectional_sync(
    agent: &Agent,
    our_sync_state: SyncStateV1,
    their_sync_state: Option<SyncStateV1>,
    read: RecvStream,
    write: SendStream,
) -> Result<usize, SyncError> {
    let (tx, mut rx) = channel::<SyncMessage>(256);

    let mut read = FramedRead::new(read, LengthDelimitedCodec::new());
    let mut write = FramedWrite::new(write, LengthDelimitedCodec::new());

    tx.send(SyncMessage::V1(SyncMessageV1::State(our_sync_state)))
        .await
        .map_err(|_| SyncSendError::ChannelClosed)?;

    tx.send(SyncMessage::V1(SyncMessageV1::Clock(
        agent.clock().new_timestamp().into(),
    )))
    .await
    .map_err(|_| SyncSendError::ChannelClosed)?;

    let (_sent_count, recv_count) = tokio::try_join!(
        async move {
            let mut count = 0;
            let mut buf = BytesMut::new();
            while let Some(msg) = rx.recv().await {
                msg.write_to_stream((&mut buf).writer())
                    .map_err(SyncMessageEncodeError::from)
                    .map_err(SyncSendError::from)?;

                let buf_len = buf.len();
                write
                    .send(buf.split().freeze())
                    .await
                    .map_err(SyncSendError::from)?;

                count += 1;

                counter!("corro.sync.chunk.sent.bytes", buf_len as u64);
            }

            let mut send = write.into_inner();
            if let Err(e) = send.finish().await {
                warn!("could not properly finish QUIC send stream: {e}");
            }

            debug!(actor_id = %agent.actor_id(), "done writing sync messages (count: {count})");

            Ok::<_, SyncError>(count)
        },
        async move {
            let their_sync_state = match their_sync_state {
                Some(state) => state,
                None => {
                    if let Some(buf_res) = read.next().await {
                        let mut buf = buf_res.map_err(SyncRecvError::from)?;
                        let msg = SyncMessage::from_buf(&mut buf).map_err(SyncRecvError::from)?;
                        if let SyncMessage::V1(SyncMessageV1::State(state)) = msg {
                            state
                        } else {
                            return Err(SyncRecvError::UnexpectedSyncMessage.into());
                        }
                    } else {
                        return Err(SyncRecvError::UnexpectedEndOfStream.into());
                    }
                }
            };

            let their_actor_id = their_sync_state.actor_id;

            tokio::spawn(
                process_sync(
                    agent.actor_id(),
                    agent.pool().clone(),
                    agent.bookie().clone(),
                    their_sync_state,
                    tx,
                )
                .inspect_err(|e| error!("could not process sync request: {e}")),
            );

            let mut count = 0;

            while let Ok(Some(buf_res)) =
                tokio::time::timeout(Duration::from_secs(5), read.next()).await
            {
                let mut buf = buf_res.map_err(SyncRecvError::from)?;
                match SyncMessage::from_buf(&mut buf) {
                    Ok(msg) => {
                        let len = match msg {
                            SyncMessage::V1(SyncMessageV1::Changeset(change)) => {
                                let len = change.len();
                                process_single_version(agent, change)
                                    .await
                                    .map_err(SyncRecvError::from)?;
                                len
                            }
                            SyncMessage::V1(SyncMessageV1::State(_)) => {
                                warn!("received sync state message more than once, ignoring");
                                continue;
                            }
                            SyncMessage::V1(SyncMessageV1::Clock(ts)) => {
                                if let Err(e) = agent.clock().update_with_timestamp(&ts) {
                                    warn!(
                                        "could not update clock from actor {their_actor_id}: {e}"
                                    );
                                }
                                continue;
                            }
                        };
                        count += len;
                    }
                    Err(e) => return Err(SyncRecvError::from(e).into()),
                }
            }
            debug!(actor_id = %agent.actor_id(), "done reading sync messages");

            Ok(count)
        }
    )?;

    Ok(recv_count)
}

#[cfg(test)]
mod tests {
    use camino::Utf8PathBuf;
    use corro_types::{
        config::TlsConfig,
        tls::{generate_ca, generate_client_cert, generate_server_cert},
    };
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn test_mutual_tls() -> eyre::Result<()> {
        let ca_cert = generate_ca()?;
        let (server_cert, server_cert_signed) = generate_server_cert(
            &ca_cert.serialize_pem()?,
            &ca_cert.serialize_private_key_pem(),
            "127.0.0.1".parse()?,
        )?;

        let (client_cert, client_cert_signed) = generate_client_cert(
            &ca_cert.serialize_pem()?,
            &ca_cert.serialize_private_key_pem(),
        )?;

        let tmpdir = TempDir::new()?;
        let base_path = Utf8PathBuf::from(tmpdir.path().display().to_string());

        let cert_file = base_path.join("cert.pem");
        let key_file = base_path.join("cert.key");
        let ca_file = base_path.join("ca.pem");

        let client_cert_file = base_path.join("client-cert.pem");
        let client_key_file = base_path.join("client-cert.key");

        tokio::fs::write(&cert_file, &server_cert_signed).await?;
        tokio::fs::write(&key_file, server_cert.serialize_private_key_pem()).await?;

        tokio::fs::write(&ca_file, ca_cert.serialize_pem()?).await?;

        tokio::fs::write(&client_cert_file, &client_cert_signed).await?;
        tokio::fs::write(&client_key_file, client_cert.serialize_private_key_pem()).await?;

        let gossip_config = GossipConfig {
            bind_addr: "127.0.0.1:0".parse()?,
            bootstrap: vec![],
            tls: Some(TlsConfig {
                cert_file,
                key_file,
                ca_file: Some(ca_file),
                client: Some(TlsClientConfig {
                    cert_file: client_cert_file,
                    key_file: client_key_file,
                }),
                insecure: false,
            }),
            idle_timeout_secs: 30,
            plaintext: false,
            max_mtu: None,
            disable_gso: false,
        };

        let server = gossip_server_endpoint(&gossip_config).await?;
        let addr = server.local_addr()?;

        let client = gossip_client_endpoint(&gossip_config).await?;

        let res = tokio::try_join!(
            async {
                Ok(client
                    .connect(addr, &addr.ip().to_string())
                    .unwrap()
                    .await
                    .unwrap())
            },
            async {
                let conn = match server.accept().await {
                    None => eyre::bail!("None accept!"),
                    Some(connecting) => connecting.await.unwrap(),
                };
                Ok(conn)
            }
        )?;

        let client_conn = res.0;

        let client_conn_peer_id = client_conn
            .peer_identity()
            .unwrap()
            .downcast::<Vec<rustls::Certificate>>()
            .unwrap();

        let mut server_cert_signed_buf =
            std::io::Cursor::new(server_cert_signed.as_bytes().to_vec());

        assert_eq!(
            client_conn_peer_id[0].0,
            rustls_pemfile::certs(&mut server_cert_signed_buf)?[0]
        );

        let server_conn = res.1;

        let server_conn_peer_id = server_conn
            .peer_identity()
            .unwrap()
            .downcast::<Vec<rustls::Certificate>>()
            .unwrap();

        let mut client_cert_signed_buf =
            std::io::Cursor::new(client_cert_signed.as_bytes().to_vec());

        assert_eq!(
            server_conn_peer_id[0].0,
            rustls_pemfile::certs(&mut client_cert_signed_buf)?[0]
        );

        Ok(())
    }
}
