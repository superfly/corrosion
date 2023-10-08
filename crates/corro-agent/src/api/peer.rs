use std::cmp;
use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Buf, BufMut, BytesMut};
use compact_str::format_compact;
use corro_types::agent::{Agent, KnownDbVersion, SplitPool};
use corro_types::broadcast::{ChangeSource, ChangeV1, Changeset, Timestamp};
use corro_types::change::{row_to_change, Change};
use corro_types::config::{GossipConfig, TlsClientConfig};
use corro_types::sync::{SyncMessage, SyncMessageEncodeError, SyncMessageV1, SyncStateV1};
use futures::{Stream, TryFutureExt};
use metrics::counter;
use quinn::{RecvStream, SendStream};
use rusqlite::{params, Connection};
use speedy::Writable;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc::{channel, Sender};
use tokio::task::block_in_place;
use tokio_stream::StreamExt;
use tokio_util::codec::{Encoder, FramedRead, LengthDelimitedCodec};
use tracing::{debug, error, info, trace, warn};

use crate::agent::SyncRecvError;
use crate::api::public::ChunkedChanges;

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
        Duration::from_secs(config.idle_timeout_secs as u64)
            .try_into()
            .unwrap(),
    ));

    // max concurrent bidirectional streams
    transport_config.max_concurrent_bidi_streams(32u32.into());

    // max concurrent unidirectional streams
    transport_config.max_concurrent_uni_streams(2048u32.into());

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

const MAX_CHANGES_BYTES_PER_MESSAGE: usize = 64 * 1024;
const MIN_CHANGES_BYTES_PER_MESSAGE: usize = 2 * 1024;

const ADAPT_CHUNK_SIZE_THRESHOLD: Duration = Duration::from_millis(500);

async fn process_range(
    booked: &Booked,
    pool: &SplitPool,
    range: &RangeInclusive<i64>,
    actor_id: ActorId,
    is_local: bool,
    sender: &Sender<SyncMessage>,
) -> eyre::Result<()> {
    let (start, end) = (range.start(), range.end());
    trace!("processing range {start}..={end} for {}", actor_id);

    let overlapping: Vec<(_, KnownDbVersion)> = {
        booked
            .read(format!(
                "process_range(overlapping):{}",
                actor_id.as_simple()
            ))
            .await
            .overlapping(range)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    };

    for (versions, known_version) in overlapping {
        debug!("got overlapping range {versions:?} in {range:?}");

        let mut processed = BTreeSet::new();
        // optimization, cleared versions can't be revived... sending a single batch!
        if let KnownDbVersion::Cleared = &known_version {
            sender
                .send(SyncMessage::V1(SyncMessageV1::Changeset(ChangeV1 {
                    actor_id,
                    changeset: Changeset::Empty { versions },
                })))
                .await?;
            continue;
        }

        for version in versions {
            let known = {
                booked
                    .read(format!("process_range[{version}]:{}", actor_id.as_simple()))
                    .await
                    .get(&version)
                    .cloned()
            };
            if let Some(known_version) = known {
                process_version(
                    pool,
                    actor_id,
                    is_local,
                    version,
                    known_version,
                    booked,
                    vec![],
                    sender,
                )
                .await?;
                processed.insert(version);
            }
        }

        debug!("processed versions {processed:?}");
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn handle_known_version(
    conn: &mut Connection,
    actor_id: ActorId,
    is_local: bool,
    version: i64,
    init_known: KnownDbVersion,
    booked: &Booked,
    seqs_needed: Vec<RangeInclusive<i64>>,
    last_seq: i64,
    ts: Timestamp,
    sender: &Sender<SyncMessage>,
) -> eyre::Result<()> {
    debug!(%actor_id, %version, "handle known version! known: {init_known:?}, seqs_needed: {seqs_needed:?}");
    let mut seqs_iter = seqs_needed.into_iter();
    while let Some(range_needed) = seqs_iter.by_ref().next() {
        match &init_known {
            KnownDbVersion::Current { db_version, .. } => {
                let bw = booked.blocking_write(format_compact!(
                    "sync_handle_known[{version}]:{}",
                    actor_id.as_simple()
                ));
                match bw.get(&version) {
                    Some(known) => {
                        // a current version cannot go back to a partial version
                        if known.is_cleared() {
                            debug!(%actor_id, version, "in-memory bookkeeping has been cleared, aborting.");
                            break;
                        }
                    }
                    None => {
                        warn!(%actor_id, version, "in-memory bookkeeping vanished, aborting.");
                        break;
                    }
                }

                // this is a read transaction!
                let tx = conn.transaction()?;

                let mut prepped = tx.prepare_cached(r#"
                    SELECT "table", pk, cid, val, col_version, db_version, seq, COALESCE(site_id, crsql_site_id()), cl
                        FROM crsql_changes
                        WHERE site_id IS ?
                        AND db_version = ?
                        AND seq >= ? AND seq <= ?
                        ORDER BY seq ASC
                "#)?;
                let site_id: Option<[u8; 16]> = (!is_local)
                    .then_some(actor_id)
                    .map(|actor_id| actor_id.to_bytes());

                let start_seq = range_needed.start();
                let end_seq = range_needed.end();

                let rows = prepped.query_map(
                    params![site_id, db_version, start_seq, end_seq],
                    row_to_change,
                )?;

                // drop write lock!
                drop(bw);

                send_change_chunks(
                    sender,
                    ChunkedChanges::new(rows, *start_seq, *end_seq, MAX_CHANGES_BYTES_PER_MESSAGE),
                    actor_id,
                    version,
                    last_seq,
                    ts,
                )?;
            }
            KnownDbVersion::Partial { seqs, .. } => {
                let mut partial_seqs = seqs.clone();
                let mut range_needed = range_needed.clone();

                let mut last_sent_seq = None;

                'outer: loop {
                    let overlapping: Vec<RangeInclusive<i64>> =
                        partial_seqs.overlapping(&range_needed).cloned().collect();

                    for range in overlapping {
                        // since there can be partial overlap, we need to only
                        // send back the specific range we have or else we risk
                        // sending bad data and creating inconsistencies

                        // pick the biggest start
                        // e.g. if 0..=10 is needed, and we have 2..=7 then
                        //      we need to fetch from 2
                        let start_seq = cmp::max(range.start(), range_needed.start());

                        // pick the smallest end
                        // e.g. if 0..=10 is needed, and we have 2..=7 then
                        //      we need to stop at 7
                        let end_seq = cmp::min(range.end(), range_needed.end());

                        debug!("partial, effective range: {start_seq}..={end_seq}");

                        let bw = booked.blocking_write(format_compact!(
                            "sync_handle_known(partial)[{version}]:{}",
                            actor_id.as_simple()
                        ));
                        let maybe_current_version = match bw.get(&version) {
                            Some(known) => match known {
                                KnownDbVersion::Partial { seqs, .. } => {
                                    if seqs != &partial_seqs {
                                        info!(%actor_id, version, "different partial sequences, updating! range_needed: {range_needed:?}");
                                        partial_seqs = seqs.clone();
                                        if let Some(new_start_seq) = last_sent_seq.take() {
                                            range_needed =
                                                (new_start_seq + 1)..=*range_needed.end();
                                        }
                                        continue 'outer;
                                    }
                                    None
                                }
                                known @ KnownDbVersion::Current { .. } => Some(known.clone()),
                                KnownDbVersion::Cleared => {
                                    debug!(%actor_id, version, "in-memory bookkeeping has been cleared, aborting.");
                                    break;
                                }
                            },
                            None => {
                                warn!(%actor_id, version, "in-memory bookkeeping vanished!");
                                break;
                            }
                        };

                        if let Some(known) = maybe_current_version {
                            info!(%actor_id, version, "switched from partial to current version");

                            // drop write lock
                            drop(bw);

                            // restart the seqs_needed here!
                            let mut seqs_needed: Vec<RangeInclusive<i64>> = seqs_iter.collect();
                            if let Some(new_start_seq) = last_sent_seq.take() {
                                range_needed = (new_start_seq + 1)..=*range_needed.end();
                            }
                            seqs_needed.insert(0, range_needed);

                            return handle_known_version(
                                conn,
                                actor_id,
                                is_local,
                                version,
                                known,
                                booked,
                                seqs_needed,
                                last_seq,
                                ts,
                                sender,
                            );
                        }

                        // this is a read transaction!
                        let tx = conn.transaction()?;

                        let mut prepped = tx.prepare_cached(
                            r#"
                            SELECT "table", pk, cid, val, col_version, db_version, seq, site_id, cl
                                FROM __corro_buffered_changes
                                WHERE site_id = ?
                                  AND version = ?
                                  AND seq >= ? AND seq <= ?
                                  ORDER BY seq ASC"#,
                        )?;

                        let site_id: [u8; 16] = actor_id.to_bytes();

                        let rows = prepped.query_map(
                            params![site_id, version, start_seq, end_seq],
                            row_to_change,
                        )?;

                        // drop write lock!
                        drop(bw);

                        send_change_chunks(
                            sender,
                            ChunkedChanges::new(
                                rows,
                                *start_seq,
                                *end_seq,
                                MAX_CHANGES_BYTES_PER_MESSAGE,
                            ),
                            actor_id,
                            version,
                            last_seq,
                            ts,
                        )?;

                        debug!(%actor_id, version, "done sending chunks of partial changes");

                        last_sent_seq = Some(*end_seq);
                    }
                    break;
                }
            }
            KnownDbVersion::Cleared => unreachable!(),
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_version(
    pool: &SplitPool,
    actor_id: ActorId,
    is_local: bool,
    version: i64,
    known_version: KnownDbVersion,
    booked: &Booked,
    mut seqs_needed: Vec<RangeInclusive<i64>>,
    sender: &Sender<SyncMessage>,
) -> eyre::Result<()> {
    let mut conn = pool.read().await?;

    let (last_seq, ts) = {
        let (last_seq, ts) = match &known_version {
            KnownDbVersion::Current { last_seq, ts, .. } => (*last_seq, *ts),
            KnownDbVersion::Partial { last_seq, ts, .. } => (*last_seq, *ts),
            KnownDbVersion::Cleared => return Ok(()),
        };
        if seqs_needed.is_empty() {
            seqs_needed = vec![(0..=last_seq)];
        }

        (last_seq, ts)
    };

    block_in_place(|| {
        handle_known_version(
            &mut conn,
            actor_id,
            is_local,
            version,
            known_version,
            booked,
            seqs_needed,
            last_seq,
            ts,
            sender,
        )
    })?;

    trace!("done processing version: {version} for actor_id: {actor_id}");

    Ok(())
}

fn send_change_chunks<I: Iterator<Item = rusqlite::Result<Change>>>(
    sender: &Sender<SyncMessage>,
    mut chunked: ChunkedChanges<I>,
    actor_id: ActorId,
    version: i64,
    last_seq: i64,
    ts: Timestamp,
) -> eyre::Result<()> {
    let mut max_buf_size = chunked.max_buf_size();
    loop {
        if sender.is_closed() {
            eyre::bail!("sync message sender channel is closed");
        }
        match chunked.next() {
            Some(Ok((changes, seqs))) => {
                let start = Instant::now();

                sender.blocking_send(SyncMessage::V1(SyncMessageV1::Changeset(ChangeV1 {
                    actor_id,
                    changeset: Changeset::Full {
                        version,
                        changes,
                        seqs,
                        last_seq,
                        ts,
                    },
                })))?;

                let elapsed = start.elapsed();

                if elapsed > Duration::from_secs(5) {
                    eyre::bail!("time out: peer is too slow");
                }

                if elapsed > ADAPT_CHUNK_SIZE_THRESHOLD {
                    if max_buf_size <= MIN_CHANGES_BYTES_PER_MESSAGE {
                        eyre::bail!("time out: peer is too slow even after reducing throughput");
                    }

                    max_buf_size /= 2;
                    debug!("adapting max chunk size to {max_buf_size} bytes");

                    chunked.set_max_buf_size(max_buf_size);
                }
            }
            Some(Err(e)) => {
                error!(%actor_id, version, "could not process changes to send via sync: {e}");
                break;
            }
            None => {
                break;
            }
        }
    }

    Ok(())
}

async fn process_sync(
    local_actor_id: ActorId,
    pool: SplitPool,
    bookie: Bookie,
    sync_state: SyncStateV1,
    sender: Sender<SyncMessage>,
) -> eyre::Result<()> {
    let booked_actors: HashMap<ActorId, Booked> = {
        bookie
            .read("process_sync")
            .await
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    };

    for (actor_id, booked) in booked_actors {
        if actor_id == sync_state.actor_id {
            trace!("skipping itself!");
            // don't send the requester's data
            continue;
        }
        // trace!(actor_id = %local_actor_id, "processing sync for {actor_id}, last: {:?}", booked.last().await);

        let is_local = actor_id == local_actor_id;

        // 1. process needed versions
        if let Some(needed) = sync_state.need.get(&actor_id) {
            for range in needed {
                process_range(&booked, &pool, range, actor_id, is_local, &sender).await?;
            }
        }

        // 2. process partial needs
        if let Some(partially_needed) = sync_state.partial_need.get(&actor_id) {
            for (version, seqs_needed) in partially_needed.iter() {
                let known = {
                    booked
                        .read(format!("process_sync(partials)[{version}]"))
                        .await
                        .get(version)
                        .cloned()
                };
                if let Some(known) = known {
                    process_version(
                        &pool,
                        actor_id,
                        is_local,
                        *version,
                        known,
                        &booked,
                        seqs_needed.clone(),
                        &sender,
                    )
                    .await?;
                }
            }
        }

        // 3. process newer-than-heads
        let their_last_version = sync_state.heads.get(&actor_id).copied().unwrap_or(0);
        let our_last_version = booked
            .read(format!(
                "process_sync(our_last_version):{}",
                actor_id.as_simple()
            ))
            .await
            .last()
            .unwrap_or(0);

        trace!(actor_id = %local_actor_id, "their last version: {their_last_version} vs ours: {our_last_version}");

        if their_last_version >= our_last_version {
            // nothing to teach the other node!
            continue;
        }

        process_range(
            &booked,
            &pool,
            &((their_last_version + 1)..=our_last_version),
            actor_id,
            is_local,
            &sender,
        )
        .await?;
    }

    debug!("done processing sync state");

    Ok(())
}

fn encode_sync_msg(
    codec: &mut LengthDelimitedCodec,
    encode_buf: &mut BytesMut,
    send_buf: &mut BytesMut,
    msg: SyncMessage,
) -> Result<(), SyncSendError> {
    msg.write_to_stream(encode_buf.writer())
        .map_err(SyncMessageEncodeError::from)?;

    codec.encode(encode_buf.split().freeze(), send_buf)?;
    Ok(())
}

async fn encode_write_sync_msg<W: AsyncWrite + Unpin>(
    codec: &mut LengthDelimitedCodec,
    encode_buf: &mut BytesMut,
    send_buf: &mut BytesMut,
    msg: SyncMessage,
    write: &mut W,
) -> Result<(), SyncSendError> {
    encode_sync_msg(codec, encode_buf, send_buf, msg)?;

    write_sync_buf(send_buf, write).await
}

async fn write_sync_buf<W: AsyncWrite + Unpin>(
    send_buf: &mut BytesMut,
    write: &mut W,
) -> Result<(), SyncSendError> {
    while send_buf.has_remaining() {
        let n = write.write_buf(send_buf).await?;
        if n == 0 {
            break;
        }
        counter!("corro.sync.chunk.sent.bytes", n as u64);
    }

    Ok(())
}

pub async fn read_sync_msg<R: Stream<Item = std::io::Result<BytesMut>> + Unpin>(
    read: &mut R,
) -> Result<Option<SyncMessage>, SyncRecvError> {
    match tokio::time::timeout(Duration::from_secs(5), read.next()).await {
        Ok(Some(buf_res)) => match buf_res {
            Ok(mut buf) => {
                counter!("corro.sync.chunk.recv.bytes", buf.len() as u64);
                match SyncMessage::from_buf(&mut buf) {
                    Ok(msg) => Ok(Some(msg)),
                    Err(e) => Err(SyncRecvError::from(e)),
                }
            }
            Err(e) => Err(SyncRecvError::from(e)),
        },
        Ok(None) => Ok(None),
        Err(_e) => Err(SyncRecvError::TimedOut),
    }
}

pub async fn bidirectional_sync(
    agent: &Agent,
    our_sync_state: SyncStateV1,
    their_sync_state: Option<SyncStateV1>,
    read: RecvStream,
    mut write: SendStream,
) -> Result<usize, SyncError> {
    let (tx, mut rx) = channel::<SyncMessage>(256);

    let mut read = FramedRead::new(read, LengthDelimitedCodec::new());

    let mut codec = LengthDelimitedCodec::new();
    let mut send_buf = BytesMut::new();
    let mut encode_buf = BytesMut::new();

    encode_write_sync_msg(
        &mut codec,
        &mut encode_buf,
        &mut send_buf,
        SyncMessage::V1(SyncMessageV1::State(our_sync_state)),
        &mut write,
    )
    .await?;
    write.flush().await.map_err(SyncSendError::from)?;

    let their_sync_state = match their_sync_state {
        Some(state) => state,
        None => match read_sync_msg(&mut read).await? {
            Some(SyncMessage::V1(SyncMessageV1::State(state))) => state,
            Some(_) => return Err(SyncRecvError::ExpectedSyncState.into()),
            None => return Err(SyncRecvError::UnexpectedEndOfStream.into()),
        },
    };

    let their_actor_id = their_sync_state.actor_id;

    encode_write_sync_msg(
        &mut codec,
        &mut encode_buf,
        &mut send_buf,
        SyncMessage::V1(SyncMessageV1::Clock(agent.clock().new_timestamp().into())),
        &mut write,
    )
    .await?;
    write.flush().await.map_err(SyncSendError::from)?;

    match read_sync_msg(&mut read).await? {
        Some(SyncMessage::V1(SyncMessageV1::Clock(ts))) => match their_actor_id.try_into() {
            Ok(id) => {
                if let Err(e) = agent
                    .clock()
                    .update_with_timestamp(&uhlc::Timestamp::new(ts.to_ntp64(), id))
                {
                    warn!("could not update clock from actor {their_actor_id}: {e}");
                }
            }
            Err(e) => {
                error!("could not convert ActorId to uhlc ID: {e}");
            }
        },
        Some(_) => return Err(SyncRecvError::ExpectedClockMessage.into()),
        None => return Err(SyncRecvError::UnexpectedEndOfStream.into()),
    }

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

    let tx_changes = agent.tx_changes().clone();

    let (_sent_count, recv_count) = tokio::try_join!(
        async move {
            let mut count = 0;

            let mut check_buf = tokio::time::interval(Duration::from_secs(1));

            loop {
                tokio::select! {
                    maybe_msg = rx.recv() => match maybe_msg {
                        Some(msg) => {
                            if let SyncMessage::V1(SyncMessageV1::Changeset(change)) = &msg {
                                count += change.len();
                            }
                            encode_sync_msg(&mut codec, &mut encode_buf, &mut send_buf, msg)?;

                            if send_buf.len() >= 16 * 1024 {
                                write_sync_buf(&mut send_buf, &mut write).await?;
                            }
                        },
                        None => {
                            break;
                        }
                    },
                    _ = check_buf.tick() => {
                        if !send_buf.is_empty() {
                            write_sync_buf(&mut send_buf, &mut write).await?;
                        }
                    }
                }
            }

            if !send_buf.is_empty() {
                write_sync_buf(&mut send_buf, &mut write).await?;
            }

            if let Err(e) = write.finish().await {
                warn!("could not properly finish QUIC send stream: {e}");
            }

            debug!(actor_id = %agent.actor_id(), "done writing sync messages (count: {count})");

            counter!("corro.sync.changes.sent", count as u64, "actor_id" => their_actor_id.to_string());

            Ok::<_, SyncError>(count)
        },
        async move {
            let mut count = 0;

            loop {
                match read_sync_msg(&mut read).await {
                    Ok(None) => {
                        break;
                    }
                    Err(e) => {
                        error!("sync recv error: {e}");
                        break;
                    }
                    Ok(Some(msg)) => match msg {
                        SyncMessage::V1(SyncMessageV1::Changeset(change)) => {
                            count += change.len();
                            tx_changes
                                .send((change, ChangeSource::Sync))
                                .await
                                .map_err(|_| SyncRecvError::ChangesChannelClosed)?;
                        }
                        SyncMessage::V1(SyncMessageV1::State(_)) => {
                            warn!("received sync state message more than once, ignoring");
                            continue;
                        }
                        SyncMessage::V1(SyncMessageV1::Clock(_)) => {
                            warn!("received sync clock message more than once, ignoring");
                            continue;
                        }
                    },
                }
            }

            debug!(actor_id = %agent.actor_id(), "done reading sync messages");

            counter!("corro.sync.changes.recv", count as u64, "actor_id" => their_actor_id.to_string());

            Ok(count)
        }
    )?;

    Ok(recv_count)
}

#[cfg(test)]
mod tests {
    use axum::{Extension, Json};
    use camino::Utf8PathBuf;
    use corro_tests::TEST_SCHEMA;
    use corro_types::{
        api::{ColumnName, TableName},
        config::{Config, TlsConfig},
        pubsub::pack_columns,
        tls::{generate_ca, generate_client_cert, generate_server_cert},
    };
    use hyper::StatusCode;
    use tempfile::TempDir;
    use tokio::sync::mpsc;
    use tripwire::Tripwire;

    use crate::{
        agent::{process_multiple_changes, setup},
        api::public::api_v1_db_schema,
    };

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_known_version() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();

        let (tripwire, _tripwire_worker, _tripwire_tx) = Tripwire::new_simple();

        let dir = tempfile::tempdir()?;

        let (agent, _agent_options) = setup(
            Config::builder()
                .db_path(dir.path().join("corrosion.db").display().to_string())
                .gossip_addr("127.0.0.1:0".parse()?)
                .api_addr("127.0.0.1:0".parse()?)
                .build()?,
            tripwire,
        )
        .await?;

        let (status_code, _res) =
            api_v1_db_schema(Extension(agent.clone()), Json(vec![TEST_SCHEMA.to_owned()])).await;

        assert_eq!(status_code, StatusCode::OK);

        let actor_id = ActorId(uuid::Uuid::new_v4());

        let ts = agent.clock().new_timestamp().into();

        let change1 = Change {
            table: TableName("tests".into()),
            pk: pack_columns(&vec![1i64.into()])?,
            cid: ColumnName("text".into()),
            val: "one".into(),
            col_version: 1,
            db_version: 1,
            seq: 0,
            site_id: actor_id.to_bytes(),
            cl: 1,
        };

        let change2 = Change {
            table: TableName("tests".into()),
            pk: pack_columns(&vec![2i64.into()])?,
            cid: ColumnName("text".into()),
            val: "two".into(),
            col_version: 1,
            db_version: 2,
            seq: 0,
            site_id: actor_id.to_bytes(),
            cl: 1,
        };

        process_multiple_changes(
            &agent,
            vec![
                (
                    ChangeV1 {
                        actor_id,
                        changeset: Changeset::Full {
                            version: 1,
                            changes: vec![change1.clone()],
                            seqs: 0..=0,
                            last_seq: 0,
                            ts,
                        },
                    },
                    ChangeSource::Sync,
                ),
                (
                    ChangeV1 {
                        actor_id,
                        changeset: Changeset::Full {
                            version: 2,
                            changes: vec![change2.clone()],
                            seqs: 0..=0,
                            last_seq: 0,
                            ts,
                        },
                    },
                    ChangeSource::Sync,
                ),
            ],
        )
        .await?;

        let known1 = KnownDbVersion::Current {
            db_version: 1,
            last_seq: 0,
            ts,
        };

        let known2 = KnownDbVersion::Current {
            db_version: 2,
            last_seq: 0, // original last seq
            ts,
        };

        let booked = agent
            .bookie()
            .read("test")
            .await
            .get(&actor_id)
            .cloned()
            .unwrap();

        {
            let read = booked.read("test").await;

            assert_eq!(read.get(&1).unwrap().clone(), known1);
            assert_eq!(read.get(&2).unwrap().clone(), known2);
        }

        {
            let (tx, mut rx) = mpsc::channel(5);
            let mut conn = agent.pool().read().await?;

            {
                let mut prepped = conn.prepare("SELECT * FROM crsql_changes;")?;
                let mut rows = prepped.query([])?;

                loop {
                    let row = rows.next()?;
                    if row.is_none() {
                        break;
                    }

                    println!("ROW: {row:?}");
                }
            }

            block_in_place(|| {
                handle_known_version(
                    &mut conn,
                    actor_id,
                    false,
                    1,
                    known1,
                    &booked,
                    vec![0..=0],
                    0,
                    ts,
                    &tx,
                )
            })?;

            let msg = rx.recv().await.unwrap();
            assert_eq!(
                msg,
                SyncMessage::V1(SyncMessageV1::Changeset(ChangeV1 {
                    actor_id,
                    changeset: Changeset::Full {
                        version: 1,
                        changes: vec![change1],
                        seqs: 0..=0,
                        last_seq: 0,
                        ts,
                    }
                }))
            );

            block_in_place(|| {
                handle_known_version(
                    &mut conn,
                    actor_id,
                    false,
                    2,
                    known2,
                    &booked,
                    vec![0..=0],
                    0,
                    ts,
                    &tx,
                )
            })?;

            let msg = rx.recv().await.unwrap();
            assert_eq!(
                msg,
                SyncMessage::V1(SyncMessageV1::Changeset(ChangeV1 {
                    actor_id,
                    changeset: Changeset::Full {
                        version: 2,
                        changes: vec![change2],
                        seqs: 0..=0,
                        last_seq: 0,
                        ts,
                    }
                }))
            );
        }

        // make_broadcastable_change(&agent, |tx| {
        //     tx.execute("INSERT INTO test (id, text) VALUES (1, \"one\")", [])
        // })?;

        // make_broadcastable_change(&agent, |tx| {
        //     tx.execute("INSERT INTO test (id, text) VALUES (2, \"two\")", [])
        // })?;

        Ok(())
    }

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
