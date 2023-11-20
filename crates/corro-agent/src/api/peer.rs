use std::cmp;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{BufMut, BytesMut};
use compact_str::format_compact;
use corro_types::agent::{Agent, KnownDbVersion, KnownVersion, PartialVersion, SplitPool};
use corro_types::broadcast::{
    BiPayload, BiPayloadV1, ChangeSource, ChangeV1, Changeset, Timestamp,
};
use corro_types::change::{row_to_change, Change, ChunkedChanges};
use corro_types::config::{GossipConfig, TlsClientConfig};
use corro_types::sync::{
    generate_sync, SyncMessage, SyncMessageEncodeError, SyncMessageV1, SyncNeedV1, SyncRejectionV1,
    SyncRequestV1, SyncStateV1, SyncTraceContextV1,
};
use futures::stream::FuturesUnordered;
use futures::{Future, Stream, TryFutureExt, TryStreamExt};
use itertools::Itertools;
use metrics::{counter, increment_counter};
use quinn::{RecvStream, SendStream};
use rand::seq::SliceRandom;
use rangemap::RangeInclusiveSet;
use rusqlite::{params, Connection};
use speedy::Writable;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{self, unbounded_channel, Sender};
use tokio::task::block_in_place;
use tokio::time::timeout;
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use tokio_stream::StreamExt as TokioStreamExt;
// use tokio_stream::StreamExt as TokioStreamExt;
use tokio_util::codec::{Encoder, FramedRead, LengthDelimitedCodec};
use tracing::{debug, error, info, info_span, trace, warn, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::agent::SyncRecvError;
use crate::transport::{Transport, TransportError};

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
    #[error(transparent)]
    Rejection(#[from] SyncRejectionV1),
    #[error(transparent)]
    Transport(#[from] TransportError),
}

#[derive(Debug, thiserror::Error)]
pub enum SyncSendError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Write(#[from] quinn::WriteError),

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
    transport_config.max_concurrent_uni_streams(256u32.into());

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

const MAX_CHANGES_BYTES_PER_MESSAGE: usize = 8 * 1024;
const MIN_CHANGES_BYTES_PER_MESSAGE: usize = 1024;

const ADAPT_CHUNK_SIZE_THRESHOLD: Duration = Duration::from_millis(500);

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
                                KnownVersion::Partial(PartialVersion { seqs, .. }) => {
                                    if seqs != &partial_seqs {
                                        warn!(%actor_id, version, "different partial sequences, updating! range_needed: {range_needed:?}");
                                        partial_seqs = seqs.clone();
                                        if let Some(new_start_seq) = last_sent_seq.take() {
                                            range_needed =
                                                (new_start_seq + 1)..=*range_needed.end();
                                        }
                                        continue 'outer;
                                    }
                                    None
                                }
                                known @ KnownVersion::Current(_) => Some(known.into()),
                                KnownVersion::Cleared => {
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
                            warn!(%actor_id, version, "switched from partial to current version");

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
    sender: Sender<SyncMessage>,
    recv: mpsc::Receiver<SyncRequestV1>,
) -> eyre::Result<()> {
    let chunked_reqs = ReceiverStream::new(recv).chunks_timeout(10, Duration::from_millis(500));
    tokio::pin!(chunked_reqs);

    let (job_tx, job_rx) = unbounded_channel();

    let mut buf =
        futures::StreamExt::buffer_unordered(
            UnboundedReceiverStream::<
                std::pin::Pin<Box<dyn Future<Output = eyre::Result<()>> + Send>>,
            >::new(job_rx),
            6,
        );

    let mut current_haves = vec![];
    let mut partial_needs = vec![];

    loop {
        let reqs = tokio::select! {
            maybe_reqs = chunked_reqs.next() => match maybe_reqs {
                Some(reqs) => reqs,
                None => break,
            },
            Some(res) = buf.next() => {
                res?;
                continue;
            },
            else => {
                break;
            }
        };

        let agg = reqs
            .into_iter()
            .flatten()
            .group_by(|req| req.0)
            .into_iter()
            .map(|(actor_id, reqs)| (actor_id, reqs.flat_map(|(_, reqs)| reqs).collect()))
            .collect::<Vec<(ActorId, Vec<SyncNeedV1>)>>();

        for (actor_id, needs) in agg {
            let booked = match { bookie.read("process_sync").await.get(&actor_id).cloned() } {
                Some(booked) => booked,
                None => continue,
            };

            let is_local = actor_id == local_actor_id;

            let mut cleared: RangeInclusiveSet<i64> = RangeInclusiveSet::new();

            {
                let read = booked.read("process_need(full)").await;

                for need in needs {
                    match need {
                        SyncNeedV1::Full { versions } => {
                            for version in versions {
                                match read.get(&version) {
                                    Some(KnownVersion::Cleared) => {
                                        cleared.insert(version..=version);
                                    }
                                    Some(known) => {
                                        current_haves.push((version, KnownDbVersion::from(known)));
                                    }
                                    None => continue,
                                }
                            }
                        }
                        SyncNeedV1::Partial { version, seqs } => match read.get(&version) {
                            Some(KnownVersion::Cleared) => {
                                cleared.insert(version..=version);
                            }
                            Some(known) => {
                                partial_needs.push((version, KnownDbVersion::from(known), seqs));
                            }
                            None => continue,
                        },
                    }
                }
            }

            for versions in cleared {
                let sender = sender.clone();
                if job_tx
                    .send(Box::pin(async move {
                        sender
                            .send(SyncMessage::V1(SyncMessageV1::Changeset(ChangeV1 {
                                actor_id,
                                changeset: Changeset::Empty { versions },
                            })))
                            .await
                            .map_err(eyre::Report::from)
                    }))
                    .is_err()
                {
                    eyre::bail!("could not send into job channel");
                }
            }

            for (version, known_version) in current_haves.drain(..) {
                let pool = pool.clone();
                let booked = booked.clone();
                let sender = sender.clone();
                if job_tx
                    .send(Box::pin(async move {
                        process_version(
                            &pool,
                            actor_id,
                            is_local,
                            version,
                            known_version,
                            &booked,
                            vec![],
                            &sender,
                        )
                        .await
                    }))
                    .is_err()
                {
                    eyre::bail!("could not send into job channel");
                }
            }

            for (version, known_version, seqs_needed) in partial_needs.drain(..) {
                let pool = pool.clone();
                let booked = booked.clone();
                let sender = sender.clone();
                if job_tx
                    .send(Box::pin(async move {
                        process_version(
                            &pool,
                            actor_id,
                            is_local,
                            version,
                            known_version,
                            &booked,
                            seqs_needed,
                            &sender,
                        )
                        .await
                    }))
                    .is_err()
                {
                    eyre::bail!("could not send into job channel");
                }
            }
        }
    }
    debug!("done w/ sync server loop");

    drop(job_tx);

    buf.try_collect().await?;

    debug!("done processing sync state");

    Ok(())
}

fn chunk_range(
    range: RangeInclusive<i64>,
    chunk_size: i64,
) -> impl Iterator<Item = RangeInclusive<i64>> {
    range
        .clone()
        .step_by(chunk_size as usize)
        .map(move |block_start| {
            let block_end = (block_start + chunk_size).min(*range.end());
            block_start..=block_end
        })
}

fn encode_sync_msg(
    codec: &mut LengthDelimitedCodec,
    encode_buf: &mut BytesMut,
    send_buf: &mut BytesMut,
    msg: SyncMessage,
) -> Result<(), SyncSendError> {
    msg.write_to_stream(encode_buf.writer())
        .map_err(SyncMessageEncodeError::from)?;

    let data = encode_buf.split().freeze();
    trace!("encoded sync message, len: {}", data.len());
    codec.encode(data, send_buf)?;
    Ok(())
}

async fn encode_write_bipayload_msg(
    codec: &mut LengthDelimitedCodec,
    encode_buf: &mut BytesMut,
    send_buf: &mut BytesMut,
    msg: BiPayload,
    write: &mut SendStream,
) -> Result<(), SyncSendError> {
    encode_bipayload_msg(codec, encode_buf, send_buf, msg)?;

    write_buf(send_buf, write).await
}

fn encode_bipayload_msg(
    codec: &mut LengthDelimitedCodec,
    encode_buf: &mut BytesMut,
    send_buf: &mut BytesMut,
    msg: BiPayload,
) -> Result<(), SyncSendError> {
    msg.write_to_stream(encode_buf.writer())
        .map_err(SyncMessageEncodeError::from)?;

    codec.encode(encode_buf.split().freeze(), send_buf)?;
    Ok(())
}

async fn encode_write_sync_msg(
    codec: &mut LengthDelimitedCodec,
    encode_buf: &mut BytesMut,
    send_buf: &mut BytesMut,
    msg: SyncMessage,
    write: &mut SendStream,
) -> Result<(), SyncSendError> {
    encode_sync_msg(codec, encode_buf, send_buf, msg)?;

    write_buf(send_buf, write).await
}

#[tracing::instrument(skip_all, fields(buf_size = send_buf.len()), err)]
async fn write_buf(send_buf: &mut BytesMut, write: &mut SendStream) -> Result<(), SyncSendError> {
    let len = send_buf.len();
    write.write_chunk(send_buf.split().freeze()).await?;
    counter!("corro.sync.chunk.sent.bytes", len as u64);

    Ok(())
}

#[tracing::instrument(skip(read), fields(buf_size = tracing::field::Empty), err)]
pub async fn read_sync_msg<R: Stream<Item = std::io::Result<BytesMut>> + Unpin>(
    read: &mut R,
) -> Result<Option<SyncMessage>, SyncRecvError> {
    match read.next().await {
        Some(buf_res) => match buf_res {
            Ok(mut buf) => {
                counter!("corro.sync.chunk.recv.bytes", buf.len() as u64);
                tracing::Span::current().record("buf_size", buf.len());
                match SyncMessage::from_buf(&mut buf) {
                    Ok(msg) => Ok(Some(msg)),
                    Err(e) => Err(SyncRecvError::from(e)),
                }
            }
            Err(e) => Err(SyncRecvError::from(e)),
        },
        None => Ok(None),
    }
}

#[tracing::instrument(skip_all, err)]
pub async fn parallel_sync(
    agent: &Agent,
    transport: &Transport,
    members: Vec<(ActorId, SocketAddr)>,
    our_sync_state: SyncStateV1,
) -> Result<usize, SyncError> {
    trace!(
        self_actor_id = %agent.actor_id(),
        "parallel syncing w/ {}",
        members
            .iter()
            .map(|(actor_id, _)| actor_id.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );

    let mut trace_ctx = SyncTraceContextV1::default();
    opentelemetry::global::get_text_map_propagator(|prop| {
        prop.inject_context(&tracing::Span::current().context(), &mut trace_ctx)
    });

    let results = FuturesUnordered::from_iter(members.iter().map(|(actor_id, addr)| {
        let trace_ctx = trace_ctx.clone();
        async {
            (
                *actor_id,
                *addr,
                async {
                    let mut codec = LengthDelimitedCodec::new();
                    let mut send_buf = BytesMut::new();
                    let mut encode_buf = BytesMut::new();

                    let actor_id = *actor_id;
                    let (mut tx, rx) = transport.open_bi(*addr).await?;
                    let mut read = FramedRead::new(rx, LengthDelimitedCodec::new());

                    encode_write_bipayload_msg(
                        &mut codec,
                        &mut encode_buf,
                        &mut send_buf,
                        BiPayload::V1(BiPayloadV1::SyncStart {actor_id: agent.actor_id(), trace_ctx}),
                        &mut tx,
                    ).instrument(info_span!("write_sync_start"))
                    .await?;

                    trace!(%actor_id, self_actor_id = %agent.actor_id(), "sent start payload");

                    encode_write_sync_msg(
                        &mut codec,
                        &mut encode_buf,
                        &mut send_buf,
                        SyncMessage::V1(SyncMessageV1::Clock(agent.clock().new_timestamp().into())),
                        &mut tx,
                    ).instrument(info_span!("write_sync_clock"))
                    .await?;

                    trace!(%actor_id, self_actor_id = %agent.actor_id(), "sent clock payload");
                    tx.flush().instrument(info_span!("quic_flush")).await.map_err(SyncSendError::from)?;

                    trace!(%actor_id, self_actor_id = %agent.actor_id(), "flushed sync payloads");

                    let their_sync_state = match timeout(Duration::from_secs(2), read_sync_msg(&mut read)).instrument(info_span!("read_sync_state")).await.map_err(SyncRecvError::from)?? {
                        Some(SyncMessage::V1(SyncMessageV1::State(state))) => state,
                        Some(SyncMessage::V1(SyncMessageV1::Rejection(rejection))) => {
                            return Err(rejection.into())
                        }
                        Some(_) => return Err(SyncRecvError::ExpectedSyncState.into()),
                        None => return Err(SyncRecvError::UnexpectedEndOfStream.into()),
                    };
                    trace!(%actor_id, self_actor_id = %agent.actor_id(), "read state payload: {their_sync_state:?}");

                    match timeout(Duration::from_secs(2), read_sync_msg(&mut read)).instrument(info_span!("read_sync_clock")).await.map_err(SyncRecvError::from)??  {
                        Some(SyncMessage::V1(SyncMessageV1::Clock(ts))) => match actor_id.try_into() {
                            Ok(id) => {
                                if let Err(e) = agent
                                    .clock()
                                    .update_with_timestamp(&uhlc::Timestamp::new(ts.to_ntp64(), id))
                                {
                                    warn!("could not update clock from actor {actor_id}: {e}");
                                }
                            }
                            Err(e) => {
                                error!("could not convert ActorId to uhlc ID: {e}");
                            }
                        },
                        Some(_) => return Err(SyncRecvError::ExpectedClockMessage.into()),
                        None => return Err(SyncRecvError::UnexpectedEndOfStream.into()),
                    }
                    trace!(%actor_id, self_actor_id = %agent.actor_id(), "read clock payload");

                    increment_counter!("corro.sync.client.member", "id" => actor_id.to_string(), "addr" => addr.to_string());

                    let needs = our_sync_state.compute_available_needs(&their_sync_state);

                    trace!(%actor_id, self_actor_id = %agent.actor_id(), "computed needs");

                    Ok::<_, SyncError>((needs, tx, read))
                }.await
            )
        }.instrument(info_span!("sync_client_handshake", %actor_id, %addr))
    }))
    .collect::<Vec<(ActorId, SocketAddr, Result<_, SyncError>)>>()
    .await;

    debug!("collected member needs and such!");

    #[allow(clippy::manual_try_fold)]
    let syncers = results.into_iter().fold(Ok(vec![]), |agg, (actor_id, addr, res)| {
        match res {
            Ok((needs, tx, read)) => {
                let mut v = agg.unwrap_or_default();
                v.push((actor_id, addr, needs, tx, read));
                Ok(v)
            },
            Err(e) => {
                increment_counter!("corro.sync.client.handshake.errors", "actor_id" => actor_id.to_string(), "addr" => addr.to_string(), "error" => e.to_string());
                match agg {
                    Ok(v) if !v.is_empty() => Ok(v),
                    _ => Err(e)
                }
            }
        }
    })?;

    let len = syncers.len();

    let (readers, mut servers) = {
        let mut rng = rand::thread_rng();
        syncers.into_iter().fold(
            (Vec::with_capacity(len), Vec::with_capacity(len)),
            |(mut readers, mut servers), (actor_id, addr, needs, tx, read)| {
                if needs.is_empty() {
                    trace!(%actor_id, "no needs!");
                    return (readers, servers);
                }
                readers.push((actor_id, read));

                trace!(%actor_id, "needs: {needs:?}");

                debug!(%actor_id, %addr, "needs len: {}", needs.values().map(|needs| needs.iter().map(|need| match need {
                    SyncNeedV1::Full {versions} => (versions.end() - versions.start()) as usize + 1,
                    SyncNeedV1::Partial {..} => 0,
                }).sum::<usize>()).sum::<usize>());

                servers.push((
                    actor_id,
                    addr,
                    needs
                        .into_iter()
                        .flat_map(|(actor_id, needs)| {
                            let mut needs: Vec<_> = needs
                                .into_iter()
                                .flat_map(|need| match need {
                                    // chunk the versions, sometimes it's 0..=1000000 and that's far too big for a chunk!
                                    SyncNeedV1::Full { versions } => chunk_range(versions, 10)
                                        .map(|versions| SyncNeedV1::Full { versions })
                                        .collect(),

                                    need => vec![need],
                                })
                                .collect();

                            // NOTE: IMPORTANT! shuffle the vec so we don't keep looping over the same later on
                            needs.shuffle(&mut rng);

                            needs
                                .into_iter()
                                .map(|need| (actor_id, need))
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>(),
                    tx,
                ));

                (readers, servers)
            },
        )
    };

    if readers.is_empty() && servers.is_empty() {
        return Ok(0);
    }

    tokio::spawn(async move {
        // reusable buffers and constructs
        let mut codec = LengthDelimitedCodec::new();
        let mut send_buf = BytesMut::new();
        let mut encode_buf = BytesMut::new();

        // already requested full versions
        let mut req_full: HashMap<ActorId, RangeInclusiveSet<i64>> = HashMap::new();

        // already requested partial version sequences
        let mut req_partials: HashMap<(ActorId, i64), RangeInclusiveSet<i64>> = HashMap::new();

        let start = Instant::now();

        loop {
            if servers.is_empty() {
                break;
            }
            let mut next_servers = Vec::with_capacity(servers.len());
            'servers: for (server_actor_id, addr, mut needs, mut tx) in servers {
                if needs.is_empty() {
                    continue;
                }
                for (actor_id, need) in needs.drain(0..cmp::min(10, needs.len())) {
                    let actual_needs = match need {
                        SyncNeedV1::Full { versions } => {
                            let range = req_full.entry(actor_id).or_default();

                            let mut new_versions =
                                RangeInclusiveSet::from_iter([versions.clone()].into_iter());

                            // check if we've already requested
                            for overlap in range.overlapping(&versions) {
                                new_versions.remove(overlap.clone());
                            }

                            if new_versions.is_empty() {
                                continue;
                            }

                            new_versions
                                .into_iter()
                                .map(|versions| {
                                    range.remove(versions.clone());
                                    SyncNeedV1::Full { versions }
                                })
                                .collect()
                        }
                        SyncNeedV1::Partial { version, seqs } => {
                            let range = req_partials.entry((actor_id, version)).or_default();
                            let mut new_seqs =
                                RangeInclusiveSet::from_iter(seqs.clone().into_iter());

                            for seqs in seqs {
                                for overlap in range.overlapping(&seqs) {
                                    new_seqs.remove(overlap.clone());
                                }
                            }

                            if new_seqs.is_empty() {
                                continue;
                            }

                            vec![SyncNeedV1::Partial {
                                version,
                                seqs: new_seqs
                                    .into_iter()
                                    .map(|seqs| {
                                        range.remove(seqs.clone());
                                        seqs
                                    })
                                    .collect(),
                            }]
                        }
                    };

                    if actual_needs.is_empty() {
                        warn!(%server_actor_id, %actor_id, %addr, "nothing to send!");
                        continue;
                    }

                    let req_len = actual_needs.len();

                    if let Err(e) = encode_sync_msg(
                        &mut codec,
                        &mut encode_buf,
                        &mut send_buf,
                        SyncMessage::V1(SyncMessageV1::Request(vec![(actor_id, actual_needs)])),
                    ) {
                        error!(%server_actor_id, %actor_id, %addr, "could not encode sync request: {e} (elapsed: {:?})", start.elapsed());
                        continue 'servers;
                    }

                    counter!("corro.sync.client.req.sent", req_len as u64, "actor_id" => server_actor_id.to_string());
                }

                if !send_buf.is_empty() {
                    if let Err(e) = write_buf(&mut send_buf, &mut tx).await {
                        error!(%server_actor_id, %addr, "could not write sync requests: {e} (elapsed: {:?})", start.elapsed());
                        continue;
                    }
                }

                if needs.is_empty() {
                    if let Err(e) = tx.finish().instrument(info_span!("quic_finish")).await {
                        warn!("could not finish stream while sending sync requests: {e}");
                    }
                    debug!(%server_actor_id, %addr, "done trying to sync w/ actor after {:?}", start.elapsed());
                    continue;
                }

                next_servers.push((server_actor_id, addr, needs, tx));
            }
            servers = next_servers;
        }
    }.instrument(info_span!("send_sync_requests")));

    // now handle receiving changesets!

    let counts = FuturesUnordered::from_iter(readers.into_iter().map(|(actor_id, mut read)| {
        let tx_changes = agent.tx_changes().clone();
        async move {
            let mut count = 0;

            loop {
                match read_sync_msg(&mut read).await {
                    Ok(None) => {
                        break;
                    }
                    Err(e) => {
                        error!(%actor_id, "sync recv error: {e}");
                        break;
                    }
                    Ok(Some(msg)) => match msg {
                        SyncMessage::V1(SyncMessageV1::Changeset(change)) => {
                            let changes_len = cmp::max(change.len(), 1);
                            // tracing::Span::current().record("changes_len", changes_len);
                            count += changes_len;
                            counter!("corro.sync.changes.recv", changes_len as u64, "actor_id" => actor_id.to_string());
                            tx_changes
                                .send((change, ChangeSource::Sync))
                                .await
                                .map_err(|_| SyncRecvError::ChangesChannelClosed)?;
                        }
                        SyncMessage::V1(SyncMessageV1::Request(_)) => {
                            warn!("received sync request message unexpectedly, ignoring");
                            continue;
                        }
                        SyncMessage::V1(SyncMessageV1::State(_)) => {
                            warn!("received sync state message unexpectedly, ignoring");
                            continue;
                        }
                        SyncMessage::V1(SyncMessageV1::Clock(_)) => {
                            warn!("received sync clock message unexpectedly, ignoring");
                            continue;
                        }
                        SyncMessage::V1(SyncMessageV1::Rejection(rejection)) => {
                            return Err(rejection.into())
                        }
                    },
                }
            }

            debug!(%actor_id, %count, "done reading sync messages");

            Ok(count)
        }.instrument(info_span!("read_sync_requests_responses", %actor_id))
    }))
    .collect::<Vec<Result<usize, SyncError>>>()
    .await;

    for res in counts.iter() {
        if let Err(e) = res {
            error!("could not properly recv from peer: {e}");
        }
    }

    Ok(counts.into_iter().flatten().sum::<usize>())
}

#[tracing::instrument(skip(agent, their_actor_id, read, write), fields(actor_id = %their_actor_id), err)]
pub async fn serve_sync(
    agent: &Agent,
    their_actor_id: ActorId,
    trace_ctx: SyncTraceContextV1,
    mut read: FramedRead<RecvStream, LengthDelimitedCodec>,
    mut write: SendStream,
) -> Result<usize, SyncError> {
    let context =
        opentelemetry::global::get_text_map_propagator(|propagator| propagator.extract(&trace_ctx));
    tracing::Span::current().set_parent(context);

    debug!(actor_id = %their_actor_id, self_actor_id = %agent.actor_id(), "received sync request");
    let mut codec = LengthDelimitedCodec::new();
    let mut send_buf = BytesMut::new();
    let mut encode_buf = BytesMut::new();

    // read the clock
    match read_sync_msg(&mut read)
        .instrument(info_span!("read_peer_clock"))
        .await?
    {
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

    trace!(actor_id = %their_actor_id, self_actor_id = %agent.actor_id(), "read clock");

    let sync_state = generate_sync(agent.bookie(), agent.actor_id()).await;

    // first, send the current sync state
    encode_write_sync_msg(
        &mut codec,
        &mut encode_buf,
        &mut send_buf,
        SyncMessage::V1(SyncMessageV1::State(sync_state)),
        &mut write,
    )
    .instrument(info_span!("write_sync_state"))
    .await?;

    trace!(actor_id = %their_actor_id, self_actor_id = %agent.actor_id(), "sent sync state");

    // then the current clock's timestamp
    encode_write_sync_msg(
        &mut codec,
        &mut encode_buf,
        &mut send_buf,
        SyncMessage::V1(SyncMessageV1::Clock(agent.clock().new_timestamp().into())),
        &mut write,
    )
    .instrument(info_span!("write_sync_clock"))
    .await?;
    trace!(actor_id = %their_actor_id, self_actor_id = %agent.actor_id(), "sent clock");

    // ensure we flush here so the data gets there fast. clock needs to be fresh!
    write
        .flush()
        .instrument(info_span!("quic_flush"))
        .await
        .map_err(SyncSendError::from)?;
    trace!(actor_id = %their_actor_id, self_actor_id = %agent.actor_id(), "flushed sync messages");

    let (tx_need, rx_need) = mpsc::channel(1024);
    let (tx, mut rx) = mpsc::channel::<SyncMessage>(256);

    tokio::spawn(
        process_sync(
            agent.actor_id(),
            agent.pool().clone(),
            agent.bookie().clone(),
            tx,
            rx_need,
        )
        .instrument(info_span!("process_sync"))
        .inspect_err(|e| error!("could not process sync request: {e}")),
    );

    let (send_res, recv_res) = tokio::join!(
        async move {
            let mut count = 0;

            let mut check_buf = tokio::time::interval(Duration::from_secs(1));

            let mut stopped = false;

            loop {
                tokio::select! {
                    biased;

                    stopped_res = write.stopped() => {
                        match stopped_res {
                            Ok(code) => {
                                debug!(actor_id = %their_actor_id, "send stream was stopped by peer, code: {code}");
                            },
                            Err(e) => {
                                warn!(actor_id = %their_actor_id, "error waiting for stop from stream: {e}");
                            }
                        }
                        stopped = true;
                        break;
                    },

                    maybe_msg = rx.recv() => match maybe_msg {
                        Some(msg) => {
                            if let SyncMessage::V1(SyncMessageV1::Changeset(change)) = &msg {
                                count += change.len();
                            }
                            encode_sync_msg(&mut codec, &mut encode_buf, &mut send_buf, msg)?;

                            if send_buf.len() >= 16 * 1024 {
                                write_buf(&mut send_buf, &mut write).await?;
                            }
                        },
                        None => {
                            break;
                        }
                    },

                    _ = check_buf.tick() => {
                        if !send_buf.is_empty() {
                            write_buf(&mut send_buf, &mut write).await?;
                        }
                    }
                }
            }

            if !stopped {
                if !send_buf.is_empty() {
                    write_buf(&mut send_buf, &mut write).await?;
                }

                if let Err(e) = write.finish().await {
                    warn!("could not properly finish QUIC send stream: {e}");
                }
            }

            debug!(actor_id = %agent.actor_id(), "done writing sync messages (count: {count})");

            counter!("corro.sync.changes.sent", count as u64, "actor_id" => their_actor_id.to_string());

            Ok::<_, SyncError>(count)
        }.instrument(info_span!("process_versions_to_send")),
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
                        SyncMessage::V1(SyncMessageV1::Request(req)) => {
                            trace!(actor_id = %their_actor_id, self_actor_id = %agent.actor_id(), "read req: {req:?}");
                            count += req
                                .iter()
                                .map(|(_, needs)| {
                                    needs.iter().map(|need| need.count()).sum::<usize>()
                                })
                                .sum::<usize>();
                            tx_need
                                .send(req)
                                .await
                                .map_err(|_| SyncRecvError::RequestsChannelClosed)?;
                        }
                        SyncMessage::V1(SyncMessageV1::Changeset(_)) => {
                            warn!(actor_id = %their_actor_id, "received sync changeset message unexpectedly, ignoring");
                            continue;
                        }
                        SyncMessage::V1(SyncMessageV1::State(_)) => {
                            warn!(actor_id = %their_actor_id, "received sync state message unexpectedly, ignoring");
                            continue;
                        }
                        SyncMessage::V1(SyncMessageV1::Clock(_)) => {
                            warn!(actor_id = %their_actor_id, "received sync clock message more than once, ignoring");
                            continue;
                        }
                        SyncMessage::V1(SyncMessageV1::Rejection(rejection)) => {
                            return Err(rejection.into())
                        }
                    },
                }
            }

            debug!(actor_id = %agent.actor_id(), "done reading sync messages");

            counter!("corro.sync.requests.recv", count as u64, "actor_id" => their_actor_id.to_string());

            Ok(count)
        }.instrument(info_span!("process_version_requests"))
    );

    if let Err(e) = send_res {
        error!(actor_id = %their_actor_id, "could not complete serving sync due to a send side error: {e}");
    }

    recv_res
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

            assert_eq!(KnownDbVersion::from(read.get(&1).unwrap()), known1);
            assert_eq!(KnownDbVersion::from(read.get(&2).unwrap()), known2);
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
