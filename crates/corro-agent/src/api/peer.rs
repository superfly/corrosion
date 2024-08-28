use std::cmp;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{BufMut, BytesMut};
use corro_types::actor::ClusterId;
use corro_types::agent::{Agent, SplitPool};
use corro_types::base::{CrsqlSeq, Version};
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
use metrics::counter;
use quinn::{RecvStream, SendStream};
use rand::seq::SliceRandom;
use rangemap::{RangeInclusiveMap, RangeInclusiveSet};
use rusqlite::{named_params, Connection};
use speedy::Writable;
use std::string::String;
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

use corro_types::{actor::ActorId, agent::Bookie};

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
        transport_config.min_mtu(max_mtu);
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

    let mut transport_config = build_quinn_transport_config(config);

    // client should send keepalives!
    transport_config.keep_alive_interval(Some(Duration::from_secs(
        config.idle_timeout_secs as u64 / 2,
    )));

    client_config.transport_config(Arc::new(transport_config));

    Ok(client_config)
}

pub async fn gossip_client_endpoint(config: &GossipConfig) -> eyre::Result<quinn::Endpoint> {
    let client_config = build_quinn_client_config(config).await?;

    let mut client = quinn::Endpoint::client(config.client_addr)?;

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
fn handle_need(
    conn: &mut Connection,
    agent: &Agent,
    actor_id: ActorId,
    need: SyncNeedV1,
    sender: &Sender<SyncMessage>,
    last_cleared_ts: Option<Timestamp>,
) -> eyre::Result<()> {
    debug!(%actor_id, "handle known versions! need: {need:?}");

    let mut empties: RangeInclusiveMap<Version, Timestamp> = RangeInclusiveMap::new();

    // this is a read transaction!
    let tx = conn.transaction()?;

    let mut prepped = tx.prepare_cached(
        "
        SELECT start_version, end_version, last_seq, ts
            FROM __corro_bookkeeping
            WHERE
                actor_id = :actor_id
                AND
                start_version >= COALESCE((
                    -- try to find the previous range
                    SELECT start_version
                        FROM __corro_bookkeeping
                        WHERE
                            actor_id = :actor_id AND
                            start_version < :start -- AND end_version IS NOT NULL
                        ORDER BY start_version DESC
                        LIMIT 1
                ), 1)
                AND
                start_version <= COALESCE((
                    -- try to find the next range
                    SELECT start_version
                        FROM __corro_bookkeeping
                        WHERE
                            actor_id = :actor_id AND
                            start_version > :end -- AND end_version IS NOT NULL
                        ORDER BY start_version ASC
                        LIMIT 1
                ), :end + 1)
                AND
                (
                    -- [:start]---[start_version]---[:end]
                    ( start_version BETWEEN :start AND :end ) OR

                    -- [start_version]---[:start]---[:end]---[end_version]
                    ( start_version <= :start AND end_version >= :end ) OR

                    -- [:start]---[start_version]---[:end]---[end_version]
                    ( start_version <= :end AND end_version >= :end ) OR

                    -- [:start]---[end_version]---[:end]
                    ( end_version BETWEEN :start AND :end )
                )
            ",
    )?;

    match need {
        SyncNeedV1::Full { versions } => {
            let mut rows = prepped.query(named_params! {
                ":actor_id": actor_id,
                ":start": versions.start(),
                ":end": versions.end(),
            })?;

            let mut unprocessed = RangeInclusiveSet::new();
            unprocessed.insert(versions.clone());

            loop {
                let row = match rows.next()? {
                    Some(row) => row,
                    None => {
                        break;
                    }
                };

                let version: Version = row.get(0)?;
                let end_version: Option<Version> = row.get(1)?;
                let ts: Timestamp = row.get(3)?;

                if let Some(end_version) = end_version {
                    // cleared versions!

                    // pick the biggest start between ours and the versions
                    let start_version = cmp::max(version, *versions.start());

                    // pick the smallest end between ours and the versions
                    let end_version = cmp::min(end_version, *versions.end());

                    empties.insert(start_version..=end_version, ts);

                    // we have now processed this range!
                    unprocessed.remove(start_version..=end_version);

                    continue;
                }

                debug!(%actor_id, ?version, "not empty");

                // since this is not a cleared version, those aren't supposed to fail!
                let last_seq: CrsqlSeq = row.get(2)?;

                let mut prepped = tx.prepare_cached(
                    r#"
                        SELECT c."table", c.pk, c.cid, c.val, c.col_version, c.db_version, c.seq, c.site_id, c.cl
                            FROM __corro_bookkeeping AS bk
                        INNER JOIN crsql_changes AS c ON c.site_id = bk.actor_id AND c.db_version = bk.db_version
                            WHERE bk.actor_id = :actor_id
                            AND bk.start_version = :version
                            ORDER BY c.seq ASC
                    "#,
                )?;

                let rows = prepped.query_map(
                    named_params! {
                        ":actor_id": actor_id,
                        ":version": version
                    },
                    row_to_change,
                )?;

                if let Some(empty) = send_change_chunks(
                    sender,
                    ChunkedChanges::new(rows, CrsqlSeq(0), last_seq, MAX_CHANGES_BYTES_PER_MESSAGE),
                    actor_id,
                    version,
                    last_seq,
                    ts,
                )? {
                    empties.insert(empty..=empty, ts);
                }
            }

            // now process the last unprocessed in case we have partials
            for versions in unprocessed {
                for version in versions {
                    let seqs = tx
                        .prepare_cached("
                        SELECT start_seq, end_seq, last_seq, ts FROM __corro_seq_bookkeeping WHERE site_id = :actor_id AND version = :version
                        ")?.query_map(named_params!{
                            ":actor_id": actor_id,
                            ":version": version
                        },|row| Ok((row.get(0)?..=row.get(1)?, row.get(2)?, row.get(3)?)))?.collect::<rusqlite::Result<Vec<(RangeInclusive<CrsqlSeq>, CrsqlSeq, Timestamp)>>>()?;

                    for (range_needed, last_seq, ts) in seqs {
                        let mut prepped = tx.prepare_cached(
                            r#"
                                SELECT "table", pk, cid, val, col_version, db_version, seq, site_id, cl
                                    FROM __corro_buffered_changes
                                    WHERE site_id = :actor_id
                                        AND version = :version
                                        AND seq BETWEEN :start_seq AND :end_seq
                                    ORDER BY seq ASC
                            "#,
                        )?;

                        let start_seq = range_needed.start();
                        let end_seq = range_needed.end();

                        let rows = prepped.query_map(
                            named_params! {
                                ":actor_id": actor_id,
                                ":version": version,
                                ":start_seq": start_seq,
                                ":end_seq": end_seq
                            },
                            row_to_change,
                        )?;

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
                    }
                }
            }

            if !empties.is_empty() {
                for (versions, ts) in empties {
                    sender.blocking_send(SyncMessage::V1(SyncMessageV1::Changeset(ChangeV1 {
                        actor_id,
                        changeset: Changeset::Empty {
                            versions,
                            ts: Some(ts),
                        },
                    })))?;
                }
            }
        }
        SyncNeedV1::Partial { version, seqs } => {
            let mut rows = prepped.query(named_params! {
                ":actor_id": actor_id,
                ":start": version,
                ":end": version,
            })?;

            match rows.next()? {
                Some(row) => {
                    let version: Version = row.get(0)?;
                    let end_version: Option<Version> = row.get(1)?;
                    // ts could be null, since we previously didn't store timestamp for empties
                    let ts: Option<Timestamp> = row.get(3)?;
                    let ts: Timestamp =
                        ts.unwrap_or(Timestamp::from(agent.clock().new_timestamp()));

                    if end_version.is_some() {
                        // send this one right away

                        sender.blocking_send(SyncMessage::V1(SyncMessageV1::Changeset(
                            ChangeV1 {
                                actor_id,
                                changeset: Changeset::Empty {
                                    versions: version..=version,
                                    ts: Some(ts),
                                },
                            },
                        )))?;
                        return Ok(());
                    }

                    // since this is not a cleared version, those aren't supposed to fail!
                    let last_seq: CrsqlSeq = row.get(2)?;

                    for range_needed in seqs {
                        let mut prepped = tx.prepare_cached(
                            r#"
                                SELECT c."table", c.pk, c.cid, c.val, c.col_version, c.db_version, c.seq, c.site_id, c.cl
                                    FROM __corro_bookkeeping AS bk
                                INNER JOIN crsql_changes AS c ON c.site_id = bk.actor_id AND c.db_version = bk.db_version
                                    WHERE bk.actor_id = :actor_id
                                    AND bk.start_version = :version
                                    AND c.seq BETWEEN :start AND :end
                                    ORDER BY c.seq ASC
                            "#,
                        )?;

                        let rows = prepped.query_map(
                            named_params! {
                                ":actor_id": actor_id,
                                ":version": version,
                                ":start": range_needed.start(),
                                ":end": range_needed.end(),
                            },
                            row_to_change,
                        )?;

                        if let Some(empty) = send_change_chunks(
                            sender,
                            ChunkedChanges::new(
                                rows,
                                *range_needed.start(),
                                *range_needed.end(),
                                MAX_CHANGES_BYTES_PER_MESSAGE,
                            ),
                            actor_id,
                            version,
                            last_seq,
                            ts,
                        )? {
                            sender.blocking_send(SyncMessage::V1(SyncMessageV1::Changeset(
                                ChangeV1 {
                                    actor_id,
                                    changeset: Changeset::Empty {
                                        versions: empty..=empty,
                                        ts: Some(ts),
                                    },
                                },
                            )))?;
                        }
                    }
                }
                None => {
                    for seqs_range in seqs {
                        let seqs = tx
                            .prepare_cached(
                                "
                                SELECT start_seq, end_seq, last_seq, ts
                                    FROM __corro_seq_bookkeeping
                                    WHERE
                                        site_id = :actor_id AND
                                        version = :version AND
                                        (
                                            -- [:start]---[start_seq]---[:end]
                                            ( start_seq BETWEEN :start AND :end ) OR
                        
                                            -- [start_seq]---[:start]---[:end]---[end_seq]
                                            ( start_seq <= :start AND end_seq >= :end ) OR
                        
                                            -- [:start]---[start_seq]---[:end]---[end_seq]
                                            ( start_seq <= :end AND end_seq >= :end ) OR
                        
                                            -- [:start]---[end_seq]---[:end]
                                            ( end_seq BETWEEN :start AND :end )
                                        )
                                ",
                            )?
                            .query_map(
                                named_params! {
                                    ":actor_id": actor_id,
                                    ":version": version,
                                    ":start": seqs_range.start(),
                                    ":end": seqs_range.end(),
                                },
                                |row| Ok((row.get(0)?..=row.get(1)?, row.get(2)?, row.get(3)?)),
                            )?
                            .collect::<rusqlite::Result<
                                Vec<(RangeInclusive<CrsqlSeq>, CrsqlSeq, Timestamp)>,
                            >>()?;

                        for (range_needed, last_seq, ts) in seqs {
                            let mut prepped = tx.prepare_cached(
                                        r#"
                                            SELECT "table", pk, cid, val, col_version, db_version, seq, site_id, cl
                                                FROM __corro_buffered_changes
                                                WHERE site_id = :actor_id
                                                    AND version = :version
                                                    AND seq BETWEEN :start_seq AND :end_seq
                                                ORDER BY seq ASC
                                        "#,
                                    )?;

                            // scope query to only the sequences we have
                            let start_seq = cmp::max(range_needed.start(), seqs_range.start());
                            let end_seq = cmp::min(range_needed.end(), seqs_range.end());

                            let rows = prepped.query_map(
                                named_params! {
                                    ":actor_id": actor_id,
                                    ":version": version,
                                    ":start_seq": start_seq,
                                    ":end_seq": end_seq
                                },
                                row_to_change,
                            )?;

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
                        }
                    }
                }
            }
        }
        SyncNeedV1::Empty { ts, .. } => {
            if last_cleared_ts.is_none() {
                return Ok(());
            }
            debug!("processing empty versions to {actor_id} with ts: {:?}", ts);
            let ts = ts.unwrap_or(Default::default());
            let mut stmt = tx.prepare_cached(
                "
                SELECT start_version, end_version, ts FROM __corro_bookkeeping
                    WHERE actor_id = crsql_site_id() AND end_version IS NOT NULL AND ts > ?
                    ORDER BY ts",
            )?;
            let rows = stmt
                .query_map([ts], |row| {
                    Ok((Version(row.get(0)?)..=Version(row.get(1)?), row.get(2)?))
                })?
                .collect::<rusqlite::Result<Vec<(RangeInclusive<Version>, Timestamp)>>>()?
                .iter()
                .fold(HashMap::new(), |mut acc, item| {
                    acc.entry(item.1)
                        .and_modify(|arr: &mut Vec<RangeInclusive<Version>>| {
                            arr.push(item.clone().0)
                        })
                        .or_insert(vec![item.clone().0]);
                    acc
                });

            let mut rows = Vec::from_iter(rows.iter());
            rows.sort_by(|a, b| a.0.cmp(b.0));
            let mut count = 0;
            for (ts, versions) in rows {
                sender.blocking_send(SyncMessage::V1(SyncMessageV1::Changeset(ChangeV1 {
                    actor_id: agent.actor_id(),
                    changeset: Changeset::EmptySet {
                        versions: versions.clone(),
                        ts: *ts,
                    },
                })))?;
                count += versions.len();
            }

            debug!("sent {count} empty versions during sync!");
        }
    }

    Ok(())
}

fn send_change_chunks<I: Iterator<Item = rusqlite::Result<Change>>>(
    sender: &Sender<SyncMessage>,
    mut chunked: ChunkedChanges<I>,
    actor_id: ActorId,
    version: Version,
    last_seq: CrsqlSeq,
    ts: Timestamp,
) -> eyre::Result<Option<Version>> {
    let mut max_buf_size = chunked.max_buf_size();
    loop {
        if sender.is_closed() {
            eyre::bail!("sync message sender channel is closed");
        }
        match chunked.next() {
            Some(Ok((changes, seqs))) => {
                let start = Instant::now();

                if changes.is_empty() && *seqs.start() == CrsqlSeq(0) && *seqs.end() == last_seq {
                    return Ok(Some(version));
                } else {
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
                }

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
                error!(%actor_id, %version, "could not process changes to send via sync: {e}");
                break;
            }
            None => {
                break;
            }
        }
    }

    Ok(None)
}

async fn process_sync(
    agent: Agent,
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

    let last_ts = agent
        .clone()
        .booked()
        .read("process_sync(read_cleared_ts))")
        .await
        .last_cleared_ts();
    loop {
        enum Branch {
            Reqs(Vec<Vec<(ActorId, Vec<SyncNeedV1>)>>),
        }

        let branch = tokio::select! {
            maybe_reqs = chunked_reqs.next() => match maybe_reqs {
                Some(reqs) => {
                    Branch::Reqs(reqs)
                },
                None => break
            },
            Some(res) = buf.next() => {
                res?;
                continue;
            },
            else => {
                break;
            }
        };

        match branch {
            Branch::Reqs(reqs) => {
                let agg = reqs
                    .into_iter()
                    .flatten()
                    .group_by(|req| req.0)
                    .into_iter()
                    .map(|(actor_id, reqs)| (actor_id, reqs.flat_map(|(_, reqs)| reqs).collect()))
                    .collect::<Vec<(ActorId, Vec<SyncNeedV1>)>>();

                for (actor_id, needs) in agg {
                    let booked = bookie
                        .read("process_sync get actor")
                        .await
                        .get(&actor_id)
                        .cloned();
                    let booked = match booked {
                        Some(b) => b,
                        None => continue,
                    };
                    let booked_read = booked.read("process_sync check needs").await;

                    for need in needs {
                        match &need {
                            SyncNeedV1::Full { versions } => {
                                if versions.clone().all(|v| booked_read.needed().contains(&v)) {
                                    continue;
                                }
                            }
                            SyncNeedV1::Partial { version, .. } => {
                                if booked_read.needed().contains(version) {
                                    continue;
                                }
                            }
                            SyncNeedV1::Empty { ts } => {
                                debug!("received empty need with ts: {ts:?}");
                            }
                        }

                        let pool = pool.clone();
                        let sender = sender.clone();

                        let agent = agent.clone();
                        let fut = Box::pin(async move {
                            let mut conn = pool.read().await?;

                            block_in_place(|| {
                                handle_need(&mut conn, &agent, actor_id, need, &sender, last_ts)
                            })?;

                            Ok(())
                        });

                        if job_tx.send(fut).is_err() {
                            eyre::bail!("could not send into job channel");
                        }
                    }
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

fn chunk_range<T: std::iter::Step + std::ops::Add<u64, Output = T> + std::cmp::Ord + Copy>(
    range: RangeInclusive<T>,
    chunk_size: usize,
) -> impl Iterator<Item = RangeInclusive<T>> {
    range.clone().step_by(chunk_size).map(move |block_start| {
        let block_end = (block_start + chunk_size as u64).min(*range.end());
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
    counter!("corro.sync.chunk.sent.bytes").increment(len as u64);

    Ok(())
}

#[tracing::instrument(skip(read), fields(buf_size = tracing::field::Empty), err)]
pub async fn read_sync_msg<R: Stream<Item = std::io::Result<BytesMut>> + Unpin>(
    read: &mut R,
) -> Result<Option<SyncMessage>, SyncRecvError> {
    match read.next().await {
        Some(buf_res) => match buf_res {
            Ok(mut buf) => {
                counter!("corro.sync.chunk.recv.bytes").increment(buf.len() as u64);
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
    our_empty_ts: HashMap<ActorId, Option<Timestamp>>,
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
                        BiPayload::V1 {data: BiPayloadV1::SyncStart {actor_id: agent.actor_id(), trace_ctx}, cluster_id: agent.cluster_id()},
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

                    counter!("corro.sync.client.member", "id" => actor_id.to_string(), "addr" => addr.to_string()).increment(1);

                    let mut needs = our_sync_state.compute_available_needs(&their_sync_state);

                    trace!(%actor_id, self_actor_id = %agent.actor_id(), "computed needs");

                    let cleared_ts = their_sync_state.last_cleared_ts;

                    if let Some(ts) = cleared_ts {
                        if let Some(last_seen) = our_empty_ts.get(&actor_id) {
                            if last_seen.is_none() || last_seen.unwrap() < ts {
                                debug!(%actor_id, "got last cleared ts {cleared_ts:?} - out last_seen {last_seen:?}");
                                needs.entry(actor_id).or_default().push( SyncNeedV1::Empty { ts: *last_seen });
                            }
                        }
                    }

                    Ok::<_, SyncError>((needs, tx, read))
                }.await
            )
        }.instrument(info_span!("sync_client_handshake", %actor_id, %addr))
    }))
    .collect::<Vec<(ActorId, SocketAddr, Result<_, SyncError>)>>()
    .await;

    debug!("collected member needs and such!");

    #[allow(clippy::manual_try_fold)]
    let syncers = results
        .into_iter()
        .fold(Ok(vec![]), |agg, (actor_id, addr, res)| match res {
            Ok((needs, tx, read)) => {
                let mut v = agg.unwrap_or_default();
                v.push((actor_id, addr, needs, tx, read));
                Ok(v)
            }
            Err(e) => {
                counter!(
                    "corro.sync.client.handshake.errors",
                    "actor_id" => actor_id.to_string(),
                    "addr" => addr.to_string(),
                    "error" => e.to_string()
                )
                .increment(1);
                match agg {
                    Ok(v) if !v.is_empty() => Ok(v),
                    _ => Err(e),
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
                    SyncNeedV1::Full {versions} => (versions.end().0 - versions.start().0) as usize + 1,
                    SyncNeedV1::Partial {..} => 0,
                    SyncNeedV1::Empty {..} => 0,
                }).sum::<usize>()).sum::<usize>());

                let actor_needs = needs
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
                    .collect::<VecDeque<_>>();

                servers.push((
                    actor_id,
                    addr,
                    actor_needs,
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
        let mut req_full: HashMap<ActorId, RangeInclusiveSet<Version>> = HashMap::new();

        // already requested partial version sequences
        let mut req_partials: HashMap<(ActorId, Version), RangeInclusiveSet<CrsqlSeq>> = HashMap::new();

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

                let mut drained = 0;

                while drained < 10 {
                    let (actor_id, need) = match needs.pop_front() {
                        Some(popped) => popped,
                        None => {
                            break;
                        }
                    };

                    drained += 1;

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
                                    range.insert(versions.clone());
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
                                        range.insert(seqs.clone());
                                        seqs
                                    })
                                    .collect(),
                            }]
                        }
                        need => {vec![need]},
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

                    counter!("corro.sync.client.req.sent", "actor_id" => server_actor_id.to_string()).increment(req_len as u64);
                }

                if !send_buf.is_empty() {
                    if let Err(e) = write_buf(&mut send_buf, &mut tx).await {
                        error!(%server_actor_id, %addr, "could not write sync requests: {e} (elapsed: {:?})", start.elapsed());
                        continue;
                    }
                } else {
                    // give some reprieve
                    tokio::task::yield_now().await;
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
        let tx_emptyset = agent.tx_emptyset().clone();

        async move {
            let mut count = 0;
            let mut last_empty_ts = None;
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
                            counter!("corro.sync.changes.recv", "actor_id" => actor_id.to_string())
                                .increment(changes_len as u64);

                            debug!(
                                "handling versions: {:?}, seqs: {:?}, len: {changes_len}",
                                change.versions(),
                                change.seqs()
                            );
                            // only accept emptyset that's from the same node that's syncing
                            if change.is_empty_set() {
                                let change_ts = change.ts();
                                tx_emptyset
                                    .send(change)
                                    .await
                                    .map_err(|_| SyncRecvError::ChangesChannelClosed)?;

                                if change_ts > last_empty_ts {
                                    last_empty_ts = change_ts;
                                }
                                continue;
                            }

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

            Ok((actor_id, count, last_empty_ts))
        }
        .instrument(info_span!("read_sync_requests_responses", %actor_id))
    }))
    .collect::<Vec<Result<(ActorId, usize, Option<Timestamp>), SyncError>>>()
    .await;

    let mut members = agent.members().write();
    let ts = Timestamp::from(agent.clock().new_timestamp());
    for res in counts.iter() {
        match res {
            Err(e) => error!("could not properly recv from peer: {e}"),
            Ok((actor_id, _, last_empty_ts)) => {
                members.update_sync_ts(actor_id, ts);
                members.update_last_empty(actor_id, *last_empty_ts);
            }
        };
    }

    Ok(counts
        .into_iter()
        .flat_map(|res| res.map(|i| i.1))
        .sum::<usize>())
}

#[tracing::instrument(skip(agent, bookie, their_actor_id, read, write), fields(actor_id = %their_actor_id), err)]
pub async fn serve_sync(
    agent: &Agent,
    bookie: &Bookie,
    their_actor_id: ActorId,
    trace_ctx: SyncTraceContextV1,
    cluster_id: ClusterId,
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

    if cluster_id != agent.cluster_id() {
        encode_write_sync_msg(
            &mut codec,
            &mut encode_buf,
            &mut send_buf,
            SyncMessage::V1(SyncMessageV1::Rejection(SyncRejectionV1::DifferentCluster)),
            &mut write,
        )
        .instrument(info_span!("write_rejection_cluster_id"))
        .await?;
        return Ok(0);
    }

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

    let _permit = match agent.limits().sync.try_acquire() {
        Ok(permit) => permit,
        Err(_) => {
            // no permits!
            encode_write_sync_msg(
                &mut codec,
                &mut encode_buf,
                &mut send_buf,
                SyncMessage::V1(SyncMessageV1::Rejection(
                    SyncRejectionV1::MaxConcurrencyReached,
                )),
                &mut write,
            )
            .instrument(info_span!("write_sync_rejection"))
            .await?;
            return Ok(0);
        }
    };

    let sync_state = generate_sync(bookie, agent.actor_id()).await;

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
            agent.clone(),
            agent.pool().clone(),
            bookie.clone(),
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

            counter!("corro.sync.changes.sent", "actor_id" => their_actor_id.to_string()).increment(count as u64);

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

            counter!("corro.sync.requests.recv", "actor_id" => their_actor_id.to_string()).increment(count as u64);

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
        base::CrsqlDbVersion,
        config::{Config, TlsConfig, DEFAULT_GOSSIP_CLIENT_ADDR},
        pubsub::pack_columns,
        tls::{generate_ca, generate_client_cert, generate_server_cert},
    };
    use hyper::StatusCode;
    use rand::{Rng, RngCore};
    use tempfile::TempDir;
    use tripwire::Tripwire;

    use crate::{
        agent::{process_multiple_changes, setup},
        api::public::api_v1_db_schema,
    };

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_need() -> eyre::Result<()> {
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
            db_version: CrsqlDbVersion(1),
            seq: CrsqlSeq(0),
            site_id: actor_id.to_bytes(),
            cl: 1,
        };

        let change2 = Change {
            table: TableName("tests".into()),
            pk: pack_columns(&vec![2i64.into()])?,
            cid: ColumnName("text".into()),
            val: "two".into(),
            col_version: 1,
            db_version: CrsqlDbVersion(2),
            seq: CrsqlSeq(0),
            site_id: actor_id.to_bytes(),
            cl: 1,
        };

        let bookie = Bookie::new(Default::default());

        process_multiple_changes(
            agent.clone(),
            bookie.clone(),
            vec![
                (
                    ChangeV1 {
                        actor_id,
                        changeset: Changeset::Full {
                            version: Version(1),
                            changes: vec![change1.clone()],
                            seqs: CrsqlSeq(0)..=CrsqlSeq(0),
                            last_seq: CrsqlSeq(0),
                            ts,
                        },
                    },
                    ChangeSource::Sync,
                    Instant::now(),
                ),
                (
                    ChangeV1 {
                        actor_id,
                        changeset: Changeset::Full {
                            version: Version(2),
                            changes: vec![change2.clone()],
                            seqs: CrsqlSeq(0)..=CrsqlSeq(0),
                            last_seq: CrsqlSeq(0),
                            ts,
                        },
                    },
                    ChangeSource::Sync,
                    Instant::now(),
                ),
            ],
        )
        .await?;

        let booked = bookie.read("test").await.get(&actor_id).cloned().unwrap();

        {
            let read = booked.read("test").await;

            assert!(read.contains_version(&Version(1)));
            assert!(read.contains_version(&Version(2)));
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
                handle_need(
                    &mut conn,
                    &agent,
                    actor_id,
                    SyncNeedV1::Full {
                        versions: Version(1)..=Version(1),
                    },
                    &tx,
                    None,
                )
            })?;

            let msg = rx.recv().await.unwrap();
            assert_eq!(
                msg,
                SyncMessage::V1(SyncMessageV1::Changeset(ChangeV1 {
                    actor_id,
                    changeset: Changeset::Full {
                        version: Version(1),
                        changes: vec![change1],
                        seqs: CrsqlSeq(0)..=CrsqlSeq(0),
                        last_seq: CrsqlSeq(0),
                        ts,
                    }
                }))
            );

            block_in_place(|| {
                handle_need(
                    &mut conn,
                    &agent,
                    actor_id,
                    SyncNeedV1::Partial {
                        version: Version(2),
                        seqs: vec![CrsqlSeq(0)..=CrsqlSeq(0)],
                    },
                    &tx,
                    None,
                )
            })?;

            let msg = rx.recv().await.unwrap();
            assert_eq!(
                msg,
                SyncMessage::V1(SyncMessageV1::Changeset(ChangeV1 {
                    actor_id,
                    changeset: Changeset::Full {
                        version: Version(2),
                        changes: vec![change2.clone()],
                        seqs: CrsqlSeq(0)..=CrsqlSeq(0),
                        last_seq: CrsqlSeq(0),
                        ts,
                    }
                }))
            );
        }

        let change3 = Change {
            table: TableName("tests".into()),
            pk: pack_columns(&vec![1i64.into()])?,
            cid: ColumnName("text".into()),
            val: "one override".into(),
            col_version: 2,
            db_version: CrsqlDbVersion(3),
            seq: CrsqlSeq(0),
            site_id: actor_id.to_bytes(),
            cl: 1,
        };

        process_multiple_changes(
            agent.clone(),
            bookie.clone(),
            vec![(
                ChangeV1 {
                    actor_id,
                    changeset: Changeset::Full {
                        version: Version(3),
                        changes: vec![change3.clone()],
                        seqs: CrsqlSeq(0)..=CrsqlSeq(0),
                        last_seq: CrsqlSeq(0),
                        ts,
                    },
                },
                ChangeSource::Sync,
                Instant::now(),
            )],
        )
        .await?;

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
                handle_need(
                    &mut conn,
                    &agent,
                    actor_id,
                    SyncNeedV1::Partial {
                        version: Version(1),
                        seqs: vec![CrsqlSeq(0)..=CrsqlSeq(0)],
                    },
                    &tx,
                    None,
                )
            })?;

            let msg = rx.recv().await.unwrap();
            if let SyncMessage::V1(SyncMessageV1::Changeset(ChangeV1 {
                actor_id: actor,
                changeset,
                ..
            })) = msg
            {
                assert_eq!(actor_id, actor);
                assert!(changeset.is_empty());
                assert_eq!(changeset.versions(), Version(1)..=Version(1));
            } else {
                panic!("{msg:?} doesn't contain an empty changeset");
            }
        }

        {
            let (tx, mut rx) = mpsc::channel(5);
            let mut conn = agent.pool().read().await?;

            block_in_place(|| {
                handle_need(
                    &mut conn,
                    &agent,
                    actor_id,
                    SyncNeedV1::Full {
                        versions: Version(1)..=Version(6),
                    },
                    &tx,
                    None,
                )
            })?;

            let msg = rx.recv().await.unwrap();
            assert_eq!(
                msg,
                SyncMessage::V1(SyncMessageV1::Changeset(ChangeV1 {
                    actor_id,
                    changeset: Changeset::Full {
                        version: Version(2),
                        changes: vec![change2],
                        seqs: CrsqlSeq(0)..=CrsqlSeq(0),
                        last_seq: CrsqlSeq(0),
                        ts,
                    }
                }))
            );

            let msg = rx.recv().await.unwrap();
            assert_eq!(
                msg,
                SyncMessage::V1(SyncMessageV1::Changeset(ChangeV1 {
                    actor_id,
                    changeset: Changeset::Full {
                        version: Version(3),
                        changes: vec![change3.clone()],
                        seqs: CrsqlSeq(0)..=CrsqlSeq(0),
                        last_seq: CrsqlSeq(0),
                        ts,
                    }
                }))
            );

            let msg = rx.recv().await.unwrap();
            if let SyncMessage::V1(SyncMessageV1::Changeset(ChangeV1 {
                actor_id: actor,
                changeset,
                ..
            })) = msg
            {
                assert_eq!(actor_id, actor);
                assert!(changeset.is_empty());
                assert_eq!(changeset.versions(), Version(1)..=Version(1));
            } else {
                panic!("{msg:?} doesn't contain an empty changeset");
            }
        }

        // overwrite v2
        let change4 = Change {
            table: TableName("tests".into()),
            pk: pack_columns(&vec![2i64.into()])?,
            cid: ColumnName("text".into()),
            val: "two override".into(),
            col_version: 2,
            db_version: CrsqlDbVersion(4),
            seq: CrsqlSeq(0),
            site_id: actor_id.to_bytes(),
            cl: 1,
        };

        process_multiple_changes(
            agent.clone(),
            bookie.clone(),
            vec![(
                ChangeV1 {
                    actor_id,
                    changeset: Changeset::Full {
                        version: Version(4),
                        changes: vec![change4.clone()],
                        seqs: CrsqlSeq(0)..=CrsqlSeq(0),
                        last_seq: CrsqlSeq(0),
                        ts,
                    },
                },
                ChangeSource::Sync,
                Instant::now(),
            )],
        )
        .await?;

        let ts2 = agent.clock().new_timestamp().into();

        let mut rng = rand::thread_rng();

        let len = 10u64;
        let mut last_seq = 0u64;

        let changes = (0u64..len)
            .map(|i| {
                let mut b = vec![0u8; 16];
                rng.fill_bytes(&mut b);

                let int: i64 = rng.sample(rand::distributions::Standard);
                let float: f64 = rng.sample(rand::distributions::Standard);
                [
                    ("int", int.into()),
                    ("float", float.into()),
                    ("blob", b.into()),
                ]
                .into_iter()
                .map(|(col, val)| {
                    let c = Change {
                        table: TableName("wide".into()),
                        pk: pack_columns(&vec![
                            i.to_be_bytes().to_vec().into(),
                            i.to_string().into(),
                        ])
                        .unwrap(),
                        cid: ColumnName(col.into()),
                        val,
                        col_version: 1,
                        db_version: CrsqlDbVersion(5),
                        seq: CrsqlSeq(last_seq),
                        site_id: actor_id.to_bytes(),
                        cl: 1,
                    };
                    last_seq += 1;
                    c
                })
                .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        last_seq -= 1;
        let last_seq = CrsqlSeq(last_seq);

        let changes_v1 = changes
            .iter()
            .map(|changes| {
                let seqs = changes.first().unwrap().seq..=changes.last().unwrap().seq;
                ChangeV1 {
                    actor_id,
                    changeset: Changeset::Full {
                        version: Version(5),
                        changes: changes.clone(),
                        seqs,
                        last_seq,
                        ts: ts2,
                    },
                }
            })
            .collect::<Vec<_>>();

        process_multiple_changes(
            agent.clone(),
            bookie.clone(),
            changes_v1
                .iter()
                .map(|change| (change.clone(), ChangeSource::Sync, Instant::now()))
                .collect(),
        )
        .await?;

        {
            let (tx, mut rx) = mpsc::channel(5);
            let mut conn = agent.pool().read().await?;

            block_in_place(|| {
                handle_need(
                    &mut conn,
                    &agent.clone(),
                    actor_id,
                    SyncNeedV1::Full {
                        versions: Version(1)..=Version(1000),
                    },
                    &tx,
                    None,
                )
            })?;

            let msg = rx.recv().await.unwrap();
            assert_eq!(
                msg,
                SyncMessage::V1(SyncMessageV1::Changeset(ChangeV1 {
                    actor_id,
                    changeset: Changeset::Full {
                        version: Version(3),
                        changes: vec![change3],
                        seqs: CrsqlSeq(0)..=CrsqlSeq(0),
                        last_seq: CrsqlSeq(0),
                        ts,
                    }
                }))
            );

            let msg = rx.recv().await.unwrap();
            assert_eq!(
                msg,
                SyncMessage::V1(SyncMessageV1::Changeset(ChangeV1 {
                    actor_id,
                    changeset: Changeset::Full {
                        version: Version(4),
                        changes: vec![change4],
                        seqs: CrsqlSeq(0)..=CrsqlSeq(0),
                        last_seq: CrsqlSeq(0),
                        ts,
                    }
                }))
            );

            let msg = rx.recv().await.unwrap();
            assert_eq!(
                msg,
                SyncMessage::V1(SyncMessageV1::Changeset(ChangeV1 {
                    actor_id,
                    changeset: Changeset::Full {
                        version: Version(5),
                        changes: changes.iter().flatten().cloned().collect(),
                        seqs: CrsqlSeq(0)..=last_seq,
                        last_seq,
                        ts: ts2,
                    }
                }))
            );

            let msg = rx.recv().await.unwrap();
            if let SyncMessage::V1(SyncMessageV1::Changeset(ChangeV1 {
                actor_id: actor,
                changeset,
                ..
            })) = msg
            {
                assert_eq!(actor_id, actor);
                assert!(changeset.is_empty());
                assert_eq!(changeset.versions(), Version(1)..=Version(2));
            } else {
                panic!("{msg:?} doesn't contain an empty changeset");
            }
        }

        {
            let (tx, mut rx) = mpsc::channel(5);
            let mut conn = agent.pool().read().await?;

            block_in_place(|| {
                handle_need(
                    &mut conn,
                    &agent.clone(),
                    actor_id,
                    SyncNeedV1::Partial {
                        version: Version(5),
                        seqs: vec![CrsqlSeq(4)..=CrsqlSeq(7)],
                    },
                    &tx,
                    None,
                )
            })?;

            let msg = rx.recv().await.unwrap();
            assert_eq!(
                msg,
                SyncMessage::V1(SyncMessageV1::Changeset(ChangeV1 {
                    actor_id,
                    changeset: Changeset::Full {
                        version: Version(5),
                        changes: changes
                            .iter()
                            .flatten()
                            .enumerate()
                            .filter_map(|(i, c)| if (4..=7).contains(&i) {
                                Some(c.clone())
                            } else {
                                None
                            })
                            .collect(),
                        seqs: CrsqlSeq(4)..=CrsqlSeq(7),
                        last_seq,
                        ts: ts2,
                    }
                }))
            );

            block_in_place(|| {
                handle_need(
                    &mut conn,
                    &agent,
                    actor_id,
                    SyncNeedV1::Partial {
                        version: Version(5),
                        seqs: vec![CrsqlSeq(2)..=CrsqlSeq(2), CrsqlSeq(15)..=CrsqlSeq(24)],
                    },
                    &tx,
                    None,
                )
            })?;

            let msg = rx.recv().await.unwrap();
            assert_eq!(
                msg,
                SyncMessage::V1(SyncMessageV1::Changeset(ChangeV1 {
                    actor_id,
                    changeset: Changeset::Full {
                        version: Version(5),
                        changes: changes
                            .iter()
                            .flatten()
                            .enumerate()
                            .filter_map(|(i, c)| if i == 2 { Some(c.clone()) } else { None })
                            .collect(),
                        seqs: CrsqlSeq(2)..=CrsqlSeq(2),
                        last_seq,
                        ts: ts2,
                    }
                }))
            );

            let msg = rx.recv().await.unwrap();
            assert_eq!(
                msg,
                SyncMessage::V1(SyncMessageV1::Changeset(ChangeV1 {
                    actor_id,
                    changeset: Changeset::Full {
                        version: Version(5),
                        changes: changes
                            .iter()
                            .flatten()
                            .enumerate()
                            .filter_map(|(i, c)| if (15..=24).contains(&i) {
                                Some(c.clone())
                            } else {
                                None
                            })
                            .collect(),
                        seqs: CrsqlSeq(15)..=CrsqlSeq(24),
                        last_seq,
                        ts: ts2,
                    }
                }))
            );
        }

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
            client_addr: DEFAULT_GOSSIP_CLIENT_ADDR,
            external_addr: None,
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
