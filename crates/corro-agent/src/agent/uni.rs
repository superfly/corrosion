use std::sync::atomic::AtomicU64;

use corro_types::{
    agent::Agent,
    broadcast::{
        BroadcastV1, ChangeSource, ChangeV1, PlumtreeInput, PlumtreeMsgV1, PlumtreePayload,
        PlumtreeWire, UniPayload, UniPayloadV1,
    },
    config::BroadcastMethod,
};
use metrics::counter;
use plum_foca::{GossipMsg, Payload};
use speedy::Readable;
use tokio::sync::{mpsc::error::TrySendError, Semaphore};
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};
use tracing::{debug, error, trace, warn};
use tripwire::Tripwire;

use crate::agent::util::should_log_pow_10;

static LOG_COUNTER: AtomicU64 = AtomicU64::new(0);
static DROP_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);
static REJECT_LOG_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Max uni streams being processed at once, across all connections.
/// Streams arriving at the cap are stopped instead of queued.
const MAX_INFLIGHT_UNI_STREAMS: usize = 2_000;
static UNI_STREAM_PERMITS: Semaphore = Semaphore::const_new(MAX_INFLIGHT_UNI_STREAMS);

/// Application error code sent with STOP_SENDING when we're over capacity.
const UNI_STREAM_OVERLOADED: quinn::VarInt = quinn::VarInt::from_u32(1);

/// Spawn a task that accepts unidirectional broadcast streams, then
/// spawns another task for each incoming stream to handle.
pub fn spawn_unipayload_handler(tripwire: &Tripwire, conn: &quinn::Connection, agent: Agent) {
    let cluster_id = agent.cluster_id();
    let broadcast_method = agent.broadcast_method();
    let tx_changes = agent.tx_changes().clone();
    let tx_plumtree = agent.tx_plumtree().clone();
    let change_dict = agent.change_dict_slot();

    tokio::spawn({
        let conn = conn.clone();
        let mut tripwire = tripwire.clone();
        async move {
            loop {
                let mut rx = tokio::select! {
                    rx_res = conn.accept_uni() => match rx_res {
                        Ok(rx) => rx,
                        Err(e) => {
                            debug!("could not accept unidirectional stream from connection: {e}");
                            return;
                        }
                    },
                    _ = &mut tripwire => {
                        debug!("connection cancelled");
                        return;
                    }
                };

                counter!("corro.peer.stream.accept.total", "type" => "uni").increment(1);

                let permit = match UNI_STREAM_PERMITS.try_acquire() {
                    Ok(permit) => permit,
                    Err(_) => {
                        counter!("corro.peer.stream.rejected.total", "type" => "uni").increment(1);
                        if let Some(n) = should_log_pow_10(&REJECT_LOG_COUNTER) {
                            warn!(
                                remote_addr = %conn.remote_address(),
                                "too many in-flight uni streams ({MAX_INFLIGHT_UNI_STREAMS}), stopping stream (log count: {n})"
                            );
                        }
                        let _ = rx.stop(UNI_STREAM_OVERLOADED);
                        continue;
                    }
                };

                trace!(
                    "accepted a unidirectional stream from {}",
                    conn.remote_address()
                );

                let change_dict = change_dict.load_full();
                let remote_addr = conn.remote_address();

                tokio::spawn({
                    let tx_changes = tx_changes.clone();
                    let tx_plumtree = tx_plumtree.clone();
                    let broadcast_method = broadcast_method;
                    async move {
                        // held until this stream is fully processed
                        let _permit = permit;
                        let mut framed = FramedRead::new(
                            rx,
                            LengthDelimitedCodec::builder()
                                .max_frame_length(100 * 1_024 * 1_024)
                                .new_codec(),
                        );

                        let mut changes: Vec<(ChangeV1, ChangeSource, Option<BroadcastV1>)> =
                            vec![];
                        loop {
                            match StreamExt::next(&mut framed).await {
                                Some(Ok(b)) => {
                                    counter!("corro.peer.stream.bytes.recv.total", "type" => "uni")
                                        .increment(b.len() as u64);
                                    match UniPayload::read_from_buffer(&b) {
                                        Ok(payload) => {
                                            trace!("parsed a payload: {payload:?}");

                                            match payload {
                                                UniPayload::V1 {
                                                    data: UniPayloadV1::Broadcast(bcast),
                                                    cluster_id: payload_cluster_id,
                                                } => {
                                                    if cluster_id != payload_cluster_id {
                                                        continue;
                                                    }
                                                    let compressed = bcast.is_compressed();
                                                    match bcast.into_change(change_dict.as_deref())
                                                    {
                                                        Ok(change) => {
                                                            changes.push((
                                                                change,
                                                                ChangeSource::Broadcast,
                                                                compressed.then_some(bcast),
                                                            ));
                                                        }
                                                        Err(e) => {
                                                            error!(
                                                                "could not decode broadcast change: {e}"
                                                            );
                                                            continue;
                                                        }
                                                    }
                                                }
                                                UniPayload::V1 {
                                                    data:
                                                        UniPayloadV1::Plumtree(PlumtreeWire::V1 {
                                                            data: wire_msg,
                                                        }),
                                                    cluster_id: payload_cluster_id,
                                                } => {
                                                    if cluster_id != payload_cluster_id {
                                                        continue;
                                                    }

                                                    match broadcast_method {
                                                        // route gossips if we are using broadcast method
                                                        BroadcastMethod::Gossip => {
                                                            if let PlumtreeMsgV1::Gossip(msg) =
                                                                wire_msg
                                                            {
                                                                if let Some(n) =
                                                                    should_log_pow_10(&LOG_COUNTER)
                                                                {
                                                                    warn!(%remote_addr, "broadcast algorithm set to gossip but received plumtree message (log count: {n})");
                                                                }
                                                                let compressed = msg
                                                                    .payload
                                                                    .bcast
                                                                    .is_compressed();
                                                                match msg.payload.bcast.into_change(
                                                                    change_dict.as_deref(),
                                                                ) {
                                                                    Ok(change) => {
                                                                        changes.push((
                                                                            change,
                                                                            ChangeSource::Broadcast,
                                                                            compressed.then_some(
                                                                                msg.payload.bcast,
                                                                            ),
                                                                        ));
                                                                    }
                                                                    Err(e) => {
                                                                        error!(
                                                                            "could not decode plumtree broadcast change: {e}"
                                                                        );
                                                                        continue;
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        BroadcastMethod::Plumtree => {
                                                            let msg_type: &'static str =
                                                                (&wire_msg).into();
                                                            // use try_send so we don't block if the channel is full
                                                            // syncs and duplicate broadcast would recover droped changes
                                                            match tx_plumtree.try_send(
                                                                PlumtreeInput::Wire(wire_msg),
                                                            ) {
                                                                Ok(()) => {}
                                                                Err(TrySendError::Full(_)) => {
                                                                    counter!("corro.peer.stream.uni.dropped", "channel" => "plumtree", "msg_type" => msg_type).increment(1);
                                                                    if let Some(n) =
                                                                        should_log_pow_10(
                                                                            &DROP_LOG_COUNTER,
                                                                        )
                                                                    {
                                                                        warn!(msg_type, "plumtree channel full, dropped message (log count: {n})");
                                                                    }
                                                                }
                                                                Err(TrySendError::Closed(_)) => {
                                                                    error!(
                                                                        "could not route Plumtree msg: channel closed"
                                                                    );
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            error!("could not decode UniPayload: {e}");
                                            continue;
                                        }
                                    }
                                }
                                Some(Err(e)) => {
                                    error!("decode error: {e}");
                                }
                                None => break,
                            }
                        }

                        match broadcast_method {
                            BroadcastMethod::Plumtree => {
                                if !changes.is_empty() {
                                    if let Some(n) = should_log_pow_10(&LOG_COUNTER) {
                                        warn!(%remote_addr, "broadcast algorithm set to plumtree but received gossip message (log count: {n})");
                                    }
                                }
                                for (change, _, original_bcast) in changes.into_iter().rev() {
                                    let id = change.message_id();
                                    let bcast = original_bcast
                                        .unwrap_or_else(|| BroadcastV1::Change(change));
                                    let wire = PlumtreeMsgV1::Gossip(GossipMsg {
                                        round: 1,
                                        sender: id.actor_id,
                                        payload: PlumtreePayload { id, bcast },
                                    });
                                    let msg_type: &'static str = (&wire).into();
                                    match tx_plumtree.try_send(PlumtreeInput::Wire(wire)) {
                                        Ok(()) => {}
                                        Err(TrySendError::Full(_)) => {
                                            counter!("corro.peer.stream.uni.dropped", "channel" => "plumtree", "msg_type" => msg_type).increment(1);
                                            if let Some(n) = should_log_pow_10(&DROP_LOG_COUNTER) {
                                                warn!(msg_type, "plumtree channel full, dropped legacy gossip change (log count: {n})");
                                            }
                                        }
                                        Err(TrySendError::Closed(_)) => {
                                            error!(
                                                "could not route legacy gossip change to plumtree: channel closed"
                                            );
                                            return;
                                        }
                                    }
                                }
                            }
                            BroadcastMethod::Gossip => {
                                for change in changes.into_iter().rev() {
                                    match tx_changes.try_send(change) {
                                        Ok(()) => {}
                                        Err(TrySendError::Full(_)) => {
                                            counter!("corro.peer.stream.uni.dropped", "channel" => "changes", "msg_type" => "change").increment(1);
                                            if let Some(n) = should_log_pow_10(&DROP_LOG_COUNTER) {
                                                warn!("changes channel full, dropped broadcast change (log count: {n})");
                                            }
                                        }
                                        Err(TrySendError::Closed(_)) => {
                                            error!("could not send change for processing: channel closed");
                                            return;
                                        }
                                    }
                                }
                            }
                        }
                    }
                });
            }
        }
    });
}
