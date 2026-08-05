use corro_types::{
    agent::Agent,
    broadcast::{
        BroadcastV1, ChangeSource, ChangeV1, PlumtreeInput, PlumtreeMsgV1, PlumtreeWire,
        UniPayload, UniPayloadV1,
    },
    config::BroadcastMethod,
};
use metrics::counter;
use plum_foca::GossipMsg;
use speedy::Readable;
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};
use tracing::{debug, error, trace, warn};
use tripwire::Tripwire;

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
                let rx = tokio::select! {
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

                trace!(
                    "accepted a unidirectional stream from {}",
                    conn.remote_address()
                );

                let change_dict = change_dict.load_full();

                tokio::spawn({
                    let tx_changes = tx_changes.clone();
                    let tx_plumtree = tx_plumtree.clone();
                    let broadcast_method = broadcast_method;
                    async move {
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
                                                    let tx = tx_plumtree.clone();

                                                    match broadcast_method {
                                                        // route gossips if we are using broadcast method
                                                        BroadcastMethod::Gossip => {
                                                            if let PlumtreeMsgV1::Gossip(msg) =
                                                                wire_msg
                                                            {
                                                                warn!("broadcast algorithm set to gossip but node receieved plumtree message");
                                                                changes.push((
                                                                    msg.payload,
                                                                    ChangeSource::Broadcast,
                                                                    None,
                                                                ));
                                                            }
                                                        }
                                                        BroadcastMethod::Plumtree => {
                                                            if let Err(e) = tx
                                                                .send(PlumtreeInput::Wire(wire_msg))
                                                                .await
                                                            {
                                                                error!(
                                                                    "could not route Plumtree msg: {e}"
                                                                );
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
                                warn!("broadcast algorithm set to plumtree but node receieved gossip message.");
                                for (change, _, _) in changes.into_iter().rev() {
                                    let wire = PlumtreeMsgV1::Gossip(GossipMsg {
                                        round: 1,
                                        sender: change.actor_id,
                                        payload: change,
                                    });
                                    if let Err(e) =
                                        tx_plumtree.send(PlumtreeInput::Wire(wire)).await
                                    {
                                        error!(
                                            "could not route legacy gossip change to plumtree: {e}"
                                        );
                                        return;
                                    }
                                }
                            }
                            BroadcastMethod::Gossip => {
                                for change in changes.into_iter().rev() {
                                    if let Err(e) = tx_changes.send(change).await {
                                        error!("could not send change for processing: {e}");
                                        return;
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
