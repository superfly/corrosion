use corro_types::{
    agent::Agent,
    broadcast::{BroadcastV1, ChangeSource, PlumtreeInput, PlumtreeMsg, UniPayload, UniPayloadV1},
};
use metrics::counter;
use speedy::Readable;
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};
use tracing::{debug, error, trace};
use tripwire::Tripwire;

/// Spawn a task that accepts unidirectional broadcast streams, then
/// spawns another task for each incoming stream to handle.
pub fn spawn_unipayload_handler(tripwire: &Tripwire, conn: &quinn::Connection, agent: Agent) {
    let cluster_id = agent.cluster_id();
    let tx_changes = agent.tx_changes().clone();
    let tx_plumtree = agent.tx_plumtree().clone();

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

                tokio::spawn({
                    let tx_changes = tx_changes.clone();
                    let tx_plumtree = tx_plumtree.clone();
                    async move {
                        let mut framed = FramedRead::new(
                            rx,
                            LengthDelimitedCodec::builder()
                                .max_frame_length(100 * 1_024 * 1_024)
                                .new_codec(),
                        );

                        let mut changes = vec![];
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
                                                    data:
                                                        UniPayloadV1::Broadcast(BroadcastV1::Change(
                                                            change,
                                                        )),
                                                    cluster_id: payload_cluster_id,
                                                } => {
                                                    if cluster_id != payload_cluster_id {
                                                        continue;
                                                    }
                                                    changes.push((change, ChangeSource::Broadcast));
                                                }
                                                UniPayload::V1 {
                                                    data:
                                                        UniPayloadV1::PlumTree(PlumtreeMsg::V1 {
                                                            data: wire_msg,
                                                        }),
                                                    cluster_id: payload_cluster_id,
                                                } => {
                                                    if cluster_id != payload_cluster_id {
                                                        continue;
                                                    }
                                                    let tx = tx_plumtree.clone();
                                                    tokio::spawn(async move {
                                                        if let Err(e) = tx
                                                            .send(PlumtreeInput::Wire(wire_msg))
                                                            .await
                                                        {
                                                            error!(
                                                                "could not route PlumTree msg: {e}"
                                                            );
                                                        }
                                                    });
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

                        for change in changes.into_iter().rev() {
                            if let Err(e) = tx_changes.send(change).await {
                                error!("could not send change for processing: {e}");
                                return;
                            }
                        }
                    }
                });
            }
        }
    });
}
