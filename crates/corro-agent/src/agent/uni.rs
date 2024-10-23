use corro_types::{
    agent::Agent,
    broadcast::{BroadcastV1, ChangeSource, UniPayload, UniPayloadV1},
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
                    let agent = agent.clone();
                    async move {
                        let mut framed = FramedRead::new(
                            rx,
                            LengthDelimitedCodec::builder()
                                .max_frame_length(100 * 1_024 * 1_024)
                                .new_codec(),
                        );

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
                                                    cluster_id,
                                                } => {
                                                    if cluster_id != agent.cluster_id() {
                                                        continue;
                                                    }
                                                    if let Err(e) = agent
                                                        .tx_changes()
                                                        .send((change, ChangeSource::Broadcast))
                                                        .await
                                                    {
                                                        error!(
                                                            "could not send change for processing: {e}"
                                                        );
                                                        return;
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
                    }
                });
            }
        }
    });
}
