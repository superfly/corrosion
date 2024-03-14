use crate::agent::util::process_multiple_changes;
use corro_types::{
    agent::{Agent, Bookie},
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
pub fn spawn_unipayload_handler(
    agent: Agent,
    bookie: Bookie,
    tripwire: &Tripwire,
    conn: &quinn::Connection,
) {
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

                debug!(
                    "accepted a unidirectional stream from {}",
                    conn.remote_address()
                );

                tokio::spawn({
                    let agent = agent.clone();
                    let bookie = bookie.clone();
                    async move {
                        let mut framed = FramedRead::new(rx, LengthDelimitedCodec::new());

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
                                                    priority,
                                                } => {
                                                    if cluster_id != agent.cluster_id() {
                                                        continue;
                                                    }

                                                    if priority {
                                                        let agent = agent.clone();
                                                        let bookie = bookie.clone();

                                                        tokio::spawn(async move {
                                                            if let Err(e) =
                                                                process_multiple_changes(
                                                                    agent,
                                                                    bookie,
                                                                    vec![(
                                                                        change,
                                                                        ChangeSource::Broadcast,
                                                                        std::time::Instant::now(),
                                                                    )],
                                                                )
                                                                .await
                                                            {
                                                                error!("Process priority change failed: {:?}", e);
                                                            }
                                                        });
                                                    } else {
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
