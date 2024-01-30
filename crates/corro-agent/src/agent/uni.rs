use corro_types::{
    broadcast::{BroadcastV1, ChangeSource, ChangeV1, UniPayload, UniPayloadV1},
    channel::CorroSender,
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
    tripwire: &Tripwire,
    conn: &quinn::Connection,
    changes_tx: CorroSender<(ChangeV1, ChangeSource)>,
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
                    let changes_tx = changes_tx.clone();
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
                                                UniPayload::V1(UniPayloadV1::Broadcast(
                                                    BroadcastV1::Change(change),
                                                )) => {
                                                    if let Err(e) = changes_tx
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
