use crate::api::peer::serve_sync;
use corro_types::{
    agent::Agent,
    broadcast::{BiPayload, BiPayloadV1},
};
use metrics::increment_counter;
use speedy::Readable;
use std::time::Duration;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};
use tracing::{debug, error, trace, warn};
use tripwire::Tripwire;

/// Spawn a task that listens for incoming bi-directional sync streams
/// on a given connection.
///
/// For every incoming stream, spawn another task to handle the
/// stream.  Valid incoming BiPayload messages are passed to
/// `crate::api::peer::serve_sync()`
pub fn spawn_bipayload_handler(agent: &Agent, tripwire: &Tripwire, conn: &quinn::Connection) {
    let conn = conn.clone();
    let agent = agent.clone();
    let mut tripwire = tripwire.clone();
    tokio::spawn(async move {
        loop {
            let (tx, rx) = tokio::select! {
                tx_rx_res = conn.accept_bi() => match tx_rx_res {
                    Ok(tx_rx) => tx_rx,
                    Err(e) => {
                        debug!("could not accept bidirectional stream from connection: {e}");
                        return;
                    }
                },
                _ = &mut tripwire => {
                    debug!("connection cancelled");
                    return;
                }
            };

            increment_counter!("corro.peer.stream.accept.total", "type" => "bi");

            debug!(
                "accepted a bidirectional stream from {}",
                conn.remote_address()
            );

            // TODO: implement concurrency limit for sync requests
            tokio::spawn({
                let agent = agent.clone();
                async move {
                    let mut framed = FramedRead::new(rx, LengthDelimitedCodec::new());

                    loop {
                        match timeout(Duration::from_secs(5), StreamExt::next(&mut framed)).await {
                            Err(_e) => {
                                warn!("timed out receiving bidirectional frame");
                                return;
                            }
                            Ok(None) => {
                                return;
                            }
                            Ok(Some(res)) => match res {
                                Ok(b) => {
                                    match BiPayload::read_from_buffer(&b) {
                                        Ok(payload) => {
                                            match payload {
                                                BiPayload::V1(BiPayloadV1::SyncStart {
                                                    actor_id,
                                                    trace_ctx,
                                                }) => {
                                                    trace!(
                                                        "framed read buffer len: {}",
                                                        framed.read_buffer().len()
                                                    );
                                                    // println!("got sync state: {state:?}");
                                                    if let Err(e) = serve_sync(
                                                        &agent, actor_id, trace_ctx, framed, tx,
                                                    )
                                                    .await
                                                    {
                                                        warn!("could not complete receiving sync: {e}");
                                                    }
                                                    break;
                                                }
                                            }
                                        }

                                        Err(e) => {
                                            warn!("could not decode BiPayload: {e}");
                                        }
                                    }
                                }

                                Err(e) => {
                                    error!("could not read framed payload from bidirectional stream: {e}");
                                }
                            },
                        }
                    }
                }
            });
        }
    });
}
