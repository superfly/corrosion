use crate::api::peer::{encode_write_sync_msg, serve_sync};
use bytes::BytesMut;
use corro_types::{
    agent::{Agent, Bookie},
    broadcast::{BiPayload, BiPayloadV1},
    sync::{SyncMessage, SyncMessageV1, SyncRejectionV1},
};
use metrics::counter;
use speedy::Readable;
use std::time::Duration;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};
use tracing::{debug, error, info_span, trace, warn, Instrument};
use tripwire::Tripwire;

/// Spawn a task that listens for incoming bi-directional sync streams
/// on a given connection.
///
/// For every incoming stream, spawn another task to handle the
/// stream.  Valid incoming BiPayload messages are passed to
/// `crate::api::peer::serve_sync()`
pub fn spawn_bipayload_handler(
    agent: &Agent,
    bookie: &Bookie,
    tripwire: &Tripwire,
    conn: &quinn::Connection,
) {
    let conn = conn.clone();
    let agent = agent.clone();
    let bookie = bookie.clone();
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

            counter!("corro.peer.stream.accept.total", "type" => "bi").increment(1);

            trace!(
                "accepted a bidirectional stream from {}",
                conn.remote_address()
            );

            // Bound the number of in-flight bi-handler tasks. All bi-streams
            // are sync requests today, so we reuse `Limits.sync` rather than
            // adding a parallel counter. Acquiring before `tokio::spawn`
            // prevents an unbounded peer from exhausting tasks/memory just
            // by opening streams.
            let permit = match agent.limits().sync.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    counter!("corro.peer.stream.accept.rejected.total", "type" => "bi")
                        .increment(1);
                    warn!(
                        "rejecting bi-stream from {}: sync concurrency limit reached",
                        conn.remote_address()
                    );

                    // Best-effort: tell the peer why we rejected, then drop
                    // both stream halves. A slow peer must not wedge the
                    // accept loop, hence the timeout.
                    let mut tx = tx;
                    let _ = timeout(Duration::from_secs(5), async {
                        let mut codec = LengthDelimitedCodec::builder()
                            .max_frame_length(100 * 1_024 * 1_024)
                            .new_codec();
                        let mut send_buf = BytesMut::new();
                        let mut encode_buf = BytesMut::new();
                        if let Err(e) = encode_write_sync_msg(
                            &mut codec,
                            &mut encode_buf,
                            &mut send_buf,
                            SyncMessage::V1(SyncMessageV1::Rejection(
                                SyncRejectionV1::MaxConcurrencyReached,
                            )),
                            &mut tx,
                        )
                        .instrument(info_span!("write_bi_rejection"))
                        .await
                        {
                            debug!("could not write bi-stream rejection: {e}");
                        }
                        let _ = tx.finish();
                    })
                    .await;

                    // `rx` is dropped at scope end, closing the receive side.
                    continue;
                }
            };

            tokio::spawn({
                let agent = agent.clone();
                let bookie = bookie.clone();
                async move {
                    // Hold the permit for the task's lifetime; drops on exit
                    // (including panic), releasing capacity for the next peer.
                    let _permit = permit;
                    let mut framed = FramedRead::new(
                        rx,
                        LengthDelimitedCodec::builder()
                            .max_frame_length(100 * 1_024 * 1_024)
                            .new_codec(),
                    );

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
                                                BiPayload::V1 { data, cluster_id } => match data {
                                                    BiPayloadV1::SyncStart {
                                                        actor_id,
                                                        trace_ctx,
                                                    } => {
                                                        trace!(
                                                            "framed read buffer len: {}",
                                                            framed.read_buffer().len()
                                                        );

                                                        // println!("got sync state: {state:?}");
                                                        if let Err(e) = serve_sync(
                                                            &agent, &bookie, actor_id, trace_ctx,
                                                            cluster_id, framed, tx,
                                                        )
                                                        .await
                                                        {
                                                            warn!("could not complete receiving sync: {e}");
                                                        }
                                                        break;
                                                    }
                                                },
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
