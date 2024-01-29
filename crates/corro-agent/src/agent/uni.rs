use corro_types::{
    agent::{Agent, Bookie},
    broadcast::{BroadcastV1, UniPayload, UniPayloadV1},
    channel::{CorroReceiver, CorroSender},
};
use metrics::{counter, histogram};
use speedy::Readable;
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};
use tracing::{debug, error, info, trace};
use tripwire::Tripwire;

/// Spawn a task that accepts unidirectional broadcast streams, then
/// spawns another task for each incoming stream to handle.
pub fn spawn_unipayload_handler(
    tripwire: &Tripwire,
    conn: &quinn::Connection,
    process_uni_tx: CorroSender<UniPayload>,
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
                    let process_uni_tx = process_uni_tx.clone();
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

                                            if let Err(e) = process_uni_tx.send(payload).await {
                                                error!(
                                                    "could not send UniPayload for processing: {e}"
                                                );
                                                // this means we won't be able to process more...
                                                return;
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

/// Start an async buffer task that pull messages from one channel
/// (process_uni_rx) and moves them into the next if the variant
/// matches (bcast_msg_tx)
///
/// This task may not be needed anymore, but decouples broadcast
/// receivers and handlers, so we may still want to keep it.
pub fn spawn_unipayload_message_decoder(
    agent: &Agent,
    bookie: &Bookie,
    mut process_uni_rx: CorroReceiver<UniPayload>,
    bcast_msg_tx: CorroSender<BroadcastV1>,
) {
    tokio::spawn({
        let agent = agent.clone();
        let bookie = bookie.clone();
        async move {
            while let Some(payload) = process_uni_rx.recv().await {
                match payload {
                    UniPayload::V1(UniPayloadV1::Broadcast(bcast)) => {
                        handle_change(&agent, &bookie, bcast, &bcast_msg_tx).await
                    }
                }
            }

            info!("uni payload process loop is done!");
        }
    });
}

/// Apply a single broadcast to the local actor state, then
/// re-broadcast to other cluster members via the bcast_msg_tx channel
async fn handle_change(
    agent: &Agent,
    bookie: &Bookie,
    bcast: BroadcastV1,
    bcast_msg_tx: &CorroSender<BroadcastV1>,
) {
    match bcast {
        BroadcastV1::Change(change) => {
            let diff = if let Some(ts) = change.ts() {
                if let Ok(id) = change.actor_id.try_into() {
                    Some(
                        agent
                            .clock()
                            .new_timestamp()
                            .get_diff_duration(&uhlc::Timestamp::new(ts.0, id)),
                    )
                } else {
                    None
                }
            } else {
                None
            };

            counter!("corro.broadcast.recv.count", "kind" => "change").increment(1);

            trace!("handling {} changes", change.len());

            let booked = {
                bookie
                    .write(format!(
                        "handle_change(for_actor):{}",
                        change.actor_id.as_simple()
                    ))
                    .await
                    .for_actor(change.actor_id)
            };

            if booked
                .read(format!(
                    "handle_change(contains?):{}",
                    change.actor_id.as_simple()
                ))
                .await
                .contains_all(change.versions(), change.seqs())
            {
                trace!("already seen, stop disseminating");
                return;
            }

            // If the change originated from us we're done
            if change.actor_id == agent.actor_id() {
                return;
            }

            if let Some(diff) = diff {
                histogram!("corro.broadcast.recv.lag.seconds").record(diff.as_secs_f64());
            }

            // Otherwise pass it to handle_broadcasts
            if let Err(e) = bcast_msg_tx.send(BroadcastV1::Change(change)).await {
                error!("could not send change message through broadcast channel: {e}");
            }
        }
    }
}
