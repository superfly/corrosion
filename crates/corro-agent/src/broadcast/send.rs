use crate::{broadcast::transmit_broadcast, transport::Transport};
use bytes::{BufMut, BytesMut};
use corro_types::{
    agent::Agent,
    broadcast::{BroadcastInput, UniPayload, UniPayloadV1},
};
use speedy::Writable;
use tokio_util::codec::{Encoder, LengthDelimitedCodec};
use tracing::{error, info, trace};

use super::PendingBroadcast;

#[inline]
pub fn dispatch_broadcast(
    agent: &Agent,
    transport: &Transport,
    input: BroadcastInput,
    bcast_codec: &mut LengthDelimitedCodec,
    ser_buf: &mut BytesMut,
    single_bcast_buf: &mut BytesMut,
    local_bcast_buf: &mut BytesMut,
    bcast_buf: &mut BytesMut,
    to_broadcast: &mut Vec<PendingBroadcast>,
) -> Option<()> {
    const BROADCAST_CUTOFF: usize = 64 * 1024;

    let (bcast, is_local) = match input {
        BroadcastInput::Rebroadcast(bcast) => (bcast, false),
        BroadcastInput::AddBroadcast(bcast) => (bcast, true),
    };
    trace!("adding broadcast: {bcast:?}, local? {is_local}");

    // Locally originating broadcasts are given higher priority
    if is_local {
        // todo: change this to encode a V1 payload before merging
        info!("Generating a high priority broadcast!");
        if let Err(e) = (UniPayload::V1 {
            data: UniPayloadV1::Broadcast(bcast),
            cluster_id: agent.cluster_id(),
            priority: true,
        })
        .write_to_stream(ser_buf.writer())
        {
            error!("could not encode UniPayload::V1 Broadcast: {e}");
            ser_buf.clear();
            return None;
        }

        trace!("ser buf len: {}", ser_buf.len());

        if let Err(e) = bcast_codec.encode(ser_buf.split().freeze(), single_bcast_buf) {
            error!("could not encode local broadcast: {e}");
            single_bcast_buf.clear();
            return None;
        }

        let payload = single_bcast_buf.split().freeze();
        local_bcast_buf.extend_from_slice(&payload);

        {
            let members = agent.members().read();
            for addr in members.ring0(agent.cluster_id()) {
                // this spawns, so we won't be holding onto the read lock for long
                tokio::spawn(transmit_broadcast(payload.clone(), transport.clone(), addr));
            }
        }

        if local_bcast_buf.len() >= BROADCAST_CUTOFF {
            to_broadcast.push(PendingBroadcast::new_local(
                local_bcast_buf.split().freeze(),
            ));
        }
    }
    // Re-broadcasts are given default priority
    else {
        info!("Generating a regular broadcast!");
        if let Err(e) = (UniPayload::V1 {
            data: UniPayloadV1::Broadcast(bcast),
            cluster_id: agent.cluster_id(),
            priority: false,
        })
        .write_to_stream(ser_buf.writer())
        {
            error!("could not encode UniPayload::V1 Broadcast: {e}");
            ser_buf.clear();
            return None;
        }
        trace!("ser buf len: {}", ser_buf.len());

        if let Err(e) = bcast_codec.encode(ser_buf.split().freeze(), bcast_buf) {
            error!("could not encode broadcast: {e}");
            bcast_buf.clear();
            return None;
        }

        if bcast_buf.len() >= BROADCAST_CUTOFF {
            to_broadcast.push(PendingBroadcast::new(bcast_buf.split().freeze()));
        }
    }

    Some(())
}
