use std::{io, sync::Arc, time::Duration};

use parking_lot::RwLock;
use tokio::time::Instant;

use crate::{
    server::state::GlobalState,
    types::{
        client_id::ClientId,
        outgoing::{KickReason, Outgoing},
    },
};

pub(crate) fn keep_alive_timer(
    keep_alive: u16,
    client_id: ClientId,
    last_packet_at: &Arc<RwLock<Instant>>,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    if keep_alive > 0 {
        let half_interval = Duration::from_millis(keep_alive as u64 * 500);
        log::debug!("{} keep alive: {:?}", client_id, half_interval * 2);
        let last_packet_at = Arc::clone(last_packet_at);
        let global = Arc::clone(global);
        let action_gen = move || {
            let last_packet_at = Arc::clone(&last_packet_at);
            let global = Arc::clone(&global);
            async move {
                {
                    let last_packet_at = last_packet_at.read();
                    if last_packet_at.elapsed() <= half_interval * 3 {
                        return Some(half_interval);
                    }
                }
                // timeout, kick it out
                if let Some(sender) = global.get_outgoing_sender(&client_id) {
                    if let Err(err) = sender.send(Outgoing::Kick(KickReason::Expired)).await {
                        log::warn!(
                            "send expired session message to {:?} error: {:?}",
                            client_id,
                            err
                        );
                    }
                }
                None
            }
        };

        tokio::spawn(async move {
            while let Some(duration) = action_gen().await {
                tokio::time::sleep(duration).await;
            }
        });
    }
    Ok(())
}
