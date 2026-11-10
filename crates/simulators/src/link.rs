//! The patchy cellular link between a truck (or plant) and Mosquitto.
//!
//! SPEC §5 says telematics arrive duplicated and out of order. Each sender owns a [`Modem`] that
//! does this on purpose. Some messages are sent twice, some are held back behind their successor,
//! and in a dead zone everything is buffered and then flushed in one late burst. Nothing is lost.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rand::{RngExt, SeedableRng, rngs::StdRng};
use rumqttc::{AsyncClient, Event, EventLoop, Packet, QoS};
use tokio::sync::{Notify, mpsc};
use tracing::{info, warn};

/// Breaks a simulated truck down: publish anything to `sim/trucks/{id}/breakdown`.
pub const BREAKDOWN_TOPIC: &str = "sim/trucks/+/breakdown";

/// One MQTT message on its way to the broker.
#[derive(Debug, Clone, PartialEq)]
pub struct Outgoing {
    pub topic: String,
    pub payload: Vec<u8>,
}

/// Probabilities in `0.0..=1.0`, validated when the config is read.
#[derive(Debug, Clone, Copy)]
pub struct Chaos {
    pub duplicate_rate: f64,
    pub reorder_rate: f64,
}

impl Chaos {
    #[cfg(test)]
    pub const NONE: Chaos = Chaos {
        duplicate_rate: 0.0,
        reorder_rate: 0.0,
    };
}

pub struct Modem {
    rng: StdRng,
    chaos: Chaos,
    held: Option<Outgoing>,
    dead_zone: Option<Vec<Outgoing>>,
}

impl Modem {
    pub fn new(chaos: Chaos, seed: u64) -> Self {
        Modem {
            rng: StdRng::seed_from_u64(seed),
            chaos,
            held: None,
            dead_zone: None,
        }
    }

    /// Returns what goes on the wire now, in wire order. It may be nothing, one message, or a few.
    pub fn send(&mut self, msg: Outgoing) -> Vec<Outgoing> {
        if let Some(buffer) = &mut self.dead_zone {
            buffer.push(msg);
            return Vec::new();
        }
        if self.held.is_none() && self.rng.random_bool(self.chaos.reorder_rate) {
            self.held = Some(msg);
            return Vec::new();
        }
        let mut wire = Vec::with_capacity(3);
        if self.rng.random_bool(self.chaos.duplicate_rate) {
            wire.push(msg.clone());
        }
        wire.push(msg);
        wire.extend(self.held.take());
        wire
    }

    pub fn in_dead_zone(&self) -> bool {
        self.dead_zone.is_some()
    }

    pub fn enter_dead_zone(&mut self) {
        self.dead_zone.get_or_insert_with(Vec::new);
    }

    /// Everything buffered in the dead zone, oldest first. Empty if there was no dead zone.
    pub fn leave_dead_zone(&mut self) -> Vec<Outgoing> {
        self.dead_zone.take().unwrap_or_default()
    }
}

/// Publishes everything the simulators send. The bounded channel gives backpressure when the broker is slow.
pub async fn publish(mut rx: mpsc::Receiver<Outgoing>, client: AsyncClient) -> anyhow::Result<()> {
    while let Some(msg) = rx.recv().await {
        client
            .publish(msg.topic, QoS::AtLeastOnce, false, msg.payload)
            .await?;
    }
    Ok(())
}

/// Drives the MQTT connection and hands breakdown commands to the trucks in `breakdowns`.
/// rumqttc reconnects on the next poll after an error.
pub async fn drive(
    mut eventloop: EventLoop,
    client: AsyncClient,
    breakdowns: HashMap<String, Arc<Notify>>,
) -> anyhow::Result<()> {
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                info!("connected to the MQTT broker");
                // A clean session forgets its subscriptions, so subscribe on every connect.
                if let Err(error) = client.try_subscribe(BREAKDOWN_TOPIC, QoS::AtLeastOnce) {
                    warn!(%error, "can't subscribe to breakdown commands");
                }
            }
            Ok(Event::Incoming(Packet::Publish(command))) => {
                match commanded_truck(&command.topic).and_then(|t| breakdowns.get(t)) {
                    Some(truck) => {
                        info!(topic = %command.topic, "breakdown ordered");
                        truck.notify_one();
                    }
                    None => warn!(topic = %command.topic, "breakdown for an unknown truck"),
                }
            }
            Ok(_) => {}
            Err(error) => {
                warn!(%error, "MQTT connection failed, retrying in 2 s");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

/// The truck a `sim/trucks/{id}/breakdown` command is for.
fn commanded_truck(topic: &str) -> Option<&str> {
    topic
        .strip_prefix("sim/trucks/")?
        .strip_suffix("/breakdown")
        .filter(|truck| !truck.is_empty() && !truck.contains('/'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn breakdown_commands_name_their_truck() {
        assert_eq!(
            commanded_truck("sim/trucks/truck-07/breakdown"),
            Some("truck-07")
        );
        assert_eq!(commanded_truck("sim/trucks//breakdown"), None);
        assert_eq!(commanded_truck("sim/trucks/a/b/breakdown"), None);
        assert_eq!(commanded_truck("trucks/truck-07/status"), None);
    }

    fn msg(i: u32) -> Outgoing {
        Outgoing {
            topic: "trucks/t/gps".into(),
            payload: i.to_string().into_bytes(),
        }
    }

    fn id(m: &Outgoing) -> u32 {
        String::from_utf8_lossy(&m.payload).parse().unwrap()
    }

    #[test]
    fn nothing_is_lost_and_chaos_matches_its_settings() {
        const N: u32 = 10_000;
        let mut modem = Modem::new(
            Chaos {
                duplicate_rate: 0.03,
                reorder_rate: 0.03,
            },
            42,
        );
        let mut wire: Vec<u32> = Vec::new();
        for i in 0..=N {
            // `N` is a sentinel that releases a message held back at the very end.
            wire.extend(modem.send(msg(i)).iter().map(id));
        }

        let mut first_seen: Vec<u32> = Vec::new();
        for &i in &wire {
            if !first_seen.contains(&i) {
                first_seen.push(i);
            }
        }
        for i in 0..N {
            assert!(first_seen.contains(&i), "message {i} was lost");
        }

        let duplicates = wire.len() - first_seen.len();
        assert!((200..=400).contains(&duplicates), "{duplicates} duplicates");

        // A held-back message is overtaken by exactly one neighbour, never more.
        for (pos, &i) in first_seen.iter().enumerate() {
            assert!(
                pos.abs_diff(i as usize) <= 1,
                "message {i} arrived at {pos}"
            );
        }
        let swaps = first_seen.windows(2).filter(|w| w[0] > w[1]).count();
        assert!((200..=400).contains(&swaps), "{swaps} swaps");
    }

    #[test]
    fn a_dead_zone_buffers_and_then_flushes_in_order() {
        let mut modem = Modem::new(Chaos::NONE, 1);
        assert_eq!(modem.send(msg(1)), [msg(1)]);
        modem.enter_dead_zone();
        assert!(modem.send(msg(2)).is_empty());
        assert!(modem.send(msg(3)).is_empty());
        assert_eq!(modem.leave_dead_zone(), [msg(2), msg(3)]);
        assert!(modem.leave_dead_zone().is_empty());
        assert_eq!(modem.send(msg(4)), [msg(4)]);
    }
}
