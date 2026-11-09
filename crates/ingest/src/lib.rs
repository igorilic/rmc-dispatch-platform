//! Ingest: customer orders, telematics and batch tickets in; canonical events out (SPEC §5).

pub mod mapping;
pub mod nats;
pub mod sources;
pub mod telematics;
mod validate;

pub use validate::report;

use domain::{Channel, EventEnvelope, Seed};

use crate::mapping::{Delivery, Mapping};
use crate::nats::{Bus, BusError, DeadLetter, StoredLetter};

/// What a replay did.
#[derive(Debug, Default)]
pub struct ReplayReport {
    pub replayed: usize,
    pub still_failing: Vec<StillFailing>,
}

#[derive(Debug)]
pub struct StillFailing {
    pub sequence: u64,
    pub integration: String,
    pub record: String,
    pub errors: Vec<String>,
}

/// Re-maps dead letters with the current mappings, optionally only one integration's. A letter
/// whose every record now maps is published and removed. One that still fails stays, with its new
/// errors. Replay never dead-letters again, so running it twice doesn't duplicate letters.
pub async fn replay(
    bus: &Bus,
    mappings: &[Mapping],
    seed: &Seed,
    only: Option<&str>,
) -> Result<ReplayReport, BusError> {
    let mut report = ReplayReport::default();
    for StoredLetter { sequence, letter } in bus.dead_letters().await? {
        if only.is_some_and(|integration| integration != letter.integration) {
            continue;
        }
        match remap(&letter, mappings, seed) {
            Ok(events) => {
                for event in &events {
                    bus.publish(event).await?;
                }
                bus.remove_dead_letter(sequence).await?;
                report.replayed += 1;
            }
            Err(errors) => report.still_failing.push(StillFailing {
                sequence,
                integration: letter.integration,
                record: letter.record,
                errors,
            }),
        }
    }
    Ok(report)
}

fn remap(
    letter: &DeadLetter,
    mappings: &[Mapping],
    seed: &Seed,
) -> Result<Vec<EventEnvelope>, Vec<String>> {
    if letter.channel == Channel::Mqtt {
        return telematics::map(
            &letter.reference,
            letter.raw.as_bytes(),
            letter.received_at,
            seed,
        )
        .map(|e| vec![e]);
    }
    let mapping = mappings
        .iter()
        .find(|m| m.integration == letter.integration)
        .ok_or_else(|| vec![format!("no mapping for {}", letter.integration)])?;
    let delivery = Delivery {
        body: letter.raw.as_bytes(),
        channel: letter.channel,
        reference: &letter.reference,
        received_at: letter.received_at,
    };
    let (mut events, mut errors) = (Vec::new(), Vec::new());
    for outcome in mapping.map(&delivery, seed) {
        match outcome.result {
            Ok(event) => events.push(event),
            Err(failures) => errors.extend(failures.iter().map(ToString::to_string)),
        }
    }
    if errors.is_empty() {
        Ok(events)
    } else {
        Err(errors)
    }
}
