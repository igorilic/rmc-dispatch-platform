//! Fake batching plants: the plant's batch computer, issuing batch tickets.
//!
//! A plant never batches more loads at once than it has loading bays (a SPEC §4 hard constraint).
//! The bays are a semaphore that trucks queue on. Each load produces three ticket events on
//! `plants/{id}/tickets`: `batching`, `loading` and `loaded`. Batching takes volume ÷ batching
//! rate, and loading the truck takes a fixed 3 minutes.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use domain::{DeliveryStatus, MixCode, Plant, TruckId};
use serde::Serialize;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc};
use tracing::info;

use crate::Clock;
use crate::link::{Chaos, Modem, Outgoing};

const LOADING: Duration = Duration::from_secs(3 * 60);

pub struct PlantSim {
    pub plant: Plant,
    bays: Arc<Semaphore>,
    next_ticket: AtomicU32,
    modem: Mutex<Modem>,
    out: mpsc::Sender<Outgoing>,
    clock: Clock,
}

#[derive(Serialize)]
struct Ticket<'a> {
    plant: &'a str,
    ticket_no: &'a str,
    vehicle: &'a str,
    ts_ms: i64,
    event: DeliveryStatus,
    mix: &'a str,
    volume_m3: f64,
}

impl PlantSim {
    pub fn new(
        plant: Plant,
        out: mpsc::Sender<Outgoing>,
        clock: Clock,
        chaos: Chaos,
        seed: u64,
    ) -> Self {
        PlantSim {
            bays: Arc::new(Semaphore::new(plant.loading_bays as usize)),
            next_ticket: AtomicU32::new(1),
            modem: Mutex::new(Modem::new(chaos, seed)),
            plant,
            out,
            clock,
        }
    }

    /// Waits for a free loading bay. The bay is freed when the permit is dropped.
    pub async fn acquire_bay(&self) -> anyhow::Result<OwnedSemaphorePermit> {
        Ok(Arc::clone(&self.bays).acquire_owned().await?)
    }

    /// Batches and loads one truck in `bay`, then returns the ticket number.
    pub async fn load(
        &self,
        _bay: OwnedSemaphorePermit,
        truck: &TruckId,
        mix: &MixCode,
        volume_m3: f64,
    ) -> anyhow::Result<String> {
        let n = self.next_ticket.fetch_add(1, Ordering::Relaxed);
        let prefix: String = self
            .plant
            .id
            .0
            .trim_start_matches("plant-")
            .chars()
            .take(3)
            .collect();
        let ticket_no = format!("{}-{n:06}", prefix.to_uppercase());
        let batching_hours = volume_m3 / self.plant.batching_rate_m3_per_h;

        self.emit(&ticket_no, truck, DeliveryStatus::Batching, mix, volume_m3)
            .await?;
        tokio::time::sleep(
            Duration::try_from_secs_f64(batching_hours * 3600.0).unwrap_or_default(),
        )
        .await;
        self.emit(&ticket_no, truck, DeliveryStatus::Loading, mix, volume_m3)
            .await?;
        tokio::time::sleep(LOADING).await;
        self.emit(&ticket_no, truck, DeliveryStatus::Loaded, mix, volume_m3)
            .await?;
        Ok(ticket_no)
    }

    async fn emit(
        &self,
        ticket_no: &str,
        truck: &TruckId,
        event: DeliveryStatus,
        mix: &MixCode,
        volume_m3: f64,
    ) -> anyhow::Result<()> {
        info!(plant = %self.plant.id.0, ticket_no, truck = %truck.0, ?event, "batch ticket");
        let ticket = Ticket {
            plant: &self.plant.id.0,
            ticket_no,
            vehicle: &truck.0,
            ts_ms: self.clock.now().timestamp_millis(),
            event,
            mix: &mix.0,
            volume_m3,
        };
        let msg = Outgoing {
            topic: format!("plants/{}/tickets", self.plant.id.0),
            payload: serde_json::to_vec(&ticket)?,
        };
        let wire = self.modem.lock().await.send(msg);
        for msg in wire {
            self.out.send(msg).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use domain::{Location, OperatingHours, PlantId, PlantStatus};

    fn plant(bays: u32) -> Plant {
        Plant {
            id: PlantId("plant-walldorf".into()),
            location: Location {
                lat: 49.3064,
                lon: 8.6428,
            },
            loading_bays: bays,
            batching_rate_m3_per_h: 120.0,
            operating_hours: OperatingHours {
                open: "05:00:00".parse().unwrap(),
                close: "20:00:00".parse().unwrap(),
            },
            status: PlantStatus::Online,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn never_batches_more_loads_than_it_has_bays() {
        let (tx, mut rx) = mpsc::channel(100);
        let clock = Clock::start(Utc.with_ymd_and_hms(2026, 11, 9, 6, 0, 0).unwrap());
        let sim = Arc::new(PlantSim::new(plant(2), tx, clock, Chaos::NONE, 1));

        let mut trucks = Vec::new();
        for i in 0..5 {
            let sim = Arc::clone(&sim);
            trucks.push(tokio::spawn(async move {
                let bay = sim.acquire_bay().await.unwrap();
                let truck = TruckId(format!("truck-{i}"));
                sim.load(bay, &truck, &MixCode("C30/37-XC4".into()), 9.0)
                    .await
                    .unwrap()
            }));
        }
        let mut tickets = Vec::new();
        for truck in trucks {
            tickets.push(truck.await.unwrap());
        }

        let (mut active, mut max_active, mut events) = (0i32, 0i32, 0);
        while let Ok(msg) = rx.try_recv() {
            assert_eq!(msg.topic, "plants/plant-walldorf/tickets");
            let ticket: serde_json::Value = serde_json::from_slice(&msg.payload).unwrap();
            match ticket["event"].as_str().unwrap() {
                "batching" => active += 1,
                "loaded" => active -= 1,
                _ => {}
            }
            max_active = max_active.max(active);
            events += 1;
        }
        assert_eq!(
            events, 15,
            "batching, loading and loaded for each of 5 loads"
        );
        assert_eq!(max_active, 2, "both bays used, never a third");

        tickets.sort();
        let expected: Vec<String> = (1..=5).map(|n| format!("WAL-{n:06}")).collect();
        assert_eq!(tickets, expected, "unique and sequential per plant");
    }
}
