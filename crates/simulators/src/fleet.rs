//! Truck fleet: each truck drives plant → site → plant and reports over its own flaky modem.
//!
//! One trip runs through these phases:
//! 1. wait for a bay at the home plant (`available`)
//! 2. batch and load (`loading`, drum charging)
//! 3. paperwork (`loading`, drum mixing)
//! 4. drive out (`en_route`)
//! 5. wait for the pump (`on_site`)
//! 6. discharge at the site's pour rate (`discharging`)
//! 7. drive back (`returning`)
//! 8. rest (`available`)
//!
//! GPS is sent every 10 s while moving and every 60 s while parked. Drum state is sent on change
//! and every 30 s, and status on change. Travel time is haversine distance × [`ROAD_FACTOR`] ÷
//! [`AVG_SPEED_KMH`], ±10 %. A share of trips crosses a dead zone. Until dispatch exists (M5),
//! every trip goes to a random site.

use std::future::Future;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use domain::{Location, Truck, TruckStatus};
use rand::seq::IndexedRandom;
use rand::{RngExt, SeedableRng, rngs::StdRng};
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep};
use tracing::{debug, info};

use crate::Clock;
use crate::link::{Chaos, Modem, Outgoing};
use crate::plant::PlantSim;
use crate::world::World;

pub const ROAD_FACTOR: f64 = 1.3;
pub const AVG_SPEED_KMH: f64 = 40.0;
const TICK: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy)]
pub struct FleetConfig {
    pub chaos: Chaos,
    /// Share of trips (`0.0..=1.0`) that cross a 30–120 s dead zone.
    pub dead_zone_trip_share: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Rotation {
    Charging,
    Mixing,
    Discharging,
    Idle,
}

impl Rotation {
    fn rpm(self) -> f64 {
        match self {
            Rotation::Charging => 14.0,
            Rotation::Mixing => 3.0,
            Rotation::Discharging => 8.0,
            Rotation::Idle => 0.0,
        }
    }
}

#[derive(Debug, Clone)]
enum Motion {
    Parked(Location),
    Driving {
        from: Location,
        to: Location,
        duration: Duration,
        dead_zone: Option<Range<Duration>>,
    },
}

impl Motion {
    fn position(&self, elapsed: Duration) -> Location {
        match self {
            Motion::Parked(at) => *at,
            Motion::Driving {
                from, to, duration, ..
            } => {
                let f = (elapsed.as_secs_f64() / duration.as_secs_f64()).min(1.0);
                Location {
                    lat: from.lat + (to.lat - from.lat) * f,
                    lon: from.lon + (to.lon - from.lon) * f,
                }
            }
        }
    }

    fn speed_kmh(&self) -> f64 {
        match self {
            Motion::Parked(_) => 0.0,
            Motion::Driving {
                from, to, duration, ..
            } => from.distance_km(*to) * ROAD_FACTOR / (duration.as_secs_f64() / 3600.0),
        }
    }

    fn in_dead_zone(&self, elapsed: Duration) -> bool {
        matches!(self, Motion::Driving { dead_zone: Some(zone), .. } if zone.contains(&elapsed))
    }
}

/// The JSON a truck sends: the telematics vendor's field names, not ours.
#[derive(Serialize)]
struct Envelope<'a, T> {
    vehicle: &'a str,
    seq: u64,
    ts_ms: i64,
    #[serde(flatten)]
    body: T,
}

#[derive(Serialize)]
struct Gps {
    lat: f64,
    lon: f64,
    speed_kmh: f64,
}

#[derive(Serialize)]
struct Drum {
    rotation: Rotation,
    rpm: f64,
}

#[derive(Serialize)]
struct Status {
    status: TruckStatus,
}

pub struct TruckSim {
    truck: Truck,
    home: Arc<PlantSim>,
    world: Arc<World>,
    out: mpsc::Sender<Outgoing>,
    clock: Clock,
    modem: Modem,
    rng: StdRng,
    dead_zone_trip_share: f64,
    seq: u64,
    status: Option<TruckStatus>,
    rotation: Option<Rotation>,
}

impl TruckSim {
    pub fn new(
        truck: Truck,
        home: Arc<PlantSim>,
        world: Arc<World>,
        out: mpsc::Sender<Outgoing>,
        clock: Clock,
        config: FleetConfig,
        seed: u64,
    ) -> Self {
        TruckSim {
            truck,
            home,
            world,
            out,
            clock,
            modem: Modem::new(config.chaos, seed),
            rng: StdRng::seed_from_u64(seed),
            dead_zone_trip_share: config.dead_zone_trip_share,
            seq: 0,
            status: None,
            rotation: None,
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        // Stagger the fleet so 25 trucks don't all queue at once.
        let rest = Duration::from_secs(self.rng.random_range(0..120));
        let home = Motion::Parked(self.home.plant.location);
        self.phase(TruckStatus::Available, Rotation::Idle, home, sleep(rest))
            .await?;
        loop {
            self.trip().await?;
        }
    }

    async fn trip(&mut self) -> anyhow::Result<()> {
        let plant = Arc::clone(&self.home);
        let plant_at = plant.plant.location;
        let site = self
            .world
            .sites
            .choose(&mut self.rng)
            .context("the seed has no sites")?;
        let (site_id, site_at) = (site.site.id.0.clone(), site.site.location);
        let mix = self
            .world
            .mixes
            .choose(&mut self.rng)
            .context("the seed has no mixes")?
            .code
            .clone();
        let truck = self.truck.id.clone();
        let volume = self.truck.drum_capacity_m3;

        let parked = Motion::Parked(plant_at);
        let bay = self
            .phase(
                TruckStatus::Available,
                Rotation::Idle,
                parked.clone(),
                plant.acquire_bay(),
            )
            .await??;
        let load = plant.load(bay, &truck, &mix, volume);
        let ticket = self
            .phase(
                TruckStatus::Loading,
                Rotation::Charging,
                parked.clone(),
                load,
            )
            .await??;
        info!(truck = %truck.0, ticket, site = site_id, "loaded, heading to site");
        let paperwork = self.minutes(1..=3);
        self.phase(
            TruckStatus::Loading,
            Rotation::Mixing,
            parked,
            sleep(paperwork),
        )
        .await?;

        let out = self.drive(plant_at, site_at);
        self.phase(TruckStatus::EnRoute, Rotation::Mixing, out.0, sleep(out.1))
            .await?;
        let wait = self.minutes(2..=10);
        self.phase(
            TruckStatus::OnSite,
            Rotation::Mixing,
            Motion::Parked(site_at),
            sleep(wait),
        )
        .await?;
        let pour_rate_m3_per_h = f64::from(self.rng.random_range(4u32..=12) * 5);
        let discharge =
            Duration::try_from_secs_f64(volume / pour_rate_m3_per_h * 3600.0).unwrap_or_default();
        let on_site = Motion::Parked(site_at);
        self.phase(
            TruckStatus::Discharging,
            Rotation::Discharging,
            on_site,
            sleep(discharge),
        )
        .await?;

        let back = self.drive(site_at, plant_at);
        self.phase(
            TruckStatus::Returning,
            Rotation::Idle,
            back.0,
            sleep(back.1),
        )
        .await?;
        let rest = self.minutes(5..=20);
        self.phase(
            TruckStatus::Available,
            Rotation::Idle,
            Motion::Parked(plant_at),
            sleep(rest),
        )
        .await?;
        Ok(())
    }

    fn minutes(&mut self, range: std::ops::RangeInclusive<u64>) -> Duration {
        Duration::from_secs(60 * self.rng.random_range(range))
    }

    /// The motion for one drive and how long it takes, with an occasional dead zone.
    fn drive(&mut self, from: Location, to: Location) -> (Motion, Duration) {
        let hours =
            from.distance_km(to) * ROAD_FACTOR / AVG_SPEED_KMH * self.rng.random_range(0.9..1.1);
        let duration = Duration::try_from_secs_f64(hours * 3600.0)
            .unwrap_or_default()
            .max(Duration::from_secs(60));
        let dead_zone = (duration >= Duration::from_secs(120)
            && self.rng.random_bool(self.dead_zone_trip_share))
        .then(|| {
            let start = duration.mul_f64(self.rng.random_range(0.2..0.5));
            let length =
                Duration::from_secs(self.rng.random_range(30..=120)).min(duration.mul_f64(0.4));
            start..start + length
        });
        (
            Motion::Driving {
                from,
                to,
                duration,
                dead_zone,
            },
            duration,
        )
    }

    /// Reports status, drum and position until `until` completes, then returns its output.
    async fn phase<T>(
        &mut self,
        status: TruckStatus,
        rotation: Rotation,
        motion: Motion,
        until: impl Future<Output = T>,
    ) -> anyhow::Result<T> {
        self.flush_dead_zone().await?;
        if self.status != Some(status) {
            self.status = Some(status);
            info!(truck = %self.truck.id.0, ?status, "status");
            self.emit("status", Status { status }).await?;
        }
        if self.rotation != Some(rotation) {
            self.rotation = Some(rotation);
            self.emit(
                "drum",
                Drum {
                    rotation,
                    rpm: rotation.rpm(),
                },
            )
            .await?;
        }

        let moving = matches!(motion, Motion::Driving { .. });
        let started = Instant::now();
        tokio::pin!(until);
        let mut tick: u64 = 0;
        loop {
            let elapsed = started.elapsed();
            if motion.in_dead_zone(elapsed) {
                if !self.modem.in_dead_zone() {
                    info!(truck = %self.truck.id.0, "entered a dead zone, buffering telemetry");
                }
                self.modem.enter_dead_zone();
            } else {
                self.flush_dead_zone().await?;
            }
            if moving || tick.is_multiple_of(6) {
                let at = motion.position(elapsed);
                let gps = Gps {
                    lat: round(at.lat, 6),
                    lon: round(at.lon, 6),
                    speed_kmh: round(motion.speed_kmh(), 1),
                };
                self.emit("gps", gps).await?;
            }
            if tick % 3 == 2 {
                self.emit(
                    "drum",
                    Drum {
                        rotation,
                        rpm: rotation.rpm(),
                    },
                )
                .await?;
            }
            tick += 1;
            tokio::select! {
                out = &mut until => return Ok(out),
                () = sleep(TICK) => {}
            }
        }
    }

    async fn emit<T: Serialize>(&mut self, kind: &str, body: T) -> anyhow::Result<()> {
        self.seq += 1;
        let envelope = Envelope {
            vehicle: &self.truck.id.0,
            seq: self.seq,
            ts_ms: self.clock.now().timestamp_millis(),
            body,
        };
        let msg = Outgoing {
            topic: format!("trucks/{}/{kind}", self.truck.id.0),
            payload: serde_json::to_vec(&envelope)?,
        };
        debug!(topic = %msg.topic, payload = %String::from_utf8_lossy(&msg.payload), "telemetry");
        for msg in self.modem.send(msg) {
            self.out.send(msg).await?;
        }
        Ok(())
    }

    async fn flush_dead_zone(&mut self) -> anyhow::Result<()> {
        let buffered = self.modem.leave_dead_zone();
        if !buffered.is_empty() {
            info!(truck = %self.truck.id.0, messages = buffered.len(), "left the dead zone, flushing late telemetry");
        }
        for msg in buffered {
            self.out.send(msg).await?;
        }
        Ok(())
    }
}

fn round(x: f64, decimals: i32) -> f64 {
    let factor = 10f64.powi(decimals);
    (x * factor).round() / factor
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use serde_json::Value;

    struct Seen {
        topic: String,
        body: Value,
        /// Simulated wall-clock time when the message reached the broker side of the channel.
        received_ms: i64,
    }

    /// Runs one truck until it is available again after its first trip.
    async fn one_trip(dead_zone_trip_share: f64) -> (Vec<Seen>, Arc<World>) {
        let world = Arc::new(World::seed().unwrap());
        let (tx, mut rx) = mpsc::channel(100_000);
        let clock = Clock::start(Utc.with_ymd_and_hms(2026, 11, 9, 6, 0, 0).unwrap());
        let truck = world.trucks[0].clone();
        let home = world.plant(&truck.home_plant).unwrap().clone();
        let plant = Arc::new(PlantSim::new(home, tx.clone(), clock, Chaos::NONE, 1));
        let config = FleetConfig {
            chaos: Chaos::NONE,
            dead_zone_trip_share,
        };
        let sim = TruckSim::new(truck, plant, Arc::clone(&world), tx, clock, config, 7);
        let task = tokio::spawn(sim.run());

        let mut seen = Vec::new();
        let mut available = 0;
        while available < 2 {
            let msg = rx.recv().await.unwrap();
            if !msg.topic.starts_with("trucks/") {
                continue;
            }
            let body: Value = serde_json::from_slice(&msg.payload).unwrap();
            if body["status"] == "available" {
                available += 1;
            }
            seen.push(Seen {
                topic: msg.topic,
                body,
                received_ms: clock.now().timestamp_millis(),
            });
        }
        task.abort();
        (seen, world)
    }

    fn statuses(seen: &[Seen]) -> Vec<&str> {
        seen.iter()
            .filter_map(|s| s.body["status"].as_str())
            .collect()
    }

    fn ts_of_status(seen: &[Seen], status: &str) -> i64 {
        seen.iter()
            .find(|s| s.body["status"] == status)
            .unwrap()
            .body["ts_ms"]
            .as_i64()
            .unwrap()
    }

    const LIFECYCLE: [&str; 7] = [
        "available",
        "loading",
        "en_route",
        "on_site",
        "discharging",
        "returning",
        "available",
    ];

    #[tokio::test(start_paused = true)]
    async fn a_trip_follows_the_lifecycle_and_the_travel_model() {
        let (seen, world) = one_trip(0.0).await;
        assert_eq!(statuses(&seen), LIFECYCLE);

        let seqs: Vec<u64> = seen
            .iter()
            .map(|s| s.body["seq"].as_u64().unwrap())
            .collect();
        assert_eq!(
            seqs,
            (1..=seqs.len() as u64).collect::<Vec<_>>(),
            "seq counts every message"
        );
        assert!(seen.iter().all(|s| s.body["vehicle"] == "truck-01"));

        // The first GPS fix on site is the site; the drive took haversine × road factor ÷ speed, ±10 %.
        let on_site = seen
            .iter()
            .position(|s| s.body["status"] == "on_site")
            .unwrap();
        let fix = seen[on_site..]
            .iter()
            .find(|s| s.topic.ends_with("/gps"))
            .unwrap();
        let site = Location {
            lat: fix.body["lat"].as_f64().unwrap(),
            lon: fix.body["lon"].as_f64().unwrap(),
        };
        let plant = world
            .plants
            .iter()
            .find(|p| p.id.0 == "plant-mannheim")
            .unwrap()
            .location;
        let nominal_ms = plant.distance_km(site) * ROAD_FACTOR / AVG_SPEED_KMH * 3_600_000.0;
        let driven_ms = (ts_of_status(&seen, "on_site") - ts_of_status(&seen, "en_route")) as f64;
        let ratio = driven_ms / nominal_ms.max(60_000.0);
        assert!(
            (0.89..=1.11).contains(&ratio),
            "drove {driven_ms} ms, model says {nominal_ms} ms"
        );

        // GPS every 10 s while driving.
        let en_route = seen
            .iter()
            .position(|s| s.body["status"] == "en_route")
            .unwrap();
        let fixes: Vec<i64> = seen[en_route..on_site]
            .iter()
            .filter(|s| s.topic.ends_with("/gps"))
            .map(|s| s.body["ts_ms"].as_i64().unwrap())
            .collect();
        assert!(fixes.len() > 2);
        assert!(fixes.windows(2).all(|w| w[1] - w[0] == 10_000), "{fixes:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_dead_zone_delays_messages_but_loses_none() {
        let (seen, _) = one_trip(1.0).await;
        assert_eq!(statuses(&seen), LIFECYCLE);

        let seqs: Vec<u64> = seen
            .iter()
            .map(|s| s.body["seq"].as_u64().unwrap())
            .collect();
        assert_eq!(
            seqs,
            (1..=seqs.len() as u64).collect::<Vec<_>>(),
            "no loss, no reordering"
        );

        let late = seen
            .iter()
            .filter(|s| s.received_ms - s.body["ts_ms"].as_i64().unwrap() >= 20_000);
        assert!(late.count() >= 2, "a dead zone should deliver a late burst");
    }
}
