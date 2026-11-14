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
//! and every 30 s, and status on change. A truck drives out and back along the road from its plant
//! to the site ([`crate::roads`]), or the straight line without one, at a constant speed. A drive
//! takes the haversine distance between its ends × [`ROAD_FACTOR`] ÷ [`AVG_SPEED_KMH`], ±10 %,
//! whatever the road's length, so the speed it reports is the road's km over that time. A share
//! of trips crosses a dead zone. Every trip goes to a random site: the trucks don't follow
//! dispatch's plan yet (M7).
//!
//! A truck breaks down on command ([`TruckSim::breakdown`]), wherever it is: it reports
//! `out_of_service`, stays put for [`FleetConfig::repair`], then drives back to its home plant
//! along the road it was on.

use std::future::Future;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use domain::{DrumRotation, Location, Truck, TruckStatus};
use rand::seq::IndexedRandom;
use rand::{RngExt, SeedableRng, rngs::StdRng};
use serde::Serialize;
use tokio::sync::{Notify, mpsc};
use tokio::time::{Instant, sleep};
use tracing::{debug, info};

use crate::Clock;
use crate::link::{Chaos, Modem, Outgoing};
use crate::plant::PlantSim;
use crate::roads::{Road, Roads};
use domain::Seed;

pub const ROAD_FACTOR: f64 = 1.3;
pub const AVG_SPEED_KMH: f64 = 40.0;
const TICK: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy)]
pub struct FleetConfig {
    pub chaos: Chaos,
    /// Share of trips (`0.0..=1.0`) that cross a 30–120 s dead zone.
    pub dead_zone_trip_share: f64,
    /// How long a broken-down truck stays out of service.
    pub repair: Duration,
}

fn rpm(rotation: DrumRotation) -> f64 {
    match rotation {
        DrumRotation::Charging => 14.0,
        DrumRotation::Mixing => 3.0,
        DrumRotation::Discharging => 8.0,
        DrumRotation::Idle => 0.0,
    }
}

/// One drive along a road, from `from_km` to `to_km` at a constant speed.
#[derive(Debug, Clone)]
struct Drive {
    road: Arc<Road>,
    from_km: f64,
    to_km: f64,
    duration: Duration,
    dead_zone: Option<Range<Duration>>,
}

impl Drive {
    /// How far along the road the truck is after `elapsed`.
    fn km(&self, elapsed: Duration) -> f64 {
        let f = (elapsed.as_secs_f64() / self.duration.as_secs_f64()).min(1.0);
        self.from_km + (self.to_km - self.from_km) * f
    }
}

#[derive(Debug, Clone)]
enum Motion {
    Parked(Location),
    Driving(Drive),
}

impl Motion {
    fn position(&self, elapsed: Duration) -> Location {
        match self {
            Motion::Parked(at) => *at,
            Motion::Driving(drive) => drive.road.at(drive.km(elapsed)),
        }
    }

    /// The distance driven along the road over the drive's time.
    fn speed_kmh(&self) -> f64 {
        match self {
            Motion::Parked(_) => 0.0,
            Motion::Driving(drive) => {
                (drive.to_km - drive.from_km).abs() / (drive.duration.as_secs_f64() / 3600.0)
            }
        }
    }

    fn in_dead_zone(&self, elapsed: Duration) -> bool {
        matches!(self, Motion::Driving(Drive { dead_zone: Some(zone), .. }) if zone.contains(&elapsed))
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
    rotation: DrumRotation,
    rpm: f64,
}

#[derive(Serialize)]
struct Status {
    status: TruckStatus,
}

pub struct TruckSim {
    truck: Truck,
    home: Arc<PlantSim>,
    world: Arc<Seed>,
    out: mpsc::Sender<Outgoing>,
    clock: Clock,
    modem: Modem,
    rng: StdRng,
    dead_zone_trip_share: f64,
    seq: u64,
    status: Option<TruckStatus>,
    rotation: Option<DrumRotation>,
    /// The road of the latest drive, its plant at km 0, and how far along it the truck is.
    road: Arc<Road>,
    km: f64,
    breakdown: Arc<Notify>,
    repair: Duration,
    roads: Arc<Roads>,
}

impl TruckSim {
    pub fn new(
        truck: Truck,
        home: Arc<PlantSim>,
        world: Arc<Seed>,
        out: mpsc::Sender<Outgoing>,
        clock: Clock,
        config: FleetConfig,
        seed: u64,
    ) -> Self {
        TruckSim {
            truck,
            world,
            out,
            clock,
            modem: Modem::new(config.chaos, seed),
            rng: StdRng::seed_from_u64(seed),
            dead_zone_trip_share: config.dead_zone_trip_share,
            seq: 0,
            status: None,
            rotation: None,
            road: Arc::new(Road::straight(home.plant.location, home.plant.location)),
            km: 0.0,
            breakdown: Arc::new(Notify::new()),
            repair: config.repair,
            roads: Arc::default(),
            home,
        }
    }

    /// Drives along `roads`. A pair without a road, or a truck without roads, drives straight.
    pub fn with_roads(self, roads: Arc<Roads>) -> Self {
        TruckSim { roads, ..self }
    }

    /// Notify it to break this truck down.
    pub fn breakdown(&self) -> Arc<Notify> {
        Arc::clone(&self.breakdown)
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        // Stagger the fleet so 25 trucks don't all queue at once.
        let rest = Duration::from_secs(self.rng.random_range(0..120));
        let home = Motion::Parked(self.home.plant.location);
        self.phase(
            TruckStatus::Available,
            DrumRotation::Idle,
            home,
            sleep(rest),
        )
        .await?;
        loop {
            let breakdown = Arc::clone(&self.breakdown);
            // A breakdown abandons the trip wherever it is; a bay it held is freed with it.
            tokio::select! {
                trip = self.trip() => trip?,
                () = breakdown.notified() => self.break_down().await?,
            }
        }
    }

    /// Out of service where it stands, for the repair, then back to the home plant along its road.
    async fn break_down(&mut self) -> anyhow::Result<()> {
        info!(truck = %self.truck.id.0, "broke down");
        let (road, km, repair) = (Arc::clone(&self.road), self.km, self.repair);
        self.phase(
            TruckStatus::OutOfService,
            DrumRotation::Idle,
            Motion::Parked(road.at(km)),
            sleep(repair),
        )
        .await?;
        info!(truck = %self.truck.id.0, "repaired, driving home");
        let back = self.drive(road, km, 0.0);
        self.phase(
            TruckStatus::Returning,
            DrumRotation::Idle,
            back.0,
            sleep(back.1),
        )
        .await
    }

    async fn trip(&mut self) -> anyhow::Result<()> {
        let plant = Arc::clone(&self.home);
        let plant_at = plant.plant.location;
        let site = self
            .world
            .sites
            .choose(&mut self.rng)
            .context("the seed has no sites")?;
        let site_id = site.id.0.clone();
        let site_at = self.world.geocoder.locate(site).with_context(|| {
            format!("ship-to {site_id} has neither coordinates nor a known postal code")
        })?;
        let road = self
            .roads
            .between(&plant.plant.id, &site.id, plant_at, site_at);
        let length = road.length();
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
                DrumRotation::Idle,
                parked.clone(),
                plant.acquire_bay(),
            )
            .await??;
        let load = plant.load(bay, &truck, &mix, volume);
        let ticket = self
            .phase(
                TruckStatus::Loading,
                DrumRotation::Charging,
                parked.clone(),
                load,
            )
            .await??;
        info!(truck = %truck.0, ticket, site = site_id, "loaded, heading to site");
        let paperwork = self.minutes(1..=3);
        self.phase(
            TruckStatus::Loading,
            DrumRotation::Mixing,
            parked,
            sleep(paperwork),
        )
        .await?;

        let out = self.drive(Arc::clone(&road), 0.0, length);
        self.phase(
            TruckStatus::EnRoute,
            DrumRotation::Mixing,
            out.0,
            sleep(out.1),
        )
        .await?;
        let wait = self.minutes(2..=10);
        self.phase(
            TruckStatus::OnSite,
            DrumRotation::Mixing,
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
            DrumRotation::Discharging,
            on_site,
            sleep(discharge),
        )
        .await?;

        let back = self.drive(road, length, 0.0);
        self.phase(
            TruckStatus::Returning,
            DrumRotation::Idle,
            back.0,
            sleep(back.1),
        )
        .await?;
        let rest = self.minutes(5..=20);
        self.phase(
            TruckStatus::Available,
            DrumRotation::Idle,
            Motion::Parked(plant_at),
            sleep(rest),
        )
        .await?;
        Ok(())
    }

    fn minutes(&mut self, range: std::ops::RangeInclusive<u64>) -> Duration {
        Duration::from_secs(60 * self.rng.random_range(range))
    }

    /// The motion along `road` from `from_km` to `to_km` and how long it takes, with an occasional
    /// dead zone. The time comes from the drive's two ends, whatever the road's length.
    fn drive(&mut self, road: Arc<Road>, from_km: f64, to_km: f64) -> (Motion, Duration) {
        let (from, to) = (road.at(from_km), road.at(to_km));
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
            Motion::Driving(Drive {
                road,
                from_km,
                to_km,
                duration,
                dead_zone,
            }),
            duration,
        )
    }

    /// Reports status, drum and position until `until` completes, then returns its output.
    async fn phase<T>(
        &mut self,
        status: TruckStatus,
        rotation: DrumRotation,
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
                    rpm: rpm(rotation),
                },
            )
            .await?;
        }

        let moving = matches!(motion, Motion::Driving(_));
        if let Motion::Driving(drive) = &motion {
            self.road = Arc::clone(&drive.road);
        }
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
            if let Motion::Driving(drive) = &motion {
                self.km = drive.km(elapsed);
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
                        rpm: rpm(rotation),
                    },
                )
                .await?;
            }
            tick += 1;
            tokio::select! {
                out = &mut until => {
                    // A finished drive leaves the truck at its end, even between ticks. It stays
                    // there, parked, until its next drive, and a breakdown meanwhile starts there.
                    if let Motion::Driving(drive) = &motion {
                        self.km = drive.to_km;
                    }
                    return Ok(out);
                }
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
    use domain::{PlantId, SiteId};
    use serde_json::Value;

    struct Seen {
        topic: String,
        body: Value,
        /// Simulated wall-clock time when the message reached the broker side of the channel.
        received_ms: i64,
    }

    /// Runs one truck along `roads` until it is available again after its first trip.
    async fn one_trip(
        world: Seed,
        roads: Roads,
        dead_zone_trip_share: f64,
    ) -> (Vec<Seen>, Arc<Seed>) {
        let world = Arc::new(world);
        let (tx, mut rx) = mpsc::channel(100_000);
        let clock = Clock::start(Utc.with_ymd_and_hms(2026, 11, 9, 6, 0, 0).unwrap());
        let truck = world.trucks[0].clone();
        let home = world.plant(&truck.home_plant.0).unwrap().clone();
        let plant = Arc::new(PlantSim::new(home, tx.clone(), clock, Chaos::NONE, 1));
        let config = FleetConfig {
            chaos: Chaos::NONE,
            dead_zone_trip_share,
            repair: Duration::from_secs(3600),
        };
        let sim = TruckSim::new(truck, plant, Arc::clone(&world), tx, clock, config, 7)
            .with_roads(Arc::new(roads));
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
        let (seen, world) = one_trip(Seed::rhine_neckar().unwrap(), Roads::default(), 0.0).await;
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
        let (seen, _) = one_trip(Seed::rhine_neckar().unwrap(), Roads::default(), 1.0).await;
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

    #[tokio::test(start_paused = true)]
    async fn trucks_drive_to_the_postal_code_centroid_of_an_address_only_site() {
        let mut world = Seed::rhine_neckar().unwrap();
        world.sites.retain(|s| s.location.is_none());
        let centroids: Vec<Location> = world
            .sites
            .iter()
            .map(|s| world.geocoder.locate(s).unwrap())
            .collect();
        assert!(!centroids.is_empty());

        let (seen, _) = one_trip(world, Roads::default(), 0.0).await;
        let on_site = seen
            .iter()
            .position(|s| s.body["status"] == "on_site")
            .unwrap();
        let fix = seen[on_site..]
            .iter()
            .find(|s| s.topic.ends_with("/gps"))
            .unwrap();
        let (lat, lon) = (
            fix.body["lat"].as_f64().unwrap(),
            fix.body["lon"].as_f64().unwrap(),
        );
        assert!(
            centroids
                .iter()
                .any(|c| (c.lat - lat).abs() < 1e-6 && (c.lon - lon).abs() < 1e-6),
            "parked at {lat},{lon}; expected one of {centroids:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_truck_breaks_down_on_command_and_is_back_after_the_repair() {
        let world = Arc::new(Seed::rhine_neckar().unwrap());
        let (tx, mut rx) = mpsc::channel(100_000);
        let clock = Clock::start(Utc.with_ymd_and_hms(2026, 11, 9, 6, 0, 0).unwrap());
        let truck = world.trucks[0].clone();
        let home = world.plant(&truck.home_plant.0).unwrap().clone();
        let plant = Arc::new(PlantSim::new(home, tx.clone(), clock, Chaos::NONE, 1));
        let config = FleetConfig {
            chaos: Chaos::NONE,
            dead_zone_trip_share: 0.0,
            repair: Duration::from_secs(3600),
        };
        let sim = TruckSim::new(truck, plant, Arc::clone(&world), tx, clock, config, 7);
        let breakdown = sim.breakdown();
        let task = tokio::spawn(sim.run());

        // (status, ts_ms) as reported, from the moment it heads out.
        let mut reported: Vec<(String, i64)> = Vec::new();
        let mut ordered_at = 0;
        loop {
            let msg = rx.recv().await.unwrap();
            let body: Value = serde_json::from_slice(&msg.payload).unwrap();
            let Some(status) = body["status"].as_str() else {
                continue;
            };
            let ts = body["ts_ms"].as_i64().unwrap();
            if status == "en_route" && ordered_at == 0 {
                breakdown.notify_one();
                ordered_at = clock.now().timestamp_millis();
            }
            if ordered_at > 0 {
                reported.push((status.to_owned(), ts));
            }
            if ordered_at > 0 && status == "available" {
                break;
            }
        }
        task.abort();

        let statuses: Vec<&str> = reported.iter().map(|(s, _)| s.as_str()).collect();
        assert_eq!(
            statuses,
            ["en_route", "out_of_service", "returning", "available"]
        );
        let (broke, repaired) = (reported[1].1, reported[2].1);
        assert!(broke - ordered_at < 1_000, "breaks down at once, mid-drive");
        assert!(
            repaired - broke >= 3_600_000,
            "stays out of service for the repair"
        );
    }

    const SITE: &str = "50001001";

    /// The seed with one site, north-east of truck-01's home plant, and an L-shaped road to it:
    /// east along the plant's latitude, then north. Also returns the road's three points.
    fn dog_leg() -> (Seed, Roads, [Location; 3]) {
        let mut world = Seed::rhine_neckar().unwrap();
        world.sites.retain(|s| s.id.0 == SITE);
        let plant = world.plant("plant-mannheim").unwrap().location;
        let site = world.sites[0].location.unwrap();
        assert!(
            site.lat > plant.lat && site.lon > plant.lon,
            "the site lies north-east"
        );
        let corner = Location {
            lat: plant.lat,
            lon: site.lon,
        };
        let road = Road::new(vec![plant, corner, site]).unwrap();
        let roads =
            Roads::from_iter([(PlantId("plant-mannheim".into()), SiteId(SITE.into()), road)]);
        (world, roads, [plant, corner, site])
    }

    fn loc(body: &Value) -> Location {
        Location {
            lat: body["lat"].as_f64().unwrap(),
            lon: body["lon"].as_f64().unwrap(),
        }
    }

    fn close(a: Location, b: Location) -> bool {
        (a.lat - b.lat).abs() < 1e-9 && (a.lon - b.lon).abs() < 1e-9
    }

    /// Metres from `p` to the nearest point of `line`, on a flat projection around `p`.
    fn off_m(p: Location, line: &[Location]) -> f64 {
        const M_PER_DEGREE: f64 = 111_195.0;
        let k = p.lat.to_radians().cos();
        let xy = |l: Location| {
            (
                (l.lon - p.lon) * k * M_PER_DEGREE,
                (l.lat - p.lat) * M_PER_DEGREE,
            )
        };
        line.windows(2)
            .map(|w| {
                let ((ax, ay), (bx, by)) = (xy(w[0]), xy(w[1]));
                let (dx, dy) = (bx - ax, by - ay);
                let len2 = dx * dx + dy * dy;
                let t = if len2 > 0.0 {
                    (-(ax * dx + ay * dy) / len2).clamp(0.0, 1.0)
                } else {
                    0.0
                };
                (ax + t * dx).hypot(ay + t * dy)
            })
            .fold(f64::INFINITY, f64::min)
    }

    #[tokio::test(start_paused = true)]
    async fn a_truck_drives_along_its_road_and_keeps_its_drive_time() {
        let (world, roads, road) = dog_leg();
        let [plant, corner, site] = road;
        let (seen, _) = one_trip(world, roads, 0.0).await;
        assert_eq!(statuses(&seen), LIFECYCLE);

        let en_route = seen
            .iter()
            .position(|s| s.body["status"] == "en_route")
            .unwrap();
        let on_site = seen
            .iter()
            .position(|s| s.body["status"] == "on_site")
            .unwrap();
        let fixes: Vec<&Seen> = seen[en_route..on_site]
            .iter()
            .filter(|s| s.topic.ends_with("/gps"))
            .collect();
        assert!(fixes.len() > 2);
        for fix in &fixes {
            let at = loc(&fix.body);
            assert!(off_m(at, &road) < 1.0, "{at:?} is off the road");
        }
        assert!(
            fixes
                .iter()
                .any(|f| off_m(loc(&f.body), &[plant, site]) > 100.0),
            "the truck drove the straight line"
        );

        // The drive takes the straight model's time, ±10 %, and the speed is the road's km over it.
        let driven_ms = (ts_of_status(&seen, "on_site") - ts_of_status(&seen, "en_route")) as f64;
        let nominal_ms = plant.distance_km(site) * ROAD_FACTOR / AVG_SPEED_KMH * 3_600_000.0;
        let ratio = driven_ms / nominal_ms;
        assert!(
            (0.89..=1.11).contains(&ratio),
            "drove {driven_ms} ms, model says {nominal_ms} ms"
        );
        let speed =
            (plant.distance_km(corner) + corner.distance_km(site)) / (driven_ms / 3_600_000.0);
        for fix in &fixes {
            let reported = fix.body["speed_kmh"].as_f64().unwrap();
            assert!(
                (reported - speed).abs() < 0.06,
                "reported {reported} km/h, drove {speed} km/h"
            );
        }

        let parked = seen[on_site..]
            .iter()
            .find(|s| s.topic.ends_with("/gps"))
            .unwrap();
        assert!(
            close(loc(&parked.body), site),
            "the first fix on site is the site"
        );

        // The way back is the same road, from the site.
        let returning = seen
            .iter()
            .position(|s| s.body["status"] == "returning")
            .unwrap();
        let back: Vec<Location> = seen[returning..]
            .iter()
            .filter(|s| s.topic.ends_with("/gps"))
            .map(|s| loc(&s.body))
            .collect();
        assert!(
            back.len() > 2 && close(back[0], site),
            "the way back starts on site"
        );
        for at in &back {
            assert!(off_m(*at, &road) < 1.0, "{at:?} is off the road back");
        }

        // The same seed on the straight line takes exactly as long: the time comes from the ends.
        let (straight, _) = one_trip(dog_leg().0, Roads::default(), 0.0).await;
        let straight_ms = ts_of_status(&straight, "on_site") - ts_of_status(&straight, "en_route");
        assert!(
            (driven_ms - straight_ms as f64).abs() <= 1.0,
            "drove {driven_ms} ms along the road and {straight_ms} ms straight"
        );
    }

    /// Where a truck broke down and parked, its fixes on the way home, and its first fix there.
    struct Breakdown {
        at: Location,
        parked: Location,
        way_home: Vec<Location>,
        home: Location,
    }

    /// Runs truck-01 on the dog-leg road, breaks it down on the first GPS fix for which
    /// `now(status, fix)` holds, and follows it home.
    async fn break_down_when(now: impl Fn(&str, Location) -> bool) -> Breakdown {
        let (world, roads, _) = dog_leg();
        let world = Arc::new(world);
        let (tx, mut rx) = mpsc::channel(100_000);
        let clock = Clock::start(Utc.with_ymd_and_hms(2026, 11, 9, 6, 0, 0).unwrap());
        let truck = world.trucks[0].clone();
        let plant = world.plant(&truck.home_plant.0).unwrap().clone();
        let plant = Arc::new(PlantSim::new(plant, tx.clone(), clock, Chaos::NONE, 1));
        let config = FleetConfig {
            chaos: Chaos::NONE,
            dead_zone_trip_share: 0.0,
            repair: Duration::from_secs(3600),
        };
        let sim = TruckSim::new(truck, plant, Arc::clone(&world), tx, clock, config, 7)
            .with_roads(Arc::new(roads));
        let breakdown = sim.breakdown();
        let task = tokio::spawn(sim.run());

        let mut status = String::new();
        let (mut at, mut parked, mut way_home) = (None, None, Vec::new());
        let home = loop {
            let msg = rx.recv().await.unwrap();
            if !msg.topic.starts_with("trucks/") {
                continue;
            }
            let body: Value = serde_json::from_slice(&msg.payload).unwrap();
            if let Some(s) = body["status"].as_str() {
                status = s.to_owned();
                continue;
            }
            if !msg.topic.ends_with("/gps") {
                continue;
            }
            let fix = loc(&body);
            match status.as_str() {
                _ if at.is_none() && now(&status, fix) => {
                    breakdown.notify_one();
                    at = Some(fix);
                }
                "out_of_service" if parked.is_none() => parked = Some(fix),
                "returning" => way_home.push(fix),
                "available" if at.is_some() => break fix,
                _ => {}
            }
        };
        task.abort();
        Breakdown {
            at: at.unwrap(),
            parked: parked.unwrap(),
            way_home,
            home,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_broken_down_truck_drives_home_along_its_road() {
        let (_, _, [plant, corner, _]) = dog_leg();
        // It breaks down on its first fix at least 100 m past the corner, where retracing the road
        // and driving straight home part ways.
        let run = break_down_when(|status, fix| {
            status == "en_route" && fix.lat > corner.lat && corner.distance_km(fix) >= 0.1
        })
        .await;
        assert!(close(run.parked, run.at), "it parks where it broke down");
        assert!(run.way_home.len() > 2);
        for fix in &run.way_home {
            assert!(
                off_m(*fix, &[run.at, corner, plant]) < 1.0,
                "{fix:?} is off the road home"
            );
        }
        assert!(close(run.home, plant), "the first fix at home is the plant");
    }

    #[tokio::test(start_paused = true)]
    async fn a_truck_that_breaks_down_on_site_drives_the_whole_road_home() {
        let (_, _, road) = dog_leg();
        let [plant, _, site] = road;
        let run = break_down_when(|status, _| status == "on_site").await;
        assert!(
            close(run.parked, site),
            "it parks on site, where its drive ended"
        );
        assert!(
            run.way_home.len() > 2 && close(run.way_home[0], site),
            "it sets off from the site"
        );
        for fix in &run.way_home {
            assert!(off_m(*fix, &road) < 1.0, "{fix:?} is off the road home");
        }
        assert!(close(run.home, plant), "the first fix at home is the plant");
    }
}
