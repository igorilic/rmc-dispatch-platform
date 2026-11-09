//! Construction heuristic, the first step of SPEC §4's approach. It turns orders into a plan of
//! truck loads, and never breaks a hard constraint.
//!
//! Open orders starting inside the horizon are taken by requested start. Each is split into loads:
//! full drums, with the remainder as the last load. Every online plant is tried with every truck in
//! service for each load:
//! 1. The target arrival is the requested start for the first load. For later loads it is the moment
//!    the previous load finishes discharging, so the pour stays continuous.
//! 2. The batch time is worked back from the target: travel, batching and loading.
//! 3. The batch time then moves later until the plant is open, a loading bay is free, and the truck
//!    is free. A free truck can drive empty from its previous site in time, and can still reach its
//!    next trip afterwards.
//! 4. A slot whose discharge would end after the mix design's limit is rejected.
//!
//! The slot arriving earliest wins, and fewer empty kilometres break ties. If one of an order's loads
//! can't be placed, none are: the order is unplanned, with the reason.
//!
//! [`crate::verify`] re-checks the hard constraints on the finished plan, independently of this code.
//! [`Metrics`] measures the soft objectives (pour gaps, lateness, empty km); M5's local search
//! optimises them.

use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, NaiveTime, TimeDelta, TimeZone, Utc};
use chrono_tz::Europe::Berlin;
use domain::{
    Load, Location, MixCode, Order, OrderId, OrderStatus, Plant, PlantStatus, Seed, SiteId,
    TimeWindow, Truck, TruckStatus,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::travel::TravelTimeProvider;

const VOLUME_EPSILON: f64 = 1e-6;
/// Moves of a candidate slot before giving up on one plant and truck pair.
const SEARCH_STEPS: usize = 500;

/// What the planner plans with: master data, with every site already located.
#[derive(Debug, Clone)]
pub struct World {
    pub plants: Vec<Plant>,
    pub trucks: Vec<Truck>,
    /// Located ship-tos. A ship-to missing here can't be planned.
    pub sites: HashMap<SiteId, Location>,
    /// Each mix design's maximum minutes from batching until discharge ends.
    pub mixes: HashMap<MixCode, u32>,
}

impl World {
    /// Places address-only ship-tos at their postal code's centroid.
    pub fn from_seed(seed: &Seed) -> World {
        World {
            plants: seed.plants.clone(),
            trucks: seed.trucks.clone(),
            sites: seed
                .sites
                .iter()
                .filter_map(|s| seed.geocoder.locate(s).map(|at| (s.id.clone(), at)))
                .collect(),
            mixes: seed
                .mixes
                .iter()
                .map(|m| (m.code.clone(), m.max_minutes_from_batching))
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Plan {
    /// When the plan was made; trucks start from their home plants at this moment.
    pub created_at: DateTime<Utc>,
    pub horizon_end: DateTime<Utc>,
    /// Grouped by order and ordered by sequence.
    pub loads: Vec<Load>,
    pub unplanned: Vec<Unplanned>,
    pub metrics: Metrics,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Unplanned {
    pub order: OrderId,
    pub reason: String,
}

/// The soft objectives of SPEC §4, measured on a plan.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Metrics {
    pub planned_orders: usize,
    pub unplanned_orders: usize,
    pub loads: usize,
    /// Minutes the first loads arrive after the requested start, summed over orders.
    pub lateness_minutes: f64,
    /// The longest wait at a site between one load finishing and the next arriving.
    pub max_gap_minutes: f64,
    /// Waits longer than the order's maximum gap: a cold-joint risk.
    pub gaps_over_limit: usize,
    /// Kilometres trucks drive empty to reach their loading plant.
    pub empty_km: f64,
}

/// Batching at the plant's rate, then 3 minutes to load the truck.
pub fn plant_time(volume_m3: f64, plant: &Plant) -> TimeDelta {
    hours(volume_m3 / plant.batching_rate_m3_per_h) + TimeDelta::minutes(3)
}

/// Discharging at the order's pour rate.
pub fn discharge_time(volume_m3: f64, order: &Order) -> TimeDelta {
    hours(volume_m3 / order.pour_rate_m3_per_h)
}

/// When a load finishes discharging on site.
pub fn discharge_end(load: &Load, order: &Order) -> DateTime<Utc> {
    load.eta_on_site + discharge_time(load.volume_m3, order)
}

fn hours(h: f64) -> TimeDelta {
    TimeDelta::milliseconds((h * 3_600_000.0).round() as i64)
}

fn minutes(d: TimeDelta) -> f64 {
    d.num_milliseconds() as f64 / 60_000.0
}

/// Whether an order is planned at all: open (not cancelled, not started) and starting inside the horizon.
pub fn plannable(order: &Order, now: DateTime<Utc>, horizon_end: DateTime<Utc>) -> bool {
    // ponytail: an order whose start passed over an hour ago is left alone; M5's re-dispatch owns late orders.
    matches!(
        order.status,
        OrderStatus::Unconfirmed | OrderStatus::Pending | OrderStatus::Confirmed
    ) && order.requested_start >= now - TimeDelta::hours(1)
        && order.requested_start < horizon_end
}

/// Plans every open order starting before `now + horizon`.
pub fn plan(
    world: &World,
    orders: &[Order],
    now: DateTime<Utc>,
    horizon: TimeDelta,
    travel: &dyn TravelTimeProvider,
) -> Plan {
    let horizon_end = now + horizon;
    let homes: HashMap<_, _> = world.plants.iter().map(|p| (&p.id, p.location)).collect();
    let mut planner = Planner {
        world,
        travel,
        now,
        // ponytail: loads may land up to a day past the horizon; a long pour that starts near the end still fits.
        search_until: horizon_end + TimeDelta::days(1),
        plants: world
            .plants
            .iter()
            .filter(|p| p.status == PlantStatus::Online)
            .map(|plant| PlantState {
                plant,
                bays: Vec::new(),
            })
            .collect(),
        trucks: world
            .trucks
            .iter()
            .filter(|t| t.status != TruckStatus::OutOfService)
            .filter_map(|truck| {
                Some(TruckState {
                    truck,
                    home: *homes.get(&truck.home_plant)?,
                    trips: Vec::new(),
                })
            })
            .collect(),
        empty_km: 0.0,
    };
    let mut open: Vec<&Order> = orders
        .iter()
        .filter(|o| plannable(o, now, horizon_end))
        .collect();
    open.sort_by(|a, b| (a.requested_start, &a.id.0).cmp(&(b.requested_start, &b.id.0)));

    let (mut loads, mut unplanned) = (Vec::new(), Vec::new());
    for order in open {
        match planner.plan_order(order) {
            Ok(order_loads) => loads.extend(order_loads),
            Err(reason) => unplanned.push(Unplanned {
                order: order.id.clone(),
                reason,
            }),
        }
    }
    let metrics = measure(&loads, &unplanned, orders, planner.empty_km);
    Plan {
        created_at: now,
        horizon_end,
        loads,
        unplanned,
        metrics,
    }
}

#[derive(Debug, Clone)]
struct Trip {
    batch_at: DateTime<Utc>,
    plant: Location,
    site: Location,
    discharge_end: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct TruckState<'a> {
    truck: &'a Truck,
    home: Location,
    /// Sorted by batch time.
    trips: Vec<Trip>,
}

impl TruckState<'_> {
    /// Where the truck is, and from when it is free, for a trip batching at `at`.
    fn free_before(&self, at: DateTime<Utc>, now: DateTime<Utc>) -> (Location, DateTime<Utc>) {
        self.trips
            .iter()
            .rev()
            .find(|t| t.batch_at <= at)
            .map_or((self.home, now), |t| (t.site, t.discharge_end))
    }

    fn next_after(&self, at: DateTime<Utc>) -> Option<&Trip> {
        self.trips.iter().find(|t| t.batch_at > at)
    }

    fn add(&mut self, trip: Trip) {
        let index = self.trips.partition_point(|t| t.batch_at <= trip.batch_at);
        self.trips.insert(index, trip);
    }
}

#[derive(Debug, Clone)]
struct PlantState<'a> {
    plant: &'a Plant,
    /// Batching plus loading, per load.
    bays: Vec<(DateTime<Utc>, DateTime<Utc>)>,
}

impl PlantState<'_> {
    /// `None` if a bay is free for all of `[start, end)`. Otherwise the earliest moment one frees up.
    // ponytail: rescans every interval, O(loads²) per plant; index by time if M5's 1,000-load benchmark needs it.
    fn bay_conflict(&self, start: DateTime<Utc>, end: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let overlapping: Vec<_> = self
            .bays
            .iter()
            .filter(|(s, e)| *s < end && start < *e)
            .collect();
        let bays = self.plant.loading_bays as usize;
        if overlapping.len() < bays {
            return None;
        }
        let points = std::iter::once(start)
            .chain(overlapping.iter().map(|(s, _)| *s).filter(|s| *s > start));
        let busiest = points
            .map(|p| {
                overlapping
                    .iter()
                    .filter(|(s, e)| *s <= p && p < *e)
                    .count()
            })
            .max();
        if busiest.unwrap_or(0) < bays {
            None
        } else {
            overlapping.iter().map(|(_, e)| *e).min()
        }
    }

    /// `None` if `[start, end)` is inside opening hours. Otherwise the next opening.
    fn closed(&self, start: DateTime<Utc>, end: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let hours = self.plant.operating_hours;
        let day = start.with_timezone(&Berlin).date_naive();
        let at = |day: NaiveDate, time: NaiveTime| {
            Berlin
                .from_local_datetime(&day.and_time(time))
                .earliest()
                .map(|t| t.with_timezone(&Utc))
        };
        // A DST gap at opening time is not worth modelling: try again an hour later.
        let (Some(open), Some(close)) = (at(day, hours.open), at(day, hours.close)) else {
            return Some(start + TimeDelta::hours(1));
        };
        if start < open {
            Some(open)
        } else if end > close {
            day.succ_opt()
                .and_then(|next| at(next, hours.open))
                .or(Some(start + TimeDelta::hours(1)))
        } else {
            None
        }
    }
}

struct Candidate {
    plant: usize,
    truck: usize,
    volume: f64,
    batch_at: DateTime<Utc>,
    eta: DateTime<Utc>,
    discharge_end: DateTime<Utc>,
    empty_km: f64,
}

enum Slot {
    Found(Candidate),
    /// Discharge can't end within the mix limit from this plant.
    TooFar,
    /// No free bay and truck before the search limit.
    Busy,
}

struct Planner<'a> {
    world: &'a World,
    travel: &'a dyn TravelTimeProvider,
    now: DateTime<Utc>,
    search_until: DateTime<Utc>,
    plants: Vec<PlantState<'a>>,
    trucks: Vec<TruckState<'a>>,
    empty_km: f64,
}

impl Planner<'_> {
    fn plan_order(&mut self, order: &Order) -> Result<Vec<Load>, String> {
        let site = *self.world.sites.get(&order.site).ok_or_else(|| {
            format!(
                "ship-to {} has neither coordinates nor a known postal code",
                order.site.0
            )
        })?;
        let max_minutes = *self
            .world
            .mixes
            .get(&order.mix)
            .ok_or_else(|| format!("unknown mix {}", order.mix.0))?;
        if self.plants.is_empty() {
            return Err("no plant is online".into());
        }
        if self.trucks.is_empty() {
            return Err("no truck is in service".into());
        }

        // An order is planned whole or not at all, so keep the state to roll back to.
        let saved = (self.plants.clone(), self.trucks.clone());
        let (mut loads, mut empty_km) = (Vec::new(), 0.0);
        let (mut remaining, mut target) = (order.total_volume_m3, order.requested_start);
        while remaining > VOLUME_EPSILON {
            let mut best: Option<Candidate> = None;
            let mut too_far_everywhere = true;
            for p in 0..self.plants.len() {
                for t in 0..self.trucks.len() {
                    let volume = remaining.min(self.trucks[t].truck.drum_capacity_m3);
                    match self.earliest_slot(p, t, order, site, volume, target, max_minutes) {
                        Slot::Found(c) => {
                            too_far_everywhere = false;
                            let better = best.as_ref().is_none_or(|b| {
                                (c.eta, c.empty_km).partial_cmp(&(b.eta, b.empty_km))
                                    == Some(std::cmp::Ordering::Less)
                            });
                            if better {
                                best = Some(c);
                            }
                        }
                        Slot::Busy => too_far_everywhere = false,
                        Slot::TooFar => {}
                    }
                }
            }
            let Some(c) = best else {
                (self.plants, self.trucks) = saved;
                return Err(if too_far_everywhere {
                    format!(
                        "ship-to {} is too far: no online plant can deliver {} within its {max_minutes} min limit",
                        order.site.0, order.mix.0
                    )
                } else {
                    format!(
                        "no truck and loading bay are free in time (searched until {})",
                        self.search_until
                    )
                });
            };
            let plant = &mut self.plants[c.plant];
            plant
                .bays
                .push((c.batch_at, c.batch_at + plant_time(c.volume, plant.plant)));
            self.trucks[c.truck].add(Trip {
                batch_at: c.batch_at,
                plant: plant.plant.location,
                site,
                discharge_end: c.discharge_end,
            });
            loads.push(Load {
                order: order.id.clone(),
                sequence: loads.len() as u32 + 1,
                volume_m3: c.volume,
                planned_plant: plant.plant.id.clone(),
                planned_truck: self.trucks[c.truck].truck.id.clone(),
                planned_batch_at: c.batch_at,
                eta_on_site: c.eta,
                discharge_window: TimeWindow {
                    start: c.eta,
                    end: c.batch_at + TimeDelta::minutes(i64::from(max_minutes)),
                },
            });
            empty_km += c.empty_km;
            remaining -= c.volume;
            target = c.discharge_end;
        }
        self.empty_km += empty_km;
        Ok(loads)
    }

    #[allow(clippy::too_many_arguments)]
    fn earliest_slot(
        &self,
        p: usize,
        t: usize,
        order: &Order,
        site: Location,
        volume: f64,
        target: DateTime<Utc>,
        max_minutes: u32,
    ) -> Slot {
        let (plant, truck) = (&self.plants[p], &self.trucks[t]);
        let at_plant = plant.plant.location;
        let load_time = plant_time(volume, plant.plant);
        let mix_limit = TimeDelta::minutes(i64::from(max_minutes));
        // Work back from the target. Drive time depends on the hour, so the first guess is refined below.
        let mut batch_at =
            (target - load_time - self.travel.travel(at_plant, site, target)).max(self.now);
        for _ in 0..SEARCH_STEPS {
            if batch_at > self.search_until {
                return Slot::Busy;
            }
            let loaded = batch_at + load_time;
            let eta = loaded + self.travel.travel(at_plant, site, loaded);
            let discharge_end = eta + discharge_time(volume, order);
            if discharge_end > batch_at + mix_limit {
                return Slot::TooFar;
            }
            // Arriving early means concrete ageing on site: start later instead.
            if eta < target {
                batch_at += target - eta;
                continue;
            }
            if let Some(next) = plant.closed(batch_at, loaded) {
                batch_at = next;
                continue;
            }
            if let Some(next) = plant.bay_conflict(batch_at, loaded) {
                batch_at = next;
                continue;
            }
            let (from, free_at) = truck.free_before(batch_at, self.now);
            let reach_plant = free_at + self.travel.travel(from, at_plant, free_at);
            if reach_plant > batch_at {
                batch_at = reach_plant;
                continue;
            }
            if let Some(next) = truck.next_after(batch_at)
                && discharge_end + self.travel.travel(site, next.plant, discharge_end)
                    > next.batch_at
            {
                // Doesn't fit before the truck's next trip: try right after it.
                batch_at = next.discharge_end
                    + self.travel.travel(next.site, at_plant, next.discharge_end);
                continue;
            }
            let empty_km = self.travel.distance_km(from, at_plant);
            return Slot::Found(Candidate {
                plant: p,
                truck: t,
                volume,
                batch_at,
                eta,
                discharge_end,
                empty_km,
            });
        }
        Slot::Busy
    }
}

fn measure(loads: &[Load], unplanned: &[Unplanned], orders: &[Order], empty_km: f64) -> Metrics {
    let by_id: HashMap<&OrderId, &Order> = orders.iter().map(|o| (&o.id, o)).collect();
    let mut m = Metrics {
        loads: loads.len(),
        unplanned_orders: unplanned.len(),
        empty_km,
        ..Metrics::default()
    };
    for order_loads in loads.chunk_by(|a, b| a.order == b.order) {
        let Some(order) = by_id.get(&order_loads[0].order) else {
            continue;
        };
        m.planned_orders += 1;
        m.lateness_minutes +=
            minutes((order_loads[0].eta_on_site - order.requested_start).max(TimeDelta::zero()));
        for pair in order_loads.windows(2) {
            let gap = minutes(pair[1].eta_on_site - discharge_end(&pair[0], order));
            m.max_gap_minutes = m.max_gap_minutes.max(gap);
            if gap > f64::from(order.max_gap_minutes) {
                m.gaps_over_limit += 1;
            }
        }
    }
    m
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use domain::{Customer, CustomerId, OperatingHours, PlantId, Seed, TruckId};
    use proptest::prelude::*;
    use rand::{RngExt, SeedableRng, rngs::StdRng};

    use super::*;
    use crate::travel::HaversineTravelTime;
    use crate::verify::violations;

    const WALLDORF: Location = Location {
        lat: 49.3064,
        lon: 8.6428,
    };
    const HEIDELBERG: Location = Location {
        lat: 49.3988,
        lon: 8.6724,
    };
    const LEIMEN: Location = Location {
        lat: 49.3481,
        lon: 8.6911,
    };
    const FRANKFURT: Location = Location {
        lat: 50.1109,
        lon: 8.6821,
    };

    fn at(h: u32, m: u32) -> DateTime<Utc> {
        Berlin
            .with_ymd_and_hms(2026, 11, 10, h, m, 0)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn plant(id: &str, location: Location, bays: u32, status: PlantStatus) -> Plant {
        Plant {
            id: PlantId(id.into()),
            location,
            loading_bays: bays,
            batching_rate_m3_per_h: 120.0,
            operating_hours: OperatingHours {
                open: "05:00:00".parse().unwrap(),
                close: "20:00:00".parse().unwrap(),
            },
            status,
        }
    }

    fn truck(id: &str, home: &str, capacity: f64, status: TruckStatus) -> Truck {
        Truck {
            id: TruckId(id.into()),
            home_plant: PlantId(home.into()),
            drum_capacity_m3: capacity,
            status,
            last_position: None,
        }
    }

    fn order(id: &str, site: &str, volume: f64, start: DateTime<Utc>) -> Order {
        Order {
            id: OrderId(id.into()),
            customer: CustomerId("10001001".into()),
            site: SiteId(site.into()),
            mix: MixCode("C30/37-XC4".into()),
            total_volume_m3: volume,
            requested_start: start,
            pour_rate_m3_per_h: 30.0,
            max_gap_minutes: 20,
            status: OrderStatus::Pending,
        }
    }

    fn world(plants: Vec<Plant>, trucks: Vec<Truck>, sites: &[(&str, Location)]) -> World {
        World {
            plants,
            trucks,
            sites: sites
                .iter()
                .map(|(id, loc)| (SiteId((*id).into()), *loc))
                .collect(),
            mixes: [(MixCode("C30/37-XC4".into()), 90)].into(),
        }
    }

    fn run(world: &World, orders: &[Order], now: DateTime<Utc>) -> Plan {
        let travel = HaversineTravelTime::default();
        let plan = plan(world, orders, now, TimeDelta::hours(48), &travel);
        let broken = violations(&plan, world, orders, &travel);
        assert!(broken.is_empty(), "{broken:#?}");
        plan
    }

    fn volumes(plan: &Plan) -> Vec<f64> {
        plan.loads.iter().map(|l| l.volume_m3).collect()
    }

    #[test]
    fn enough_trucks_pour_continuously_in_full_drums() {
        let trucks = (1..=5)
            .map(|i| truck(&format!("t{i}"), "p", 9.0, TruckStatus::Available))
            .collect();
        let w = world(
            vec![plant("p", WALLDORF, 2, PlantStatus::Online)],
            trucks,
            &[("s", HEIDELBERG)],
        );
        let o = order("o", "s", 42.5, at(7, 0));
        let plan = run(&w, std::slice::from_ref(&o), at(5, 0));

        assert_eq!(volumes(&plan), [9.0, 9.0, 9.0, 9.0, 6.5]);
        assert_eq!(plan.loads[0].eta_on_site, at(7, 0), "on time");
        for pair in plan.loads.windows(2) {
            assert_eq!(
                pair[1].eta_on_site,
                discharge_end(&pair[0], &o),
                "back to back"
            );
        }
        assert_eq!(
            (plan.metrics.lateness_minutes, plan.metrics.max_gap_minutes),
            (0.0, 0.0)
        );
        assert!(plan.unplanned.is_empty());
    }

    #[test]
    fn one_bay_batches_one_load_at_a_time() {
        let trucks = vec![
            truck("t1", "p", 9.0, TruckStatus::Available),
            truck("t2", "p", 9.0, TruckStatus::Available),
        ];
        let w = world(
            vec![plant("p", WALLDORF, 1, PlantStatus::Online)],
            trucks,
            &[("a", HEIDELBERG), ("b", LEIMEN)],
        );
        let orders = [
            order("a", "a", 9.0, at(9, 0)),
            order("b", "b", 9.0, at(9, 0)),
        ];
        let plan = run(&w, &orders, at(5, 0));
        let (a, b) = (&plan.loads[0], &plan.loads[1]);
        assert!(b.planned_batch_at >= a.planned_batch_at + plant_time(a.volume_m3, &w.plants[0]));
    }

    #[test]
    fn a_lone_truck_comes_back_for_the_next_load_and_the_gap_is_counted() {
        let w = world(
            vec![plant("p", WALLDORF, 2, PlantStatus::Online)],
            vec![truck("t", "p", 9.0, TruckStatus::Available)],
            &[("s", HEIDELBERG)],
        );
        let o = order("o", "s", 18.0, at(10, 0));
        let plan = run(&w, std::slice::from_ref(&o), at(5, 0));
        assert_eq!(volumes(&plan), [9.0, 9.0]);
        assert!(plan.loads[1].eta_on_site > discharge_end(&plan.loads[0], &o));
        assert!(plan.metrics.max_gap_minutes > 20.0);
        assert_eq!(
            plan.metrics.gaps_over_limit, 1,
            "a long gap is allowed but counted (soft)"
        );
    }

    #[test]
    fn a_site_too_far_for_the_mix_is_unplanned_with_a_reason() {
        let w = world(
            vec![plant("p", WALLDORF, 2, PlantStatus::Online)],
            vec![truck("t", "p", 9.0, TruckStatus::Available)],
            &[("far", FRANKFURT), ("near", HEIDELBERG)],
        );
        let orders = [
            order("far", "far", 9.0, at(11, 0)),
            order("near", "near", 9.0, at(11, 0)),
        ];
        let plan = run(&w, &orders, at(5, 0));
        assert_eq!(plan.unplanned.len(), 1);
        assert_eq!(plan.unplanned[0].order, OrderId("far".into()));
        assert!(
            plan.unplanned[0].reason.contains("too far"),
            "{}",
            plan.unplanned[0].reason
        );
        assert_eq!(plan.loads.len(), 1, "the other order is still planned");
    }

    #[test]
    fn offline_plants_and_trucks_out_of_service_are_never_used() {
        let w = world(
            vec![
                plant("near", HEIDELBERG, 2, PlantStatus::Offline),
                plant("far", WALLDORF, 2, PlantStatus::Online),
            ],
            vec![
                truck("broken", "far", 9.0, TruckStatus::OutOfService),
                truck("ok", "far", 9.0, TruckStatus::Available),
            ],
            &[("s", HEIDELBERG)],
        );
        let plan = run(&w, &[order("o", "s", 18.0, at(10, 0))], at(5, 0));
        assert!(
            plan.loads
                .iter()
                .all(|l| l.planned_plant.0 == "far" && l.planned_truck.0 == "ok")
        );
    }

    #[test]
    fn orders_that_cannot_be_placed_or_described_say_why() {
        let w = world(
            vec![plant("p", WALLDORF, 2, PlantStatus::Online)],
            vec![truck("t", "p", 9.0, TruckStatus::Available)],
            &[("s", HEIDELBERG)],
        );
        let unknown_mix = Order {
            mix: MixCode("C99/99".into()),
            ..order("mix", "s", 9.0, at(10, 0))
        };
        let plan = run(
            &w,
            &[order("nowhere", "not-located", 9.0, at(10, 0)), unknown_mix],
            at(5, 0),
        );
        let reasons: Vec<&str> = plan.unplanned.iter().map(|u| u.reason.as_str()).collect();
        // Same start, so orders are taken by id: "mix" before "nowhere".
        assert_eq!(
            reasons,
            [
                "unknown mix C99/99",
                "ship-to not-located has neither coordinates nor a known postal code"
            ]
        );
    }

    #[test]
    fn only_open_orders_inside_the_horizon_are_considered() {
        let w = world(
            vec![plant("p", WALLDORF, 2, PlantStatus::Online)],
            vec![truck("t", "p", 9.0, TruckStatus::Available)],
            &[("s", HEIDELBERG)],
        );
        let cancelled = Order {
            status: OrderStatus::Cancelled,
            ..order("cancelled", "s", 9.0, at(10, 0))
        };
        let started = Order {
            status: OrderStatus::InProgress,
            ..order("started", "s", 9.0, at(10, 0))
        };
        let later = order("later", "s", 9.0, at(10, 0) + TimeDelta::days(3));
        let plan = run(&w, &[cancelled, started, later], at(5, 0));
        assert!(plan.loads.is_empty() && plan.unplanned.is_empty());
    }

    #[test]
    fn batching_waits_for_the_plant_to_open() {
        let mut p = plant("p", WALLDORF, 2, PlantStatus::Online);
        p.operating_hours.open = "06:00:00".parse().unwrap();
        let w = world(
            vec![p],
            vec![truck("t", "p", 9.0, TruckStatus::Available)],
            &[("s", HEIDELBERG)],
        );
        let plan = run(&w, &[order("o", "s", 9.0, at(6, 0))], at(4, 0));
        assert_eq!(plan.loads[0].planned_batch_at, at(6, 0));
        assert!(plan.metrics.lateness_minutes > 0.0);
    }

    fn place(rng: &mut StdRng, spread: f64) -> Location {
        Location {
            lat: 49.35 + rng.random_range(-spread..spread),
            lon: 8.65 + rng.random_range(-spread..spread),
        }
    }

    /// A random world: plants (some offline), trucks (some out of service), sites (some far away),
    /// mixes with short and long limits, and orders around the clock.
    fn random_world(seed: u64) -> (World, Vec<Order>, DateTime<Utc>) {
        let mut rng = StdRng::seed_from_u64(seed);
        let now = at(4, 0);
        let plants: Vec<Plant> = (0..rng.random_range(1..=3))
            .map(|i| {
                let status = if rng.random_bool(0.8) {
                    PlantStatus::Online
                } else {
                    PlantStatus::Offline
                };
                let mut p = plant(
                    &format!("p{i}"),
                    place(&mut rng, 0.15),
                    rng.random_range(1..=3),
                    status,
                );
                p.batching_rate_m3_per_h = rng.random_range(60.0..150.0);
                p.operating_hours.open = format!("0{}:00:00", rng.random_range(5..=7))
                    .parse()
                    .unwrap();
                p.operating_hours.close = format!("{}:00:00", rng.random_range(17..=20))
                    .parse()
                    .unwrap();
                p
            })
            .collect();
        let trucks = (0..rng.random_range(1..=8))
            .map(|i| {
                let home = &plants[rng.random_range(0..plants.len())].id.0;
                let status = if rng.random_bool(0.15) {
                    TruckStatus::OutOfService
                } else {
                    TruckStatus::Available
                };
                truck(
                    &format!("t{i}"),
                    home,
                    rng.random_range(4..=15) as f64,
                    status,
                )
            })
            .collect();
        let sites: Vec<(String, Location)> = (0..rng.random_range(2..=8))
            .map(|i| {
                let spread = if rng.random_bool(0.2) { 0.8 } else { 0.25 };
                (format!("s{i}"), place(&mut rng, spread))
            })
            .collect();
        let mixes = [
            (MixCode("short".into()), rng.random_range(45..=75)),
            (MixCode("long".into()), 150),
        ];
        let orders = (0..rng.random_range(1..=12))
            .map(|i| {
                let site = &sites[rng.random_range(0..sites.len())].0;
                let start = now + TimeDelta::minutes(15 * rng.random_range(0..120));
                let mut o = order(
                    &format!("o{i}"),
                    site,
                    rng.random_range(1..=160) as f64 / 2.0,
                    start,
                );
                o.mix = mixes[rng.random_range(0..2)].0.clone();
                o.pour_rate_m3_per_h = rng.random_range(10.0..60.0);
                o.max_gap_minutes = rng.random_range(10..=30);
                o.status = [
                    OrderStatus::Pending,
                    OrderStatus::Confirmed,
                    OrderStatus::Cancelled,
                ][rng.random_range(0..3)];
                o
            })
            .collect();
        let sites = sites
            .into_iter()
            .map(|(id, loc)| (SiteId(id), loc))
            .collect();
        (
            World {
                plants,
                trucks,
                sites,
                mixes: mixes.into(),
            },
            orders,
            now,
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(300))]

        /// Every SPEC §4 hard constraint, checked independently of the planner, on random worlds.
        #[test]
        fn plans_never_break_a_hard_constraint(seed in any::<u64>()) {
            let (world, orders, now) = random_world(seed);
            let travel = HaversineTravelTime::default();
            let plan = plan(&world, &orders, now, TimeDelta::hours(48), &travel);
            let broken = violations(&plan, &world, &orders, &travel);
            prop_assert!(broken.is_empty(), "seed {seed}: {broken:#?}");
        }

        /// The same on the seeded Rhine-Neckar world, with random orders for its real ship-tos.
        #[test]
        fn plans_for_the_seeded_world_never_break_a_hard_constraint(seed in any::<u64>()) {
            let master = Seed::rhine_neckar().unwrap();
            let world = World::from_seed(&master);
            let mut rng = StdRng::seed_from_u64(seed);
            let now = at(4, 0);
            let orders: Vec<Order> = (0..rng.random_range(5..=25)).map(|i| {
                let site = &master.sites[rng.random_range(0..master.sites.len())];
                let customer: &Customer = master.customer(&site.customer.0).unwrap();
                let mut o = order(&format!("o{i}"), &site.id.0, rng.random_range(4..=240) as f64 / 2.0,
                    now + TimeDelta::minutes(15 * rng.random_range(4..100)));
                o.customer = customer.id.clone();
                o.mix = master.mixes[rng.random_range(0..master.mixes.len())].code.clone();
                o
            }).collect();
            let travel = HaversineTravelTime::default();
            let plan = plan(&world, &orders, now, TimeDelta::hours(48), &travel);
            let broken = violations(&plan, &world, &orders, &travel);
            prop_assert!(broken.is_empty(), "seed {seed}: {broken:#?}");
        }
    }
}
