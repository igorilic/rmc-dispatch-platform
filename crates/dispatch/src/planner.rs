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
//! Frozen loads (batching has started) are facts. They keep their bay and truck busy, count towards
//! their order's volume, and leave their sequence numbers to themselves. An order's remaining loads
//! follow on after its last frozen one finishes discharging. Trucks start from their last reported
//! position, or from their home plant until the first GPS fix.
//!
//! [`crate::verify`] re-checks the hard constraints on the finished plan, independently of this code.
//! [`Metrics`] measures the soft objectives (pour gaps, lateness, empty km) on the finished plan;
//! [`crate::search`] improves their weighted sum.

use std::collections::{BTreeMap, HashMap, HashSet};

use chrono::{DateTime, NaiveDate, NaiveTime, TimeDelta, TimeZone, Utc};
use chrono_tz::Europe::Berlin;
use domain::{
    Load, Location, MixCode, Order, OrderId, OrderStatus, Plant, PlantId, PlantStatus, Seed,
    SiteId, TimeWindow, Truck, TruckStatus,
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
#[serde(default)] // plans stored before a field existed still read back
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
    /// Every wait at a site between one load finishing and the next arriving, summed.
    pub gap_minutes: f64,
    /// The part of each wait beyond the order's maximum gap, summed.
    pub minutes_over_gap_limit: f64,
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

/// Whether an order gets loads at all: not cancelled, and either already batching (it has frozen
/// loads) or open and starting inside the horizon.
pub fn plannable(
    order: &Order,
    batching: bool,
    now: DateTime<Utc>,
    horizon_end: DateTime<Utc>,
) -> bool {
    // ponytail: an order whose start passed over an hour ago without batching is left alone.
    order.status != OrderStatus::Cancelled
        && (batching
            || matches!(
                order.status,
                OrderStatus::Unconfirmed | OrderStatus::Pending | OrderStatus::Confirmed
            ) && order.requested_start >= now - TimeDelta::hours(1)
                && order.requested_start < horizon_end)
}

/// Plans every open order starting before `now + horizon` around the `frozen` loads, which stay
/// exactly as they are.
pub fn plan(
    world: &World,
    orders: &[Order],
    frozen: &[Load],
    now: DateTime<Utc>,
    horizon: TimeDelta,
    travel: &dyn TravelTimeProvider,
) -> Plan {
    let mut planner = Planner::new(world, orders, now, now + horizon, travel);
    planner.keep(frozen.iter().map(|l| Load {
        frozen: true,
        ..l.clone()
    }));
    planner.construct();
    planner.into_plan()
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
    /// Where the truck is when planning starts.
    start: Location,
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
            .map_or((self.start, now), |t| (t.site, t.discharge_end))
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

/// The rest of an order's placed loads, from `from` (an ETA) on, to plan again with the first one
/// on `forced`: indexes into the planner's online plants and trucks in service.
pub(crate) struct Tail {
    pub(crate) order: OrderId,
    pub(crate) from: DateTime<Utc>,
    pub(crate) forced: (usize, usize),
}

/// The plan being built: every load decided so far, and the bays and trucks they keep busy.
pub(crate) struct Planner<'a> {
    world: &'a World,
    travel: &'a dyn TravelTimeProvider,
    now: DateTime<Utc>,
    horizon_end: DateTime<Utc>,
    search_until: DateTime<Utc>,
    orders: HashMap<&'a OrderId, &'a Order>,
    /// Online plants only.
    plants: Vec<PlantState<'a>>,
    /// Trucks in service only.
    trucks: Vec<TruckState<'a>>,
    /// Frozen loads included.
    pub(crate) loads: Vec<Load>,
    pub(crate) unplanned: Vec<Unplanned>,
}

impl<'a> Planner<'a> {
    pub(crate) fn new(
        world: &'a World,
        orders: &'a [Order],
        now: DateTime<Utc>,
        horizon_end: DateTime<Utc>,
        travel: &'a dyn TravelTimeProvider,
    ) -> Planner<'a> {
        let homes: HashMap<_, _> = world.plants.iter().map(|p| (&p.id, p.location)).collect();
        Planner {
            world,
            travel,
            now,
            horizon_end,
            // ponytail: loads may land up to a day past the horizon; a long pour that starts near the end still fits.
            search_until: horizon_end + TimeDelta::days(1),
            orders: orders.iter().map(|o| (&o.id, o)).collect(),
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
                        start: truck
                            .last_position
                            .or_else(|| homes.get(&truck.home_plant).copied())?,
                        trips: Vec::new(),
                    })
                })
                .collect(),
            loads: Vec::new(),
            unplanned: Vec::new(),
        }
    }

    /// Adds loads that are already decided, such as frozen ones, and keeps their bays and trucks busy.
    pub(crate) fn keep(&mut self, loads: impl IntoIterator<Item = Load>) {
        for load in loads {
            self.fix(&load);
            self.loads.push(load);
        }
    }

    /// Works out again which bays and trucks are busy, after loads were taken out.
    pub(crate) fn rebuild(&mut self) {
        for plant in &mut self.plants {
            plant.bays.clear();
        }
        for truck in &mut self.trucks {
            truck.trips.clear();
        }
        let loads = std::mem::take(&mut self.loads);
        for load in &loads {
            self.fix(load);
        }
        self.loads = loads;
    }

    /// Plans every plannable order that has no placed load yet, by requested start.
    pub(crate) fn construct(&mut self) {
        let batching: HashSet<&OrderId> = self
            .loads
            .iter()
            .filter(|l| l.frozen)
            .map(|l| &l.order)
            .collect();
        let placed: HashSet<&OrderId> = self
            .loads
            .iter()
            .filter(|l| !l.frozen)
            .map(|l| &l.order)
            .collect();
        let mut open: Vec<&'a Order> = self
            .orders
            .values()
            .copied()
            .filter(|o| {
                !placed.contains(&o.id)
                    && plannable(o, batching.contains(&o.id), self.now, self.horizon_end)
            })
            .collect();
        open.sort_by(|a, b| (a.requested_start, &a.id.0).cmp(&(b.requested_start, &b.id.0)));
        for order in open {
            self.place(order);
        }
    }

    /// Gives the unplanned orders one more try.
    pub(crate) fn retry_unplanned(&mut self) {
        for unplanned in std::mem::take(&mut self.unplanned) {
            match self.orders.get(&unplanned.order).copied() {
                Some(order) => self.place(order),
                None => self.unplanned.push(unplanned),
            }
        }
    }

    /// Takes out each tail's loads, then plans them again with the first one on its forced plant
    /// and truck. False if an order can't be placed whole; the caller rolls back.
    pub(crate) fn retime(&mut self, tails: &[Tail]) -> bool {
        self.loads.retain(|l| {
            l.frozen
                || !tails
                    .iter()
                    .any(|t| t.order == l.order && l.eta_on_site >= t.from)
        });
        self.rebuild();
        for tail in tails {
            let Some(order) = self.orders.get(&tail.order).copied() else {
                return false;
            };
            let fixed = self.loads_of(&order.id);
            match self.plan_order(order, &fixed, Some(tail.forced)) {
                Ok(new) => self.loads.extend(new),
                Err(_) => return false,
            }
        }
        true
    }

    /// The (plant, truck) indexes a placed load uses, if both can still be used.
    pub(crate) fn indexes(&self, load: &Load) -> Option<(usize, usize)> {
        Some((
            self.plants
                .iter()
                .position(|p| p.plant.id == load.planned_plant)?,
            self.trucks
                .iter()
                .position(|t| t.truck.id == load.planned_truck)?,
        ))
    }

    /// How many plants are online and how many trucks are in service.
    pub(crate) fn sizes(&self) -> (usize, usize) {
        (self.plants.len(), self.trucks.len())
    }

    pub(crate) fn into_plan(mut self) -> Plan {
        self.loads
            .sort_by(|a, b| (&a.order.0, a.sequence).cmp(&(&b.order.0, b.sequence)));
        let metrics = self.measure();
        Plan {
            created_at: self.now,
            horizon_end: self.horizon_end,
            loads: self.loads,
            unplanned: self.unplanned,
            metrics,
        }
    }

    /// The soft objectives of SPEC §4, measured on the loads as they stand.
    pub(crate) fn measure(&self) -> Metrics {
        let mut m = Metrics {
            loads: self.loads.len(),
            unplanned_orders: self.unplanned.len(),
            empty_km: self.empty_km(),
            ..Metrics::default()
        };
        // Ordered maps keep float sums, and so costs, the same from run to run.
        let mut by_order: BTreeMap<&str, Vec<&Load>> = BTreeMap::new();
        for load in &self.loads {
            by_order.entry(&load.order.0).or_default().push(load);
        }
        for mut loads in by_order.into_values() {
            let Some(order) = self.orders.get(&loads[0].order) else {
                continue;
            };
            // Sequence numbers aren't arrival order once a plant batches loads out of turn.
            loads.sort_by_key(|l| l.eta_on_site);
            m.planned_orders += 1;
            m.lateness_minutes +=
                minutes((loads[0].eta_on_site - order.requested_start).max(TimeDelta::zero()));
            for pair in loads.windows(2) {
                let gap = minutes(pair[1].eta_on_site - discharge_end(pair[0], order)).max(0.0);
                let limit = f64::from(order.max_gap_minutes);
                m.gap_minutes += gap;
                m.minutes_over_gap_limit += (gap - limit).max(0.0);
                m.max_gap_minutes = m.max_gap_minutes.max(gap);
                if gap > limit {
                    m.gaps_over_limit += 1;
                }
            }
        }
        m
    }

    /// Kilometres trucks drive empty: from where they start to their first plant, then from each
    /// site to their next plant.
    fn empty_km(&self) -> f64 {
        let mut by_truck: BTreeMap<&str, Vec<&Load>> = BTreeMap::new();
        for load in &self.loads {
            by_truck
                .entry(&load.planned_truck.0)
                .or_default()
                .push(load);
        }
        let plant_at = |id: &PlantId| {
            self.world
                .plants
                .iter()
                .find(|p| &p.id == id)
                .map(|p| p.location)
        };
        let mut km = 0.0;
        for mut trips in by_truck.into_values() {
            trips.sort_by_key(|l| l.planned_batch_at);
            let Some(truck) = self
                .world
                .trucks
                .iter()
                .find(|t| t.id == trips[0].planned_truck)
            else {
                continue;
            };
            let Some(mut at) = truck.last_position.or_else(|| plant_at(&truck.home_plant)) else {
                continue;
            };
            for load in trips {
                let Some(plant) = plant_at(&load.planned_plant) else {
                    continue;
                };
                km += self.travel.distance_km(at, plant);
                at = self
                    .orders
                    .get(&load.order)
                    .and_then(|o| self.world.sites.get(&o.site))
                    .copied()
                    .unwrap_or(plant);
            }
        }
        km
    }

    fn loads_of(&self, order: &OrderId) -> Vec<Load> {
        self.loads
            .iter()
            .filter(|l| &l.order == order)
            .cloned()
            .collect()
    }

    /// Plans what the order's loads so far leave of its volume, or lists it as unplanned.
    fn place(&mut self, order: &'a Order) {
        let fixed = self.loads_of(&order.id);
        match self.plan_order(order, &fixed, None) {
            Ok(new) => self.loads.extend(new),
            Err(reason) => self.unplanned.push(Unplanned {
                order: order.id.clone(),
                reason,
            }),
        }
    }

    /// Keeps a bay and the truck busy for a decided load.
    fn fix(&mut self, load: &Load) {
        let world = self.world;
        let Some(plant) = world.plants.iter().find(|p| p.id == load.planned_plant) else {
            return;
        };
        let batch_at = load.planned_batch_at;
        if let Some(state) = self.plants.iter_mut().find(|s| s.plant.id == plant.id) {
            state
                .bays
                .push((batch_at, batch_at + plant_time(load.volume_m3, plant)));
        }
        // Without its order or site, a load's discharge can't be timed. The store never sends one.
        let Some(order) = self.orders.get(&load.order).copied() else {
            return;
        };
        let Some(&site) = world.sites.get(&order.site) else {
            return;
        };
        if let Some(truck) = self
            .trucks
            .iter_mut()
            .find(|t| t.truck.id == load.planned_truck)
        {
            truck.add(Trip {
                batch_at,
                plant: plant.location,
                site,
                discharge_end: discharge_end(load, order),
            });
        }
    }

    /// Plans what `fixed`, the loads the order already has, leave of its volume. With `forced`,
    /// the first new load goes to that (plant, truck) or nowhere.
    fn plan_order(
        &mut self,
        order: &Order,
        fixed: &[Load],
        mut forced: Option<(usize, usize)>,
    ) -> Result<Vec<Load>, String> {
        let frozen_m3: f64 = fixed.iter().map(|l| l.volume_m3).sum();
        if order.total_volume_m3 - frozen_m3 <= VOLUME_EPSILON {
            return Ok(Vec::new());
        }
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
        let mut loads = Vec::new();
        let mut remaining = order.total_volume_m3 - frozen_m3;
        let mut target = fixed
            .iter()
            .map(|l| discharge_end(l, order))
            .fold(order.requested_start, DateTime::max);
        let taken: HashSet<u32> = fixed.iter().map(|l| l.sequence).collect();
        let mut sequences = (1..).filter(|s| !taken.contains(s));
        while remaining > VOLUME_EPSILON {
            let mut best: Option<Candidate> = None;
            let mut too_far_everywhere = true;
            let only = forced.take();
            for p in 0..self.plants.len() {
                for t in 0..self.trucks.len() {
                    if only.is_some_and(|pair| pair != (p, t)) {
                        continue;
                    }
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
                sequence: sequences.next().unwrap_or_default(),
                volume_m3: c.volume,
                planned_plant: plant.plant.id.clone(),
                planned_truck: self.trucks[c.truck].truck.id.clone(),
                planned_batch_at: c.batch_at,
                eta_on_site: c.eta,
                discharge_window: TimeWindow {
                    start: c.eta,
                    end: c.batch_at + TimeDelta::minutes(i64::from(max_minutes)),
                },
                frozen: false,
            });
            remaining -= c.volume;
            target = c.discharge_end;
        }
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

#[cfg(test)]
mod tests {
    use domain::{Customer, Seed};
    use proptest::prelude::*;
    use rand::{RngExt, SeedableRng, rngs::StdRng};

    use super::*;
    use crate::testkit::*;
    use crate::travel::HaversineTravelTime;
    use crate::verify::violations;

    fn run(world: &World, orders: &[Order], now: DateTime<Utc>) -> Plan {
        run_frozen(world, orders, &[], now)
    }

    /// Plans around `frozen`, checks the hard constraints, and checks that no frozen load moved.
    fn run_frozen(world: &World, orders: &[Order], frozen: &[Load], now: DateTime<Utc>) -> Plan {
        let travel = HaversineTravelTime::default();
        let plan = plan(world, orders, frozen, now, TimeDelta::hours(48), &travel);
        let broken = violations(&plan, world, orders, frozen, &travel);
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

    fn sequences(plan: &Plan) -> Vec<(u32, bool)> {
        let mut seen: Vec<_> = plan.loads.iter().map(|l| (l.sequence, l.frozen)).collect();
        seen.sort_unstable();
        seen
    }

    #[test]
    fn a_pour_frozen_halfway_keeps_its_loads_and_plans_the_rest() {
        let w = world(
            vec![plant("p", WALLDORF, 2, PlantStatus::Online)],
            five_trucks(),
            &[("s", HEIDELBERG)],
        );
        let o = order("o", "s", 42.5, at(7, 0));
        let first = run(&w, std::slice::from_ref(&o), at(5, 0));
        let frozen = batched(&first.loads[..2]);

        let second = run_frozen(&w, std::slice::from_ref(&o), &frozen, at(6, 50));
        assert_eq!(&second.loads[..2], frozen.as_slice());
        assert_eq!(volumes(&second), [9.0, 9.0, 9.0, 9.0, 6.5]);
        assert_eq!(
            sequences(&second),
            [(1, true), (2, true), (3, false), (4, false), (5, false)]
        );
        assert!(
            second.loads[2].eta_on_site >= discharge_end(&frozen[1], &o),
            "the rest follows the frozen loads"
        );
    }

    #[test]
    fn a_volume_cut_below_what_is_batched_plans_nothing_more() {
        let w = world(
            vec![plant("p", WALLDORF, 2, PlantStatus::Online)],
            five_trucks(),
            &[("s", HEIDELBERG)],
        );
        let o = order("o", "s", 42.5, at(7, 0));
        let frozen = batched(&run(&w, std::slice::from_ref(&o), at(5, 0)).loads[..2]);
        let cut = Order {
            total_volume_m3: 10.0,
            ..o
        };
        let second = run_frozen(&w, &[cut], &frozen, at(6, 50));
        assert_eq!(second.loads, frozen, "18 m³ already batched covers 10 m³");
        assert!(second.unplanned.is_empty());
    }

    #[test]
    fn a_cancelled_order_keeps_only_its_frozen_loads() {
        let w = world(
            vec![plant("p", WALLDORF, 2, PlantStatus::Online)],
            five_trucks(),
            &[("s", HEIDELBERG)],
        );
        let o = order("o", "s", 42.5, at(7, 0));
        let frozen = batched(&run(&w, std::slice::from_ref(&o), at(5, 0)).loads[..1]);
        let cancelled = Order {
            status: OrderStatus::Cancelled,
            ..o
        };
        let second = run_frozen(&w, &[cancelled], &frozen, at(6, 50));
        assert_eq!(
            second.loads, frozen,
            "no returns: the batched load is delivered"
        );
        assert!(second.unplanned.is_empty());
    }

    #[test]
    fn a_frozen_load_stays_although_its_truck_breaks_down_and_its_plant_goes_offline() {
        let mut w = world(
            vec![
                plant("leimen", LEIMEN, 2, PlantStatus::Online),
                plant("walldorf", WALLDORF, 2, PlantStatus::Online),
            ],
            (1..=5)
                .map(|i| truck(&format!("t{i}"), "walldorf", 9.0, TruckStatus::Available))
                .collect(),
            &[("s", HEIDELBERG)],
        );
        let o = order("o", "s", 27.0, at(7, 0));
        let frozen = batched(&run(&w, std::slice::from_ref(&o), at(5, 0)).loads[..1]);
        let (broken, offline) = (&frozen[0].planned_truck, &frozen[0].planned_plant);
        for t in w.trucks.iter_mut().filter(|t| &t.id == broken) {
            t.status = TruckStatus::OutOfService;
        }
        for p in w.plants.iter_mut().filter(|p| &p.id == offline) {
            p.status = PlantStatus::Offline;
        }

        let second = run_frozen(&w, std::slice::from_ref(&o), &frozen, at(6, 40));
        assert_eq!(second.loads[0], frozen[0]);
        assert_eq!(second.loads.len(), 3);
        assert!(
            second.loads[1..]
                .iter()
                .all(|l| &l.planned_truck != broken && &l.planned_plant != offline)
        );

        let travel = HaversineTravelTime::default();
        let mut placed_on_broken = second.clone();
        placed_on_broken.loads[2].planned_truck = broken.clone();
        let found = violations(&placed_on_broken, &w, &[o], &frozen, &travel);
        assert!(
            found.iter().any(|v| v.contains("out of service")),
            "{found:#?}"
        );
    }

    #[test]
    fn an_order_in_progress_is_planned_even_after_its_start_passed() {
        let w = world(
            vec![plant("p", WALLDORF, 2, PlantStatus::Online)],
            five_trucks(),
            &[("s", HEIDELBERG)],
        );
        let o = order("o", "s", 42.5, at(7, 0));
        let frozen = batched(&run(&w, std::slice::from_ref(&o), at(5, 0)).loads[..2]);
        let second = run_frozen(&w, std::slice::from_ref(&o), &frozen, at(8, 30));
        assert_eq!(volumes(&second), [9.0, 9.0, 9.0, 9.0, 6.5]);
        assert!(second.unplanned.is_empty());
    }

    #[test]
    fn new_loads_take_the_sequence_numbers_frozen_ones_leave_free() {
        let w = world(
            vec![plant("p", WALLDORF, 2, PlantStatus::Online)],
            five_trucks(),
            &[("s", HEIDELBERG)],
        );
        let o = order("o", "s", 42.5, at(7, 0));
        let first = run(&w, std::slice::from_ref(&o), at(5, 0));
        // The plant batched load 3 and skipped load 2.
        let frozen = batched(&[first.loads[0].clone(), first.loads[2].clone()]);
        let second = run_frozen(&w, std::slice::from_ref(&o), &frozen, at(7, 5));
        assert_eq!(
            sequences(&second),
            [(1, true), (2, false), (3, true), (4, false), (5, false)]
        );
        let total: f64 = second.loads.iter().map(|l| l.volume_m3).sum();
        assert_eq!(total, 42.5);
    }

    #[test]
    fn trucks_start_from_their_last_reported_position() {
        let mut far = truck("t", "p", 9.0, TruckStatus::Available);
        far.last_position = Some(FRANKFURT);
        let w = world(
            vec![plant("p", WALLDORF, 2, PlantStatus::Online)],
            vec![far],
            &[("s", HEIDELBERG)],
        );
        let plan = run(&w, &[order("o", "s", 9.0, at(6, 0))], at(5, 0));
        let drive = HaversineTravelTime::default().travel(FRANKFURT, WALLDORF, at(5, 0));
        assert!(plan.loads[0].planned_batch_at >= at(5, 0) + drive);
        assert!(plan.metrics.lateness_minutes > 0.0);
        assert!(plan.metrics.empty_km > 80.0, "{}", plan.metrics.empty_km);
    }

    #[test]
    fn the_checker_catches_moved_dropped_and_made_up_frozen_loads() {
        let w = world(
            vec![plant("p", WALLDORF, 1, PlantStatus::Online)],
            five_trucks(),
            &[("s", HEIDELBERG)],
        );
        let o = order("o", "s", 42.5, at(7, 0));
        let frozen = batched(&run(&w, std::slice::from_ref(&o), at(5, 0)).loads[..2]);
        let second = run_frozen(&w, std::slice::from_ref(&o), &frozen, at(6, 50));
        let travel = HaversineTravelTime::default();
        let check = |plan: &Plan| violations(plan, &w, std::slice::from_ref(&o), &frozen, &travel);

        let mut moved = second.clone();
        moved.loads[0].planned_batch_at += TimeDelta::minutes(1);
        assert!(check(&moved).iter().any(|v| v.contains("frozen")), "moved");

        let mut dropped = second.clone();
        dropped.loads.remove(0);
        assert!(
            check(&dropped).iter().any(|v| v.contains("frozen")),
            "dropped"
        );

        let mut made_up = second.clone();
        made_up.loads[3].frozen = true;
        assert!(
            check(&made_up).iter().any(|v| v.contains("frozen")),
            "made up"
        );

        // A placed load batching alongside a frozen one in the only bay.
        let mut crowded = second.clone();
        crowded.loads[2].planned_batch_at = frozen[1].planned_batch_at;
        assert!(
            check(&crowded).iter().any(|v| v.contains("bays")),
            "crowded"
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(300))]

        /// Every SPEC §4 hard constraint, checked independently of the planner, on random worlds.
        #[test]
        fn plans_never_break_a_hard_constraint(seed in any::<u64>()) {
            let (world, orders, now) = random_world(seed);
            let travel = HaversineTravelTime::default();
            let plan = plan(&world, &orders, &[], now, TimeDelta::hours(48), &travel);
            let broken = violations(&plan, &world, &orders, &[], &travel);
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
            let plan = plan(&world, &orders, &[], now, TimeDelta::hours(48), &travel);
            let broken = violations(&plan, &world, &orders, &[], &travel);
            prop_assert!(broken.is_empty(), "seed {seed}: {broken:#?}");
        }

        /// Plan, let the plants batch everything up to a random moment, then let reality change:
        /// trucks break down, plants go offline, orders change volume or are cancelled. Planning
        /// again from that moment keeps every frozen load and every hard constraint.
        #[test]
        fn replanning_around_frozen_loads_never_breaks_a_hard_constraint(seed in any::<u64>()) {
            let (mut world, mut orders, now) = random_world(seed);
            let travel = HaversineTravelTime::default();
            let first = plan(&world, &orders, &[], now, TimeDelta::hours(48), &travel);
            let mut rng = StdRng::seed_from_u64(seed.wrapping_add(1));
            let later = now + TimeDelta::minutes(rng.random_range(0..720));
            let frozen = batched(
                &first.loads.iter().filter(|l| l.planned_batch_at < later).cloned().collect::<Vec<_>>(),
            );
            for t in &mut world.trucks {
                if rng.random_bool(0.2) {
                    t.status = TruckStatus::OutOfService;
                }
            }
            for p in &mut world.plants {
                if rng.random_bool(0.2) {
                    p.status = PlantStatus::Offline;
                }
            }
            for o in &mut orders {
                match rng.random_range(0..10) {
                    0 => o.status = OrderStatus::Cancelled,
                    1 | 2 => o.total_volume_m3 = (o.total_volume_m3 * rng.random_range(0.3..1.5) * 2.0).round().max(1.0) / 2.0,
                    _ => {}
                }
            }
            let second = plan(&world, &orders, &frozen, later, TimeDelta::hours(48), &travel);
            let broken = violations(&second, &world, &orders, &frozen, &travel);
            prop_assert!(broken.is_empty(), "seed {seed}: {broken:#?}");
        }
    }
}
