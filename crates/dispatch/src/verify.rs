//! An independent check of a finished plan against SPEC §4's hard constraints.
//!
//! It recomputes every check from the plan, the master data and the orders, and never reads the
//! planner's bookkeeping. The property tests run it on random worlds, and the API tests on plans
//! built from live data. An empty list means the plan is valid.

use std::collections::HashMap;

use chrono::{DateTime, TimeDelta, Utc};
use chrono_tz::Europe::Berlin;
use domain::{Load, Order, OrderId, Plant, PlantId, PlantStatus, Truck, TruckId, TruckStatus};

use crate::planner::{Plan, World, discharge_end, plannable, plant_time};
use crate::travel::TravelTimeProvider;

const VOLUME_EPSILON: f64 = 1e-6;
const CLOCK_SLACK: TimeDelta = TimeDelta::seconds(1);

/// Every hard-constraint violation in `plan`, in words. Empty means valid.
pub fn violations(
    plan: &Plan,
    world: &World,
    orders: &[Order],
    travel: &dyn TravelTimeProvider,
) -> Vec<String> {
    let plants: HashMap<&PlantId, &Plant> = world.plants.iter().map(|p| (&p.id, p)).collect();
    let trucks: HashMap<&TruckId, &Truck> = world.trucks.iter().map(|t| (&t.id, t)).collect();
    let by_id: HashMap<&OrderId, &Order> = orders.iter().map(|o| (&o.id, o)).collect();
    let mut found = Vec::new();

    for load in &plan.loads {
        let name = format!("{} #{}", load.order.0, load.sequence);
        let (Some(order), Some(plant), Some(truck)) = (
            by_id.get(&load.order),
            plants.get(&load.planned_plant),
            trucks.get(&load.planned_truck),
        ) else {
            found.push(format!(
                "{name}: refers to an unknown order, plant or truck"
            ));
            continue;
        };
        if plant.status != PlantStatus::Online {
            found.push(format!("{name}: plant {} is offline", plant.id.0));
        }
        if truck.status == TruckStatus::OutOfService {
            found.push(format!("{name}: truck {} is out of service", truck.id.0));
        }
        if load.volume_m3 <= 0.0 || load.volume_m3 > truck.drum_capacity_m3 + VOLUME_EPSILON {
            found.push(format!(
                "{name}: {} m³ on a {} m³ drum",
                load.volume_m3, truck.drum_capacity_m3
            ));
        }
        let Some(&max_minutes) = world.mixes.get(&order.mix) else {
            found.push(format!("{name}: unknown mix {}", order.mix.0));
            continue;
        };
        let limit = load.planned_batch_at + TimeDelta::minutes(i64::from(max_minutes));
        if discharge_end(load, order) > limit + CLOCK_SLACK {
            found.push(format!(
                "{name}: discharge ends after the {max_minutes} min limit of {}",
                order.mix.0
            ));
        }
        if load.discharge_window.start != load.eta_on_site || load.discharge_window.end != limit {
            found.push(format!(
                "{name}: discharge window doesn't match the ETA and the mix limit"
            ));
        }
        let loaded = load.planned_batch_at + plant_time(load.volume_m3, plant);
        match world.sites.get(&order.site) {
            Some(&site) => {
                let expected = loaded + travel.travel(plant.location, site, loaded);
                if (load.eta_on_site - expected).abs() > CLOCK_SLACK {
                    found.push(format!(
                        "{name}: ETA {} doesn't follow from batching and travel ({expected})",
                        load.eta_on_site
                    ));
                }
            }
            None => found.push(format!("{name}: ship-to {} can't be located", order.site.0)),
        }
        if !within_opening_hours(plant, load.planned_batch_at, loaded) {
            found.push(format!(
                "{name}: batching outside {}'s opening hours",
                plant.id.0
            ));
        }
    }

    found.extend(trucks_one_load_at_a_time(plan, world, &by_id, travel));
    found.extend(bays_never_overbooked(plan, &plants));
    found.extend(orders_planned_whole_or_not_at_all(plan, orders));
    found
}

fn within_opening_hours(plant: &Plant, start: DateTime<Utc>, end: DateTime<Utc>) -> bool {
    let (start, end) = (start.with_timezone(&Berlin), end.with_timezone(&Berlin));
    let hours = plant.operating_hours;
    start.date_naive() == end.date_naive()
        && start.time() >= hours.open
        && end.time() <= hours.close
}

/// A truck starts at its home plant when the plan is made, and must reach each loading plant in
/// time after its previous discharge.
fn trucks_one_load_at_a_time(
    plan: &Plan,
    world: &World,
    orders: &HashMap<&OrderId, &Order>,
    travel: &dyn TravelTimeProvider,
) -> Vec<String> {
    let mut found = Vec::new();
    for truck in &world.trucks {
        let mut trips: Vec<&Load> = plan
            .loads
            .iter()
            .filter(|l| l.planned_truck == truck.id)
            .collect();
        trips.sort_by_key(|l| l.planned_batch_at);
        let Some(home) = world.plants.iter().find(|p| p.id == truck.home_plant) else {
            continue;
        };
        let (mut at, mut free) = (home.location, plan.created_at);
        for load in trips {
            let (Some(plant), Some(order)) = (
                world.plants.iter().find(|p| p.id == load.planned_plant),
                orders.get(&load.order),
            ) else {
                continue;
            };
            if free + travel.travel(at, plant.location, free) > load.planned_batch_at + CLOCK_SLACK
            {
                found.push(format!(
                    "{}: can't reach {} in time for {} #{}",
                    truck.id.0, plant.id.0, load.order.0, load.sequence
                ));
            }
            match world.sites.get(&order.site) {
                Some(&site) => (at, free) = (site, discharge_end(load, order)),
                None => break,
            }
        }
    }
    found
}

fn bays_never_overbooked(plan: &Plan, plants: &HashMap<&PlantId, &Plant>) -> Vec<String> {
    let mut found = Vec::new();
    for plant in plants.values() {
        let mut changes: Vec<(DateTime<Utc>, i32)> = Vec::new();
        for load in plan.loads.iter().filter(|l| l.planned_plant == plant.id) {
            changes.push((load.planned_batch_at, 1));
            changes.push((
                load.planned_batch_at + plant_time(load.volume_m3, plant),
                -1,
            ));
        }
        // A bay that frees at the moment another load starts counts as free, so ends sort first.
        changes.sort();
        let mut busy = 0;
        for (at, change) in changes {
            busy += change;
            if busy > plant.loading_bays as i32 {
                found.push(format!(
                    "{}: {busy} loads batching at {at}, but only {} bays",
                    plant.id.0, plant.loading_bays
                ));
            }
        }
    }
    found
}

fn orders_planned_whole_or_not_at_all(plan: &Plan, orders: &[Order]) -> Vec<String> {
    let mut found = Vec::new();
    for order in orders {
        let loads: Vec<&Load> = plan.loads.iter().filter(|l| l.order == order.id).collect();
        let unplanned = plan
            .unplanned
            .iter()
            .filter(|u| u.order == order.id)
            .count();
        let expected = plannable(order, plan.created_at, plan.horizon_end);
        match (expected, loads.is_empty(), unplanned) {
            (true, false, 0) => {
                let total: f64 = loads.iter().map(|l| l.volume_m3).sum();
                if (total - order.total_volume_m3).abs() > VOLUME_EPSILON {
                    found.push(format!(
                        "{}: loads add up to {total} m³ of {}",
                        order.id.0, order.total_volume_m3
                    ));
                }
                let sequences: Vec<u32> = loads.iter().map(|l| l.sequence).collect();
                if sequences != (1..=loads.len() as u32).collect::<Vec<_>>() {
                    found.push(format!("{}: load sequence {sequences:?}", order.id.0));
                }
            }
            (true, true, 1) | (false, true, 0) => {}
            _ => found.push(format!(
                "{}: planned {} loads and listed unplanned {unplanned} times",
                order.id.0,
                loads.len()
            )),
        }
    }
    found
}
