//! Fixtures shared by the planner, local search and re-dispatch tests.

use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use chrono_tz::Europe::Berlin;
use domain::{
    CustomerId, Load, Location, MixCode, OperatingHours, Order, OrderId, OrderStatus, Plant,
    PlantId, PlantStatus, SiteId, Truck, TruckId, TruckStatus,
};
use rand::{RngExt, SeedableRng, rngs::StdRng};

use crate::planner::World;

pub const WALLDORF: Location = Location {
    lat: 49.3064,
    lon: 8.6428,
};

pub const HEIDELBERG: Location = Location {
    lat: 49.3988,
    lon: 8.6724,
};

pub const LEIMEN: Location = Location {
    lat: 49.3481,
    lon: 8.6911,
};

pub const FRANKFURT: Location = Location {
    lat: 50.1109,
    lon: 8.6821,
};

pub fn at(h: u32, m: u32) -> DateTime<Utc> {
    Berlin
        .with_ymd_and_hms(2026, 11, 10, h, m, 0)
        .unwrap()
        .with_timezone(&Utc)
}

pub fn plant(id: &str, location: Location, bays: u32, status: PlantStatus) -> Plant {
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

pub fn truck(id: &str, home: &str, capacity: f64, status: TruckStatus) -> Truck {
    Truck {
        id: TruckId(id.into()),
        home_plant: PlantId(home.into()),
        drum_capacity_m3: capacity,
        status,
        last_position: None,
    }
}

pub fn order(id: &str, site: &str, volume: f64, start: DateTime<Utc>) -> Order {
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

pub fn world(plants: Vec<Plant>, trucks: Vec<Truck>, sites: &[(&str, Location)]) -> World {
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

/// The loads as the plant reports them once batching has started.
pub fn batched(loads: &[Load]) -> Vec<Load> {
    loads
        .iter()
        .map(|l| Load {
            frozen: true,
            ..l.clone()
        })
        .collect()
}

pub fn five_trucks() -> Vec<Truck> {
    (1..=5)
        .map(|i| truck(&format!("t{i}"), "p", 9.0, TruckStatus::Available))
        .collect()
}

pub fn place(rng: &mut StdRng, spread: f64) -> Location {
    Location {
        lat: 49.35 + rng.random_range(-spread..spread),
        lon: 8.65 + rng.random_range(-spread..spread),
    }
}

/// A random world: plants (some offline), trucks (some out of service), sites (some far away),
/// mixes with short and long limits, and orders around the clock.
pub fn random_world(seed: u64) -> (World, Vec<Order>, DateTime<Utc>) {
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
