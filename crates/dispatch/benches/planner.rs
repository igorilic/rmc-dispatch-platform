//! Planner benchmarks at 50, 200 and 1,000 loads (SPEC §4). Run them with `make bench`.
//!
//! The world is the seeded Rhine-Neckar one, with the fleet scaled to one truck per 8 loads so
//! the plans stay feasible, and seeded random orders over the next 48 hours.
//! - `construct`: the greedy plan from scratch;
//! - `improve`: the greedy plan plus 100 local search moves;
//! - `repair`: the busiest truck breaks down on an improved plan, then a full re-dispatch with the
//!   service's 500 ms local search budget.

// Benchmarks are test code: a broken setup should stop them loudly.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::hint::black_box;

use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use chrono_tz::Europe::Berlin;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dispatch::planner::{World, plan};
use dispatch::redispatch::{Request, Settings, Trigger, redispatch};
use dispatch::search::{Budget, Weights, plan_and_improve};
use dispatch::travel::HaversineTravelTime;
use domain::{Order, OrderId, OrderStatus, Seed, Truck, TruckId, TruckStatus};
use rand::{RngExt, SeedableRng, rngs::StdRng};

const HORIZON: TimeDelta = TimeDelta::hours(48);
const SIZES: [usize; 3] = [50, 200, 1000];

fn now() -> DateTime<Utc> {
    Berlin
        .with_ymd_and_hms(2026, 11, 10, 4, 0, 0)
        .unwrap()
        .with_timezone(&Utc)
}

/// The seeded world with `loads / 8` trucks, and orders adding up to about `loads` loads.
fn scenario(loads: usize) -> (World, Vec<Order>) {
    let seed = Seed::rhine_neckar().unwrap();
    let mut world = World::from_seed(&seed);
    world.trucks = (0..(loads / 8).max(3))
        .map(|i| Truck {
            id: TruckId(format!("truck-{i:04}")),
            status: TruckStatus::Available,
            ..seed.trucks[i % seed.trucks.len()].clone()
        })
        .collect();
    let mut rng = StdRng::seed_from_u64(loads as u64);
    let mut orders = Vec::new();
    let mut expected = 0.0;
    while expected < loads as f64 {
        let site = &seed.sites[rng.random_range(0..seed.sites.len())];
        let volume = f64::from(rng.random_range(16..=90)) / 2.0;
        let day = rng.random_range(0..2);
        let start = now() + TimeDelta::days(day) + TimeDelta::minutes(15 * rng.random_range(8..48));
        orders.push(Order {
            id: OrderId(format!("{}/BENCH-{}", site.customer.0, orders.len())),
            customer: site.customer.clone(),
            site: site.id.clone(),
            mix: seed.mixes[rng.random_range(0..seed.mixes.len())]
                .code
                .clone(),
            total_volume_m3: volume,
            requested_start: start,
            pour_rate_m3_per_h: f64::from(rng.random_range(20..=60)),
            max_gap_minutes: rng.random_range(15..=30),
            status: OrderStatus::Confirmed,
        });
        expected += (volume / 9.0).ceil();
    }
    (world, orders)
}

fn benches(c: &mut Criterion) {
    let travel = HaversineTravelTime::default();
    for loads in SIZES {
        let (world, orders) = scenario(loads);
        let greedy = plan(&world, &orders, &[], now(), HORIZON, &travel);
        eprintln!(
            "{loads}: {} trucks, {} orders, {} loads planned, {} orders unplanned",
            world.trucks.len(),
            orders.len(),
            greedy.loads.len(),
            greedy.unplanned.len()
        );

        let mut group = c.benchmark_group("construct");
        group.sample_size(10);
        group.bench_function(BenchmarkId::from_parameter(loads), |b| {
            b.iter(|| plan(&world, black_box(&orders), &[], now(), HORIZON, &travel))
        });
        group.finish();

        let improve = |moves| {
            plan_and_improve(
                &world,
                &orders,
                &[],
                now(),
                HORIZON,
                &travel,
                &Weights::default(),
                Budget::moves(moves),
                1,
            )
        };
        let mut group = c.benchmark_group("improve");
        group.sample_size(10);
        group.bench_function(BenchmarkId::from_parameter(loads), |b| {
            b.iter(|| improve(black_box(100)))
        });
        group.finish();

        let previous = improve(100);
        let busiest = world
            .trucks
            .iter()
            .max_by_key(|t| {
                previous
                    .loads
                    .iter()
                    .filter(|l| l.planned_truck == t.id)
                    .count()
            })
            .map(|t| t.id.clone())
            .unwrap();
        let mut broken = world.clone();
        for t in broken.trucks.iter_mut().filter(|t| t.id == busiest) {
            t.status = TruckStatus::OutOfService;
        }
        let triggers = [Trigger::TruckOutOfService { truck: busiest }];
        let req = Request {
            world: &broken,
            orders: &orders,
            frozen: &[],
            previous: Some(&previous),
            triggers: &triggers,
            now: now() + TimeDelta::minutes(5),
        };
        let mut group = c.benchmark_group("repair");
        group.sample_size(10);
        group.bench_function(BenchmarkId::from_parameter(loads), |b| {
            b.iter(|| redispatch(black_box(&req), &travel, &Settings::default(), 1).unwrap())
        });
        group.finish();
    }
}

criterion_group!(planner, benches);
criterion_main!(planner);
