//! Local search, the second step of SPEC §4's approach: it improves a plan the construction
//! heuristic made, without breaking a hard constraint.
//!
//! The cost is a weighted sum of the soft objectives ([`Weights`]): minutes beyond an order's
//! maximum gap (cold joints), every gap minute, lateness, empty kilometres and, when
//! re-dispatching, loads that moved compared with the previous version (churn).
//!
//! Two moves, picked at random with a seeded RNG:
//! - **relocate**: put one load on another plant and truck, then re-time the rest of its order;
//! - **swap**: exchange the trucks of two loads of different orders that batch within an hour of
//!   each other, then re-time the rest of both orders.
//!
//! Re-timing reuses the construction heuristic's slot search, so every candidate meets the hard
//! constraints by construction. A move that can't place a whole order is dropped, and a move is
//! kept only if it lowers the cost (hill climbing). Frozen loads never move. The search stops when
//! its [`Budget`] runs out. Then orders that are still unplanned get one more try, because the
//! moves may have freed capacity.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use chrono::{DateTime, TimeDelta, Utc};
use domain::{Load, Order, OrderId};
use rand::{RngExt, SeedableRng, rngs::StdRng};

use crate::planner::{Metrics, Plan, Planner, Tail, World};
use crate::travel::TravelTimeProvider;

/// A move must lower the cost by more than this to count, so float noise can't cycle.
const IMPROVEMENT: f64 = 1e-6;

/// Weights of the soft objectives. One unit is worth one minute of a continuous pour's gap.
#[derive(Debug, Clone, PartialEq)]
pub struct Weights {
    /// Per minute beyond an order's maximum gap: a cold-joint risk, so the heaviest weight.
    pub cold_joint_minute: f64,
    /// Per minute of any gap between two loads at a site.
    pub gap_minute: f64,
    /// Per minute the first load arrives after the requested start.
    pub late_minute: f64,
    pub empty_km: f64,
    /// Per load whose truck, plant or batch time differs from the previous version.
    pub churn_load: f64,
}

impl Default for Weights {
    fn default() -> Weights {
        Weights {
            cold_joint_minute: 20.0,
            gap_minute: 1.0,
            late_minute: 2.0,
            empty_km: 0.5,
            churn_load: 10.0,
        }
    }
}

/// When local search stops: after `time` or after `moves` moves, whichever comes first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Budget {
    pub time: Duration,
    pub moves: usize,
}

impl Budget {
    /// A time budget; SPEC §4's default is 500 ms.
    pub fn time(time: Duration) -> Budget {
        Budget {
            time,
            moves: usize::MAX,
        }
    }

    /// A fixed number of moves, so results are reproducible.
    pub fn moves(moves: usize) -> Budget {
        Budget {
            time: Duration::MAX,
            moves,
        }
    }
}

/// How many loads in both versions have another truck, plant or batch time.
pub fn churn(loads: &[Load], previous: &[Load]) -> usize {
    let before: HashMap<(&OrderId, u32), &Load> = previous
        .iter()
        .map(|l| ((&l.order, l.sequence), l))
        .collect();
    loads
        .iter()
        .filter(|l| {
            before.get(&(&l.order, l.sequence)).is_some_and(|b| {
                (&b.planned_truck, &b.planned_plant, b.planned_batch_at)
                    != (&l.planned_truck, &l.planned_plant, l.planned_batch_at)
            })
        })
        .count()
}

/// The weighted cost of a plan. Lower is better.
pub fn cost(metrics: &Metrics, churn: usize, w: &Weights) -> f64 {
    w.cold_joint_minute * metrics.minutes_over_gap_limit
        + w.gap_minute * metrics.gap_minutes
        + w.late_minute * metrics.lateness_minutes
        + w.empty_km * metrics.empty_km
        + w.churn_load * churn as f64
}

/// Plans like [`crate::planner::plan`], then improves the plan within `budget`.
#[allow(clippy::too_many_arguments)]
pub fn plan_and_improve(
    world: &World,
    orders: &[Order],
    frozen: &[Load],
    now: DateTime<Utc>,
    horizon: TimeDelta,
    travel: &dyn TravelTimeProvider,
    weights: &Weights,
    budget: Budget,
    seed: u64,
) -> Plan {
    let mut planner = Planner::new(world, orders, now, now + horizon, travel);
    planner.keep(frozen.iter().map(|l| Load {
        frozen: true,
        ..l.clone()
    }));
    planner.construct();
    improve(&mut planner, &[], weights, budget, seed);
    planner.into_plan()
}

/// Improves the planner's loads within `budget`. `previous` holds the loads of the version being
/// re-dispatched, for churn; it is empty for a plan from scratch.
pub(crate) fn improve(
    planner: &mut Planner,
    previous: &[Load],
    weights: &Weights,
    budget: Budget,
    seed: u64,
) {
    let deadline = Instant::now().checked_add(budget.time);
    let mut rng = StdRng::seed_from_u64(seed);
    let score = |p: &Planner| cost(&p.measure(), churn(&p.loads, previous), weights);
    let mut current = score(planner);
    if planner.loads.iter().any(|l| !l.frozen) {
        for _ in 0..budget.moves {
            if deadline.is_some_and(|d| Instant::now() >= d) {
                break;
            }
            let Some(tails) = pick(planner, &mut rng) else {
                continue;
            };
            let saved = planner.loads.clone();
            if planner.retime(&tails) {
                let candidate = score(planner);
                if candidate < current - IMPROVEMENT {
                    current = candidate;
                    continue;
                }
            }
            planner.loads = saved;
            planner.rebuild();
        }
    }
    planner.retry_unplanned();
}

/// A random relocate or swap, as the tails to plan again. `None` if the pick is a no-op.
fn pick(planner: &Planner, rng: &mut StdRng) -> Option<Vec<Tail>> {
    let placed: Vec<&Load> = planner.loads.iter().filter(|l| !l.frozen).collect();
    let a = placed[rng.random_range(0..placed.len())];
    let (plant_a, truck_a) = planner.indexes(a)?;
    if rng.random_bool(0.5) {
        let (plants, trucks) = planner.sizes();
        let forced = (rng.random_range(0..plants), rng.random_range(0..trucks));
        if forced == (plant_a, truck_a) {
            return None;
        }
        return Some(vec![Tail {
            order: a.order.clone(),
            from: a.eta_on_site,
            forced,
        }]);
    }
    let partners: Vec<&&Load> = placed
        .iter()
        .filter(|b| {
            b.order != a.order
                && b.planned_truck != a.planned_truck
                && (b.planned_batch_at - a.planned_batch_at).abs() <= TimeDelta::hours(1)
        })
        .collect();
    if partners.is_empty() {
        return None;
    }
    let b = partners[rng.random_range(0..partners.len())];
    let (plant_b, truck_b) = planner.indexes(b)?;
    let mut tails = vec![
        Tail {
            order: a.order.clone(),
            from: a.eta_on_site,
            forced: (plant_a, truck_b),
        },
        Tail {
            order: b.order.clone(),
            from: b.eta_on_site,
            forced: (plant_b, truck_a),
        },
    ];
    tails.sort_by_key(|t| t.from);
    Some(tails)
}

#[cfg(test)]
mod tests {
    use domain::{OrderStatus, PlantStatus, TruckStatus};
    use proptest::prelude::*;

    use super::*;
    use crate::planner::plan;
    use crate::testkit::*;
    use crate::travel::HaversineTravelTime;
    use crate::verify::violations;

    const HORIZON: TimeDelta = TimeDelta::hours(48);

    fn plan_cost(plan: &Plan) -> f64 {
        cost(&plan.metrics, 0, &Weights::default())
    }

    #[test]
    fn local_search_closes_a_cold_joint_gap_the_greedy_plan_leaves() {
        // Greedy fills the first load with a full 9 m³ drum, so the fourth load waits for a truck
        // to come back. Starting with the small truck lets it come back in time.
        let w = world(
            vec![plant("p", WALLDORF, 2, PlantStatus::Online)],
            vec![
                truck("t1", "p", 9.0, TruckStatus::Available),
                truck("t2", "p", 9.0, TruckStatus::Available),
                truck("small", "p", 4.0, TruckStatus::Available),
            ],
            &[("s", HEIDELBERG)],
        );
        let orders = [order("o", "s", 27.0, at(7, 0))];
        let travel = HaversineTravelTime::default();
        let greedy = plan(&w, &orders, &[], at(5, 0), HORIZON, &travel);
        assert_eq!(greedy.metrics.gaps_over_limit, 1, "{:?}", greedy.metrics);

        let improved = plan_and_improve(
            &w,
            &orders,
            &[],
            at(5, 0),
            HORIZON,
            &travel,
            &Weights::default(),
            Budget::moves(200),
            7,
        );
        assert_eq!(
            violations(&improved, &w, &orders, &[], &travel),
            Vec::<String>::new()
        );
        assert_eq!(
            improved.metrics.gaps_over_limit, 0,
            "{:?}",
            improved.metrics
        );
        assert_eq!(improved.loads[0].planned_truck.0, "small");
        assert!(plan_cost(&improved) < plan_cost(&greedy));
    }

    #[test]
    fn a_fixed_seed_and_move_count_always_give_the_same_plan() {
        let (w, orders, now) = random_world(42);
        let travel = HaversineTravelTime::default();
        let run = || {
            plan_and_improve(
                &w,
                &orders,
                &[],
                now,
                HORIZON,
                &travel,
                &Weights::default(),
                Budget::moves(300),
                3,
            )
        };
        assert_eq!(run(), run());
    }

    #[test]
    fn churn_counts_loads_that_changed_truck_plant_or_time() {
        let (w, orders, now) = random_world(7);
        let travel = HaversineTravelTime::default();
        let before = plan(&w, &orders, &[], now, HORIZON, &travel).loads;
        assert_eq!(churn(&before, &before), 0);
        let mut after = before.clone();
        after[0].planned_batch_at += TimeDelta::minutes(5);
        after.pop(); // a removed load isn't churn
        assert_eq!(churn(&after, &before), 1);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        /// On random worlds with frozen loads and changed reality, local search keeps every hard
        /// constraint and every frozen load, never plans fewer orders, and never raises the cost
        /// unless it planned more orders.
        #[test]
        fn local_search_never_breaks_a_constraint_or_raises_the_cost(seed in any::<u64>()) {
            let (mut world, mut orders, now) = random_world(seed);
            let travel = HaversineTravelTime::default();
            let first = plan(&world, &orders, &[], now, HORIZON, &travel);
            let mut rng = StdRng::seed_from_u64(seed.wrapping_add(1));
            let later = now + TimeDelta::minutes(rng.random_range(0..360));
            let frozen = batched(
                &first.loads.iter().filter(|l| l.planned_batch_at < later).cloned().collect::<Vec<_>>(),
            );
            for t in &mut world.trucks {
                if rng.random_bool(0.1) {
                    t.status = TruckStatus::OutOfService;
                }
            }
            for o in &mut orders {
                if rng.random_bool(0.1) {
                    o.status = OrderStatus::Cancelled;
                }
            }
            let greedy = plan(&world, &orders, &frozen, later, HORIZON, &travel);
            let improved = plan_and_improve(
                &world, &orders, &frozen, later, HORIZON, &travel,
                &Weights::default(), Budget::moves(200), seed,
            );
            let broken = violations(&improved, &world, &orders, &frozen, &travel);
            prop_assert!(broken.is_empty(), "seed {seed}: {broken:#?}");
            prop_assert!(improved.unplanned.len() <= greedy.unplanned.len());
            if improved.unplanned.len() == greedy.unplanned.len() {
                prop_assert!(plan_cost(&improved) <= plan_cost(&greedy) + IMPROVEMENT,
                    "seed {seed}: {} > {}", plan_cost(&improved), plan_cost(&greedy));
            }
        }
    }
}
