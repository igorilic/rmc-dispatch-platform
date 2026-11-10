//! Re-dispatch, the third step of SPEC §4's approach: when reality changes, repair the plan
//! instead of making a new one, and say what changed and why.
//!
//! A re-dispatch takes the previous version and the [`Trigger`]s that arrived since:
//! 1. **Freeze.** Loads with a delivery stay exactly as they are.
//! 2. **Keep.** Loads of orders no trigger touched stay where they were, if they still fit every
//!    hard constraint at the same plant, truck and time. A load that no longer fits, for example
//!    because its batch time has passed, sends its whole order to the next step.
//! 3. **Construct.** The heuristic plans the touched orders, new ones and unplanned ones around
//!    the frozen and kept loads.
//! 4. **Improve.** Local search runs over every load that isn't frozen. Its churn term counts
//!    loads that differ from the previous version, so it doesn't reshuffle the fleet for nothing.
//! 5. **Verify.** [`crate::verify`] checks the result. If it fails, a full re-plan (every load
//!    that isn't frozen) is tried instead. If that fails too, there is no new plan.
//!
//! [`diff`] then compares the versions load by load, and gives every change a reason: the trigger
//! that touched it, batching having started, batching not having started on time, or an earlier
//! load of the same order that moved.

use std::collections::{HashMap, HashSet};
use std::fmt;

use chrono::{DateTime, TimeDelta, Utc};
use domain::{Load, Order, OrderId, OrderStatus, PlantId, SiteId, TruckId};
use serde::{Deserialize, Serialize};
use tracing::warn;
use utoipa::ToSchema;

use crate::planner::{Metrics, Plan, Planner, Unplanned, World, plannable};
use crate::search::{Budget, Weights, improve};
use crate::travel::TravelTimeProvider;
use crate::verify::violations;

/// Something that changed and calls for a re-dispatch (SPEC §4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Trigger {
    NewOrder {
        order: OrderId,
    },
    /// SPEC's `OrderVolumeChanged`, and any other change that matters for planning.
    OrderChanged {
        order: OrderId,
        /// In words, such as `volume 40 → 49.5 m³`.
        change: String,
    },
    OrderCancelled {
        order: OrderId,
    },
    TruckOutOfService {
        truck: TruckId,
    },
    PlantOffline {
        plant: PlantId,
    },
    PlantOnline {
        plant: PlantId,
    },
    /// `orders` at the site now start `minutes` later; the orders passed in already say so.
    SiteDelayed {
        site: SiteId,
        minutes: u32,
        /// The orders the delay applies to: the site's pours of the day, not tomorrow's.
        orders: Vec<OrderId>,
    },
    /// Plan everything that isn't frozen again, as `POST /plans` asks.
    FullReplan,
}

impl Trigger {
    /// Whether this trigger concerns `load`.
    fn touches(&self, load: &Load) -> bool {
        match self {
            Trigger::NewOrder { order: id }
            | Trigger::OrderChanged { order: id, .. }
            | Trigger::OrderCancelled { order: id } => &load.order == id,
            Trigger::TruckOutOfService { truck } => &load.planned_truck == truck,
            Trigger::PlantOffline { plant } | Trigger::PlantOnline { plant } => {
                &load.planned_plant == plant
            }
            Trigger::SiteDelayed { orders, .. } => orders.contains(&load.order),
            Trigger::FullReplan => true,
        }
    }

    /// The reason for a change this trigger caused.
    fn reason(&self) -> String {
        match self {
            Trigger::NewOrder { .. } => "new order".into(),
            Trigger::OrderChanged { change, .. } => format!("order changed: {change}"),
            Trigger::OrderCancelled { .. } => "order cancelled".into(),
            Trigger::TruckOutOfService { truck } => format!("{} is out of service", truck.0),
            Trigger::PlantOffline { plant } => format!("{} is offline", plant.0),
            Trigger::PlantOnline { plant } => format!("{} is back online", plant.0),
            Trigger::SiteDelayed { site, minutes, .. } => {
                format!("site {} is delayed {minutes} min", site.0)
            }
            Trigger::FullReplan => "full re-plan requested".into(),
        }
    }
}

impl fmt::Display for Trigger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Trigger::NewOrder { order } => write!(f, "new order {}", order.0),
            Trigger::OrderChanged { order, change } => {
                write!(f, "order {} changed: {change}", order.0)
            }
            Trigger::OrderCancelled { order } => write!(f, "order {} cancelled", order.0),
            Trigger::TruckOutOfService { truck } => write!(f, "{} out of service", truck.0),
            Trigger::PlantOffline { plant } => write!(f, "{} offline", plant.0),
            Trigger::PlantOnline { plant } => write!(f, "{} back online", plant.0),
            Trigger::SiteDelayed { site, minutes, .. } => {
                write!(f, "site {} delayed {minutes} min", site.0)
            }
            Trigger::FullReplan => write!(f, "full re-plan"),
        }
    }
}

/// How plans are searched for.
#[derive(Debug, Clone, PartialEq)]
pub struct Settings {
    /// How far ahead orders are planned.
    pub horizon: TimeDelta,
    pub weights: Weights,
    pub budget: Budget,
}

impl Default for Settings {
    fn default() -> Settings {
        Settings {
            horizon: TimeDelta::hours(48),
            weights: Weights::default(),
            budget: Budget::time(std::time::Duration::from_millis(500)),
        }
    }
}

/// What a re-dispatch starts from.
#[derive(Debug, Clone, Copy)]
pub struct Request<'a> {
    pub world: &'a World,
    pub orders: &'a [Order],
    /// Loads with a delivery.
    pub frozen: &'a [Load],
    /// The version being repaired. `None` before the first plan.
    pub previous: Option<&'a Plan>,
    pub triggers: &'a [Trigger],
    pub now: DateTime<Utc>,
}

/// A new plan and how it differs from the previous one.
#[derive(Debug, Clone, PartialEq)]
pub struct Outcome {
    pub plan: Plan,
    pub diff: Diff,
}

/// Repairs the previous plan after `triggers`, or plans from scratch when there is none or a full
/// re-plan is asked for. `Err` holds the hard-constraint violations when no valid plan was found;
/// the caller keeps the previous version.
pub fn redispatch(
    req: &Request,
    travel: &dyn TravelTimeProvider,
    settings: &Settings,
    seed: u64,
) -> Result<Outcome, Vec<String>> {
    let full = req.previous.is_none() || req.triggers.contains(&Trigger::FullReplan);
    let valid = |plan: Plan| {
        let found = violations(&plan, req.world, req.orders, req.frozen, travel);
        if found.is_empty() {
            Ok(plan)
        } else {
            Err(found)
        }
    };
    let (plan, displaced) = attempt(req, travel, settings, seed, full);
    let (plan, replanned) = match valid(plan) {
        Ok(plan) => (
            plan,
            because(
                displaced,
                "its old slot no longer fits (plant, truck or loading bay)",
            ),
        ),
        Err(found) if !full => {
            warn!(violations = ?found, "repaired plan is invalid, planning everything again");
            let all = req.orders.iter().map(|o| o.id.clone()).collect();
            (
                valid(attempt(req, travel, settings, seed, true).0)?,
                because(
                    all,
                    "planned again from scratch: the repair broke a hard constraint",
                ),
            )
        }
        Err(found) => return Err(found),
    };
    let diff = diff(req.previous, &plan, req.orders, req.triggers, &replanned);
    Ok(Outcome { plan, diff })
}

fn because(orders: HashSet<OrderId>, reason: &str) -> HashMap<OrderId, String> {
    orders.into_iter().map(|o| (o, reason.to_owned())).collect()
}

fn attempt(
    req: &Request,
    travel: &dyn TravelTimeProvider,
    settings: &Settings,
    seed: u64,
    full: bool,
) -> (Plan, HashSet<OrderId>) {
    let horizon_end = req.now + settings.horizon;
    let mut planner = Planner::new(req.world, req.orders, req.now, horizon_end, travel);
    planner.keep(req.frozen.iter().map(|l| Load {
        frozen: true,
        ..l.clone()
    }));
    let previous: &[Load] = req.previous.map_or(&[], |p| &p.loads);
    let mut displaced = HashSet::new();
    if !full {
        let touched = touched_orders(req);
        let by_id: HashMap<&OrderId, &Order> = req.orders.iter().map(|o| (&o.id, o)).collect();
        let frozen: HashSet<(&OrderId, u32)> =
            req.frozen.iter().map(|l| (&l.order, l.sequence)).collect();
        let batching: HashSet<&OrderId> = req.frozen.iter().map(|l| &l.order).collect();
        let still_planned = |id: &OrderId| {
            by_id
                .get(id)
                .is_some_and(|o| plannable(o, batching.contains(id), req.now, horizon_end))
        };
        let kept = previous
            .iter()
            .filter(|l| {
                !l.frozen
                    && !frozen.contains(&(&l.order, l.sequence))
                    && !touched.contains(&l.order)
                    && still_planned(&l.order)
            })
            .cloned()
            .collect();
        displaced = planner.keep_fitting(kept);
    }
    planner.construct();
    improve(
        &mut planner,
        previous,
        &settings.weights,
        settings.budget,
        seed,
    );
    (planner.into_plan(), displaced)
}

/// Orders whose loads a trigger touched, orders a trigger names, and orders with a load that has
/// started batching since the previous version.
fn touched_orders<'a>(req: &Request<'a>) -> HashSet<&'a OrderId> {
    let previous: &[Load] = req.previous.map_or(&[], |p| &p.loads);
    let was_frozen: HashSet<(&OrderId, u32)> = previous
        .iter()
        .filter(|l| l.frozen)
        .map(|l| (&l.order, l.sequence))
        .collect();
    let mut touched: HashSet<&OrderId> = previous
        .iter()
        .filter(|l| !l.frozen)
        .filter(|l| req.triggers.iter().any(|t| t.touches(l)))
        .map(|l| &l.order)
        .collect();
    for trigger in req.triggers {
        if let Trigger::NewOrder { order }
        | Trigger::OrderChanged { order, .. }
        | Trigger::OrderCancelled { order } = trigger
        {
            touched.insert(order);
        }
    }
    touched.extend(
        req.frozen
            .iter()
            .filter(|l| !was_frozen.contains(&(&l.order, l.sequence)))
            .map(|l| &l.order),
    );
    touched
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    Added,
    Removed,
    Moved,
}

/// One load that differs between two versions, identified by order and sequence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Change {
    pub order: OrderId,
    pub sequence: u32,
    pub kind: ChangeKind,
    pub before: Option<Load>,
    pub after: Option<Load>,
    pub reason: String,
}

/// What changed between two plan versions, and why.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Diff {
    /// What triggered the re-dispatch, in words.
    pub triggers: Vec<String>,
    /// By order, then sequence.
    pub changes: Vec<Change>,
    /// Orders unplanned now that weren't before, with the reason.
    pub newly_unplanned: Vec<Unplanned>,
    /// Orders planned now that were unplanned before.
    pub newly_planned: Vec<OrderId>,
    pub metrics_before: Metrics,
    pub metrics_after: Metrics,
}

impl Diff {
    /// Nothing changed that a reader would see; there's no point storing a new version.
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty() && self.newly_unplanned.is_empty() && self.newly_planned.is_empty()
    }
}

/// Compares `plan` with `previous` (nothing, before the first plan) load by load. `replanned`
/// gives the reason for orders the repair had to plan again although no trigger touched them.
pub fn diff(
    previous: Option<&Plan>,
    plan: &Plan,
    orders: &[Order],
    triggers: &[Trigger],
    replanned: &HashMap<OrderId, String>,
) -> Diff {
    let (before_loads, before_unplanned): (&[Load], &[Unplanned]) =
        previous.map_or((&[], &[]), |p| (&p.loads, &p.unplanned));
    let by_id: HashMap<&OrderId, &Order> = orders.iter().map(|o| (&o.id, o)).collect();
    let key = |l: &Load| (l.order.0.clone(), l.sequence);
    let before: HashMap<_, &Load> = before_loads.iter().map(|l| (key(l), l)).collect();
    let after: HashMap<_, &Load> = plan.loads.iter().map(|l| (key(l), l)).collect();
    let mut keys: Vec<_> = before.keys().chain(after.keys()).cloned().collect();
    keys.sort();
    keys.dedup();

    let now = plan.created_at;
    let mut changes: Vec<(Change, Option<String>)> = Vec::new();
    for k in keys {
        let (b, a) = (before.get(&k).copied(), after.get(&k).copied());
        let unfrozen = |l: &Load| Load {
            frozen: false,
            ..l.clone()
        };
        let kind = match (b, a) {
            (None, Some(_)) => ChangeKind::Added,
            (Some(_), None) => ChangeKind::Removed,
            (Some(b), Some(a)) if unfrozen(b) != unfrozen(a) => ChangeKind::Moved,
            _ => continue,
        };
        let order = by_id.get(&OrderId(k.0.clone())).copied();
        let direct = direct_reason(b, a, order, triggers, now, plan);
        changes.push((
            Change {
                order: OrderId(k.0),
                sequence: k.1,
                kind,
                before: b.cloned(),
                after: a.cloned(),
                reason: String::new(),
            },
            direct,
        ));
    }

    let summary: Vec<String> = triggers.iter().map(ToString::to_string).collect();
    // A load that started batching since moves nothing itself, but its order was planned again.
    let was_frozen: HashSet<(&OrderId, u32)> = before_loads
        .iter()
        .filter(|l| l.frozen)
        .map(|l| (&l.order, l.sequence))
        .collect();
    let mut started: HashMap<&OrderId, u32> = HashMap::new();
    for l in plan
        .loads
        .iter()
        .filter(|l| l.frozen && !was_frozen.contains(&(&l.order, l.sequence)))
    {
        let first = started.entry(&l.order).or_insert(l.sequence);
        *first = (*first).min(l.sequence);
    }
    let eta = |c: &Change| {
        c.after
            .as_ref()
            .or(c.before.as_ref())
            .map(|l| l.eta_on_site)
    };
    let hit = |c: &Change| {
        triggers
            .iter()
            .any(|t| c.before.iter().chain(&c.after).any(|l| t.touches(l)))
    };
    let mut done = Vec::with_capacity(changes.len());
    for (change, direct) in &changes {
        let reason = direct.clone().unwrap_or_else(|| {
            // Another load of the same order changed for a reason, so the order was planned
            // again. A later load on the same truck and plant only follows it in time. A load a
            // trigger hit explains more than one that merely started batching.
            let explained = || {
                changes
                    .iter()
                    .filter(|(c, d)| c.order == change.order && d.is_some())
            };
            let cause = explained()
                .filter(|(c, _)| hit(c))
                .min_by_key(|(c, _)| eta(c))
                .or_else(|| explained().min_by_key(|(c, _)| eta(c)));
            match (cause, &change.before, &change.after) {
                (Some((c, _)), Some(b), Some(a))
                    if eta(c) < eta(change)
                        && (&b.planned_truck, &b.planned_plant)
                            == (&a.planned_truck, &a.planned_plant) =>
                {
                    format!("follows #{}, so the pour stays continuous", c.sequence)
                }
                (Some((c, _)), _, _) => format!("re-planned along with #{}", c.sequence),
                (None, _, _) if started.contains_key(&change.order) => {
                    format!(
                        "re-planned after #{} started batching",
                        started[&change.order]
                    )
                }
                (None, _, _) if replanned.contains_key(&change.order) => {
                    replanned[&change.order].clone()
                }
                (None, None, Some(_)) => {
                    match before_unplanned.iter().find(|u| u.order == change.order) {
                        Some(u) => format!("planned now; before: {}", u.reason),
                        None => "new in the plan".into(),
                    }
                }
                (None, _, _) if summary.is_empty() => "moved by local search".into(),
                (None, _, _) => format!("moved by local search after: {}", summary.join("; ")),
            }
        });
        done.push(Change {
            reason,
            ..change.clone()
        });
    }

    let was_unplanned: HashSet<&OrderId> = before_unplanned.iter().map(|u| &u.order).collect();
    let is_unplanned: HashSet<&OrderId> = plan.unplanned.iter().map(|u| &u.order).collect();
    let is_planned: HashSet<&OrderId> = plan.loads.iter().map(|l| &l.order).collect();
    Diff {
        triggers: summary,
        changes: done,
        newly_unplanned: plan
            .unplanned
            .iter()
            .filter(|u| !was_unplanned.contains(&u.order))
            .cloned()
            .collect(),
        newly_planned: before_unplanned
            .iter()
            .filter(|u| !is_unplanned.contains(&u.order) && is_planned.contains(&u.order))
            .map(|u| u.order.clone())
            .collect(),
        metrics_before: previous.map(|p| p.metrics.clone()).unwrap_or_default(),
        metrics_after: plan.metrics.clone(),
    }
}

/// A reason that needs no other change to explain it.
fn direct_reason(
    before: Option<&Load>,
    after: Option<&Load>,
    order: Option<&Order>,
    triggers: &[Trigger],
    now: DateTime<Utc>,
    plan: &Plan,
) -> Option<String> {
    if let (Some(b), Some(a)) = (before, after)
        && a.frozen
        && !b.frozen
    {
        return Some("batching started".into());
    }
    if let Some(t) = triggers
        .iter()
        .find(|t| before.into_iter().chain(after).any(|l| t.touches(l)))
    {
        return Some(t.reason());
    }
    if before.is_some_and(|b| !b.frozen && b.planned_batch_at < now) {
        return Some("batching didn't start at the planned time".into());
    }
    if after.is_some() {
        return None;
    }
    let Some(o) = order else {
        return Some("the order is gone".into());
    };
    if o.status == OrderStatus::Cancelled {
        return Some("order cancelled".into());
    }
    let batching = plan.loads.iter().any(|l| l.order == o.id && l.frozen);
    if !plannable(o, batching, now, plan.horizon_end) {
        return Some(
            "no longer planned: its start passed over an hour ago without batching".into(),
        );
    }
    plan.unplanned
        .iter()
        .find(|u| u.order == o.id)
        .map(|u| format!("unplanned: {}", u.reason))
}

#[cfg(test)]
mod tests {
    use domain::{PlantStatus, TruckStatus};
    use proptest::prelude::*;
    use rand::{RngExt, SeedableRng, rngs::StdRng};

    use super::*;
    use crate::planner::plan;
    use crate::testkit::*;
    use crate::travel::HaversineTravelTime;

    const HORIZON: TimeDelta = TimeDelta::hours(48);

    /// Two plants, six trucks, and two orders at two sites: `a` at 07:00 and `b` at 09:00.
    fn scene() -> (World, Vec<Order>) {
        let w = world(
            vec![
                plant("walldorf", WALLDORF, 2, PlantStatus::Online),
                plant("leimen", LEIMEN, 2, PlantStatus::Online),
            ],
            (1..=6)
                .map(|i| truck(&format!("t{i}"), "walldorf", 9.0, TruckStatus::Available))
                .collect(),
            &[("s", HEIDELBERG), ("s2", SCHWETZINGEN)],
        );
        (
            w,
            vec![
                order("a", "s", 40.0, at(7, 0)),
                order("b", "s2", 18.0, at(9, 0)),
            ],
        )
    }

    fn first(w: &World, orders: &[Order]) -> Plan {
        plan(
            w,
            orders,
            &[],
            at(5, 0),
            HORIZON,
            &HaversineTravelTime::default(),
        )
    }

    fn settings(moves: usize) -> Settings {
        Settings {
            budget: Budget::moves(moves),
            ..Settings::default()
        }
    }

    /// Re-dispatches, and checks that the plan is valid and every change has a reason.
    fn run(
        w: &World,
        orders: &[Order],
        frozen: &[Load],
        previous: &Plan,
        triggers: &[Trigger],
        now: DateTime<Utc>,
        moves: usize,
    ) -> Outcome {
        let travel = HaversineTravelTime::default();
        let req = Request {
            world: w,
            orders,
            frozen,
            previous: Some(previous),
            triggers,
            now,
        };
        let out = redispatch(&req, &travel, &settings(moves), 1).unwrap();
        let found = violations(&out.plan, w, orders, frozen, &travel);
        assert!(found.is_empty(), "{found:#?}");
        assert!(out.diff.changes.iter().all(|c| !c.reason.is_empty()));
        out
    }

    fn loads_of<'a>(plan: &'a Plan, order: &str) -> Vec<&'a Load> {
        plan.loads.iter().filter(|l| l.order.0 == order).collect()
    }

    fn change<'a>(diff: &'a Diff, order: &str, sequence: u32) -> Option<&'a Change> {
        diff.changes
            .iter()
            .find(|c| c.order.0 == order && c.sequence == sequence)
    }

    #[test]
    fn a_truck_breakdown_moves_its_loads_and_keeps_the_batched_one() {
        let (mut w, orders) = scene();
        let v1 = first(&w, &orders);
        let a = loads_of(&v1, "a");
        // The plant started load 1 a minute late, so it is a change too ("batching started").
        let mut started = a[0].clone();
        started.planned_batch_at += TimeDelta::minutes(1);
        let frozen = batched(&[started]);
        let broken = a[1].planned_truck.clone();
        for t in w.trucks.iter_mut().filter(|t| t.id == broken) {
            t.status = TruckStatus::OutOfService;
        }
        let trigger = Trigger::TruckOutOfService {
            truck: broken.clone(),
        };
        let out = run(&w, &orders, &frozen, &v1, &[trigger], at(6, 40), 0);

        assert!(out.plan.loads.contains(&frozen[0]));
        assert!(
            out.plan
                .loads
                .iter()
                .all(|l| l.frozen || l.planned_truck != broken)
        );
        let moved = change(&out.diff, "a", 2).unwrap();
        assert_eq!(moved.reason, format!("{} is out of service", broken.0));
        assert_eq!(
            change(&out.diff, "a", 1).unwrap().reason,
            "batching started"
        );
        // The rest of the order moved because of the breakdown, not because load 1 started.
        for c in out
            .diff
            .changes
            .iter()
            .filter(|c| c.order.0 == "a" && c.sequence > 2)
        {
            assert!(c.reason.contains("#2"), "#{}: {}", c.sequence, c.reason);
        }
        assert_eq!(out.diff.triggers, [format!("{} out of service", broken.0)]);
        if !loads_of(&v1, "b").iter().any(|l| l.planned_truck == broken) {
            assert_eq!(loads_of(&out.plan, "b"), loads_of(&v1, "b"), "b untouched");
        }
    }

    #[test]
    fn a_plant_going_offline_moves_its_loads_elsewhere() {
        let (mut w, orders) = scene();
        let v1 = first(&w, &orders);
        let offline = loads_of(&v1, "a")[0].planned_plant.clone();
        for p in w.plants.iter_mut().filter(|p| p.id == offline) {
            p.status = PlantStatus::Offline;
        }
        let trigger = Trigger::PlantOffline {
            plant: offline.clone(),
        };
        let out = run(&w, &orders, &[], &v1, &[trigger], at(5, 30), 0);
        assert!(out.plan.loads.iter().all(|l| l.planned_plant != offline));
        assert!(
            out.diff
                .changes
                .iter()
                .any(|c| c.reason == format!("{} is offline", offline.0))
        );
    }

    #[test]
    fn a_site_delay_moves_its_pour_later_and_leaves_the_other_site_alone() {
        let (w, mut orders) = scene();
        let v1 = first(&w, &orders);
        orders[0].requested_start += TimeDelta::minutes(30); // the store passes the delayed start
        let trigger = Trigger::SiteDelayed {
            site: SiteId("s".into()),
            minutes: 30,
            orders: vec![OrderId("a".into())],
        };
        let out = run(&w, &orders, &[], &v1, &[trigger], at(5, 30), 0);
        assert!(loads_of(&out.plan, "a")[0].eta_on_site >= at(7, 30));
        assert_eq!(
            change(&out.diff, "a", 1).unwrap().reason,
            "site s is delayed 30 min"
        );
        assert_eq!(loads_of(&out.plan, "b"), loads_of(&v1, "b"));
    }

    #[test]
    fn a_new_order_is_planned_around_the_others() {
        let (w, mut orders) = scene();
        let v1 = first(&w, &orders);
        orders.push(order("c", "s2", 9.0, at(11, 0)));
        let trigger = Trigger::NewOrder {
            order: OrderId("c".into()),
        };
        let out = run(&w, &orders, &[], &v1, &[trigger], at(5, 30), 0);
        let added = change(&out.diff, "c", 1).unwrap();
        assert_eq!(
            (added.kind, added.reason.as_str()),
            (ChangeKind::Added, "new order")
        );
        assert_eq!(out.diff.changes.len(), 1, "a and b keep their loads");
    }

    #[test]
    fn a_volume_change_adds_a_load_and_says_why() {
        let (w, mut orders) = scene();
        let v1 = first(&w, &orders);
        orders[0].total_volume_m3 = 49.5;
        let trigger = Trigger::OrderChanged {
            order: OrderId("a".into()),
            change: "volume 40 → 49.5 m³".into(),
        };
        let out = run(&w, &orders, &[], &v1, &[trigger], at(5, 30), 0);
        assert_eq!(loads_of(&out.plan, "a").len(), loads_of(&v1, "a").len() + 1);
        assert!(out.diff.changes.iter().any(
            |c| c.kind == ChangeKind::Added && c.reason == "order changed: volume 40 → 49.5 m³"
        ));
    }

    #[test]
    fn a_cancellation_removes_what_isnt_batched() {
        let (w, mut orders) = scene();
        let v1 = first(&w, &orders);
        let frozen = batched(&[loads_of(&v1, "a")[0].clone()]);
        orders[0].status = OrderStatus::Cancelled;
        let trigger = Trigger::OrderCancelled {
            order: OrderId("a".into()),
        };
        let out = run(&w, &orders, &frozen, &v1, &[trigger], at(6, 40), 0);
        assert_eq!(loads_of(&out.plan, "a"), [&frozen[0]]);
        let removed: Vec<_> = out
            .diff
            .changes
            .iter()
            .filter(|c| c.kind == ChangeKind::Removed)
            .collect();
        assert_eq!(removed.len(), loads_of(&v1, "a").len() - 1);
        assert!(removed.iter().all(|c| c.reason == "order cancelled"));
    }

    #[test]
    fn loads_whose_batching_never_started_are_planned_again() {
        let (w, orders) = scene();
        let v1 = first(&w, &orders);
        let out = run(&w, &orders, &[], &v1, &[], at(6, 50), 0);
        let first_a = change(&out.diff, "a", 1).unwrap();
        assert_eq!(first_a.reason, "batching didn't start at the planned time");
        assert!(
            out.plan
                .loads
                .iter()
                .all(|l| l.planned_batch_at >= at(6, 50))
        );
    }

    #[test]
    fn a_plan_that_no_longer_holds_falls_back_to_a_full_replan() {
        let (w, mut orders) = scene();
        let v1 = first(&w, &orders);
        // The order moved to another site, and nobody said so.
        orders[1].site = SiteId("s".into());
        let out = run(&w, &orders, &[], &v1, &[], at(5, 30), 0);
        assert!(!out.diff.is_empty());
    }

    #[test]
    fn a_plant_back_online_plans_what_was_unplanned() {
        let mut w = world(
            vec![plant("p", WALLDORF, 2, PlantStatus::Online)],
            five_trucks(),
            &[("s", HEIDELBERG)],
        );
        let orders = [order("o", "s", 18.0, at(9, 0))];
        let v1 = first(&w, &orders);
        w.plants[0].status = PlantStatus::Offline;
        let off = Trigger::PlantOffline {
            plant: PlantId("p".into()),
        };
        let v2 = run(&w, &orders, &[], &v1, &[off], at(5, 30), 0);
        assert_eq!(v2.diff.newly_unplanned.len(), 1);
        assert_eq!(v2.diff.newly_unplanned[0].reason, "no plant is online");

        w.plants[0].status = PlantStatus::Online;
        let on = Trigger::PlantOnline {
            plant: PlantId("p".into()),
        };
        let v3 = run(&w, &orders, &[], &v2.plan, &[on], at(5, 40), 0);
        assert_eq!(v3.diff.newly_planned, [OrderId("o".into())]);
        assert!(
            v3.diff
                .changes
                .iter()
                .all(|c| c.reason == "p is back online")
        );
    }

    #[test]
    fn nothing_new_means_an_empty_diff() {
        let (w, orders) = scene();
        let v1 = first(&w, &orders);
        let out = run(&w, &orders, &[], &v1, &[], at(5, 0), 0);
        assert!(out.diff.is_empty(), "{:#?}", out.diff.changes);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(150))]

        /// A random trigger on a random plan: the repair is valid, frozen loads stay, and without
        /// local search every change is explained by something other than local search.
        #[test]
        fn repairs_stay_valid_and_local(seed in any::<u64>()) {
            let (mut world, mut orders, now) = random_world(seed);
            let travel = HaversineTravelTime::default();
            let v1 = plan(&world, &orders, &[], now, HORIZON, &travel);
            let mut rng = StdRng::seed_from_u64(seed.wrapping_add(2));
            let later = now + TimeDelta::minutes(rng.random_range(0..240));
            let frozen = batched(
                &v1.loads.iter().filter(|l| l.planned_batch_at < later).cloned().collect::<Vec<_>>(),
            );
            let trigger = match rng.random_range(0..5) {
                0 => {
                    let t = rng.random_range(0..world.trucks.len());
                    world.trucks[t].status = TruckStatus::OutOfService;
                    Trigger::TruckOutOfService { truck: world.trucks[t].id.clone() }
                }
                1 => {
                    let p = rng.random_range(0..world.plants.len());
                    world.plants[p].status = PlantStatus::Offline;
                    Trigger::PlantOffline { plant: world.plants[p].id.clone() }
                }
                2 => {
                    let o = rng.random_range(0..orders.len());
                    orders[o].status = OrderStatus::Cancelled;
                    Trigger::OrderCancelled { order: orders[o].id.clone() }
                }
                3 => {
                    let o = rng.random_range(0..orders.len());
                    orders[o].total_volume_m3 += 9.0;
                    Trigger::OrderChanged { order: orders[o].id.clone(), change: "volume +9 m³".into() }
                }
                _ => {
                    let o = rng.random_range(0..orders.len());
                    let site = orders[o].site.clone();
                    let mut delayed = Vec::new();
                    for order in orders.iter_mut().filter(|x| x.site == site) {
                        order.requested_start += TimeDelta::minutes(30);
                        delayed.push(order.id.clone());
                    }
                    Trigger::SiteDelayed { site, minutes: 30, orders: delayed }
                }
            };
            let triggers = [trigger];
            let req = Request {
                world: &world, orders: &orders, frozen: &frozen, previous: Some(&v1),
                triggers: &triggers, now: later,
            };
            let out = redispatch(&req, &travel, &settings(0), seed);
            prop_assert!(out.is_ok(), "seed {seed}: {:?}", out.err());
            let Ok(out) = out else { return Ok(()) };
            // Without local search, every change has a cause: the trigger, time passing, batching,
            // or an old slot that no longer fits. Nothing else moves.
            for c in &out.diff.changes {
                prop_assert!(!c.reason.is_empty() && !c.reason.starts_with("moved by local search"),
                    "seed {seed}: {} #{} {:?}: {}", c.order.0, c.sequence, c.kind, c.reason);
            }
        }
    }
}
