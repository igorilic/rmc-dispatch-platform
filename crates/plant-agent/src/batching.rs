//! The batch computer: batches each of the plant's loads when its planned batch time comes.
//!
//! A load waits for a free loading bay, so the plant never batches more loads in parallel than
//! it has bays (a SPEC §4 hard constraint). Then it gets three tickets, with the same timings as
//! the simulated plants: `batching` when it starts, `loading` after volume ÷ batching rate, and
//! `loaded` 3 minutes later. Each ticket carries the time it was due, names the order and load
//! number, and goes into the outbox together with the load's new stage.
//!
//! Rules:
//! - A load the plant has started is never batched again, whatever a newer plan says about it.
//! - Frozen loads were batched by someone else, and are skipped.
//! - A load more than [`GRACE`] overdue is left to dispatch. That only happens after the agent
//!   itself was down: while the cloud link is down, the agent keeps batching on time.
//! - At startup, loads cut short by a restart finish first, with their tickets' original times.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use domain::{DeliveryStatus, LoadRef, Plant, PlantId, PlantLoad, PlantTicket};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::Agent;
use crate::store::{Started, StoreError};

/// Loading the truck, after batching.
const LOADING: TimeDelta = TimeDelta::minutes(3);
/// How late a load may still start.
// ponytail: fixed; make it a setting if plants want different tolerances.
pub const GRACE: TimeDelta = TimeDelta::minutes(15);
/// How often the plan is looked at again when nothing is due sooner.
const RECHECK: Duration = Duration::from_secs(60);

/// Batches until the plan's sender is dropped, or a load's tickets can't be stored.
pub async fn run(agent: Arc<Agent>) -> Result<(), StoreError> {
    let bays = Arc::new(Semaphore::new(agent.plant.loading_bays as usize));
    let next_seq = Arc::new(AtomicI64::new(agent.store.last_ticket_seq()? + 1));
    let mut tasks: JoinSet<Result<(), StoreError>> = JoinSet::new();
    let mut taken: HashSet<LoadRef> = HashSet::new();

    // Loads cut short by a restart take their bays before anything new starts.
    for started in agent.store.started()? {
        taken.insert(started.load.clone());
        if started.stage != DeliveryStatus::Loaded {
            info!(order = %started.load.order.0, sequence = started.load.sequence, stage = ?started.stage, "resuming a load");
            let bay = Arc::clone(&bays).acquire_owned().await;
            let Ok(bay) = bay else { return Ok(()) };
            tasks.spawn(finish(Arc::clone(&agent), bay, started));
        }
    }

    let mut plan = agent.plan.subscribe();
    loop {
        let now = agent.clock.now();
        let mut next_due: Option<DateTime<Utc>> = None;
        {
            let current = plan.borrow_and_update();
            let loads = current.as_ref().map_or(&[][..], |s| &s.plan.loads[..]);
            for planned in loads {
                let load = &planned.load;
                let key = LoadRef {
                    order: load.order.clone(),
                    sequence: load.sequence,
                };
                if load.frozen || taken.contains(&key) {
                    continue;
                }
                let due = load.planned_batch_at;
                if due > now {
                    next_due = Some(next_due.map_or(due, |d| d.min(due)));
                    continue;
                }
                taken.insert(key);
                if now - due > GRACE {
                    warn!(order = %load.order.0, sequence = load.sequence, %due, "load overdue, left to dispatch");
                    continue;
                }
                tasks.spawn(batch(
                    Arc::clone(&agent),
                    Arc::clone(&bays),
                    Arc::clone(&next_seq),
                    planned.clone(),
                ));
            }
        }
        let wait = next_due
            .and_then(|due| (due - now).to_std().ok())
            .map_or(RECHECK, |d| d.min(RECHECK));
        tokio::select! {
            changed = plan.changed() => if changed.is_err() { return Ok(()) },
            () = sleep(wait) => {}
            Some(done) = tasks.join_next() => match done {
                Ok(result) => result?,
                Err(join) if join.is_panic() => std::panic::resume_unwind(join.into_panic()),
                Err(_) => {}
            },
        }
    }
}

/// Waits for a bay, starts batching `planned`, and sees it through to `loaded`.
async fn batch(
    agent: Arc<Agent>,
    bays: Arc<Semaphore>,
    next_seq: Arc<AtomicI64>,
    planned: PlantLoad,
) -> Result<(), StoreError> {
    let Ok(bay) = bays.acquire_owned().await else {
        return Ok(());
    };
    let load = planned.load;
    let started = Started {
        load: LoadRef {
            order: load.order,
            sequence: load.sequence,
        },
        ticket_seq: next_seq.fetch_add(1, Ordering::Relaxed),
        truck: load.planned_truck,
        mix: planned.mix,
        volume_m3: load.volume_m3,
        started_at: agent.clock.now(),
        stage: DeliveryStatus::Batching,
    };
    issue(&agent, &started)?;
    finish(agent, bay, started).await
}

/// Issues the tickets after `started.stage`, each when it is due, then frees the bay.
async fn finish(
    agent: Arc<Agent>,
    _bay: OwnedSemaphorePermit,
    mut started: Started,
) -> Result<(), StoreError> {
    for stage in [DeliveryStatus::Loading, DeliveryStatus::Loaded] {
        if stage <= started.stage {
            continue;
        }
        let due = started.started_at + offset(&agent.plant, started.volume_m3, stage);
        sleep((due - agent.clock.now()).to_std().unwrap_or_default()).await;
        started.stage = stage;
        issue(&agent, &started)?;
    }
    Ok(())
}

/// When a stage's ticket is due, counted from the start of batching.
fn offset(plant: &Plant, volume_m3: f64, stage: DeliveryStatus) -> TimeDelta {
    let batching = TimeDelta::milliseconds(
        (volume_m3 / plant.batching_rate_m3_per_h * 3_600_000.0).round() as i64,
    );
    match stage {
        DeliveryStatus::Batching => TimeDelta::zero(),
        DeliveryStatus::Loading => batching,
        _ => batching + LOADING,
    }
}

/// `WAL-E000007` for Walldorf's seventh ticket: the plant's first three letters, as the
/// simulated plants number theirs, and E for the edge agent, so the two series never mix.
fn ticket_no(plant: &PlantId, seq: i64) -> String {
    let prefix: String = plant
        .0
        .trim_start_matches("plant-")
        .chars()
        .take(3)
        .collect();
    format!("{}-E{seq:06}", prefix.to_uppercase())
}

/// Puts the ticket for `started.stage` into the outbox, with the load's new stage.
fn issue(agent: &Agent, started: &Started) -> Result<(), StoreError> {
    let at = started.started_at + offset(&agent.plant, started.volume_m3, started.stage);
    let ticket = PlantTicket {
        plant: agent.plant.id.clone(),
        ticket_no: ticket_no(&agent.plant.id, started.ticket_seq),
        vehicle: started.truck.clone(),
        ts_ms: at.timestamp_millis(),
        event: started.stage,
        mix: started.mix.clone(),
        volume_m3: started.volume_m3,
        order: Some(started.load.order.clone()),
        load_no: Some(started.load.sequence),
    };
    let payload = serde_json::to_vec(&ticket)?;
    agent
        .store
        .record(started, &ticket.topic(), &payload, agent.clock.now())?;
    info!(ticket_no = %ticket.ticket_no, order = %started.load.order.0, sequence = started.load.sequence, event = ?started.stage, "batch ticket");
    agent.ticket_queued();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{Store, TempFile};
    use crate::{Clock, Synced};
    use chrono::TimeZone;
    use domain::{Load, MixCode, OrderId, PlantPlan, Seed, TimeWindow, TruckId};

    fn t0() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 11, 6, 0, 0).unwrap()
    }

    fn minutes(m: f64) -> DateTime<Utc> {
        t0() + TimeDelta::milliseconds((m * 60_000.0) as i64)
    }

    fn walldorf() -> Plant {
        Seed::rhine_neckar()
            .unwrap()
            .plant("plant-walldorf")
            .unwrap()
            .clone()
    }

    fn agent(file: &TempFile, clock: Clock) -> Arc<Agent> {
        Arc::new(Agent::new(walldorf(), Store::open(&file.0).unwrap(), clock).unwrap())
    }

    fn load(order: &str, batch_at: DateTime<Utc>) -> PlantLoad {
        PlantLoad {
            load: Load {
                order: OrderId(order.into()),
                sequence: 1,
                volume_m3: 9.0,
                planned_plant: walldorf().id,
                planned_truck: TruckId(format!("truck-{order}")),
                planned_batch_at: batch_at,
                eta_on_site: batch_at + TimeDelta::minutes(30),
                discharge_window: TimeWindow {
                    start: batch_at + TimeDelta::minutes(30),
                    end: batch_at + TimeDelta::minutes(90),
                },
                frozen: false,
            },
            mix: MixCode("C30/37-XC4".into()),
        }
    }

    fn plan(version: i64, loads: Vec<PlantLoad>) -> Option<Synced> {
        Some(Synced {
            plan: PlantPlan {
                plant: walldorf().id,
                version: Some(version),
                created_at: Some(t0()),
                loads,
            },
            at: t0(),
        })
    }

    /// Every ticket in the outbox, oldest first. Empties it, as the forwarder would.
    fn drain(agent: &Agent) -> Vec<PlantTicket> {
        let mut tickets = Vec::new();
        while let Some(q) = agent.store.oldest().unwrap() {
            tickets.push(serde_json::from_slice(&q.payload).unwrap());
            agent.store.remove(q.id).unwrap();
        }
        tickets
    }

    fn of(tickets: &[PlantTicket], order: &str) -> Vec<(DeliveryStatus, DateTime<Utc>)> {
        tickets
            .iter()
            .filter(|t| t.order.as_ref().is_some_and(|o| o.0 == order))
            .map(|t| (t.event, DateTime::from_timestamp_millis(t.ts_ms).unwrap()))
            .collect()
    }

    fn lifecycle(start: f64) -> Vec<(DeliveryStatus, DateTime<Utc>)> {
        // 9 m³ at Walldorf's 120 m³/h: 4.5 minutes of batching, then 3 of loading.
        vec![
            (DeliveryStatus::Batching, minutes(start)),
            (DeliveryStatus::Loading, minutes(start + 4.5)),
            (DeliveryStatus::Loaded, minutes(start + 7.5)),
        ]
    }

    #[tokio::test(start_paused = true)]
    async fn loads_start_on_time_and_never_more_than_the_bays() {
        let file = TempFile::new("bays");
        let agent = agent(&file, Clock::start(t0()));
        agent.plan.send_replace(plan(
            1,
            vec![
                load("a", minutes(1.0)),
                load("b", minutes(1.0)),
                load("c", minutes(1.0)),
                load("d", minutes(20.0)),
            ],
        ));
        let computer = tokio::spawn(run(Arc::clone(&agent)));
        sleep(Duration::from_secs(40 * 60)).await;
        let tickets = drain(&agent);

        assert_eq!(of(&tickets, "a"), lifecycle(1.0));
        assert_eq!(of(&tickets, "b"), lifecycle(1.0));
        assert_eq!(
            of(&tickets, "c"),
            lifecycle(8.5),
            "waits for a bay, Walldorf has 2"
        );
        assert_eq!(of(&tickets, "d"), lifecycle(20.0));

        let mut active = 0;
        for t in &tickets {
            match t.event {
                DeliveryStatus::Batching => active += 1,
                DeliveryStatus::Loaded => active -= 1,
                _ => {}
            }
            assert!(active <= 2, "{active} loads at once");
        }
        let numbers: Vec<&str> = tickets
            .iter()
            .filter(|t| t.event == DeliveryStatus::Batching)
            .map(|t| t.ticket_no.as_str())
            .collect();
        assert_eq!(
            numbers,
            ["WAL-E000001", "WAL-E000002", "WAL-E000003", "WAL-E000004"]
        );
        assert!(tickets.iter().all(|t| t.load_no == Some(1)
            && t.plant == walldorf().id
            && t.vehicle.0 == format!("truck-{}", t.order.as_ref().unwrap().0)));
        computer.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn a_load_is_never_batched_twice_when_a_newer_plan_moves_it() {
        let file = TempFile::new("moved");
        let agent = agent(&file, Clock::start(t0()));
        agent
            .plan
            .send_replace(plan(1, vec![load("a", minutes(1.0))]));
        let computer = tokio::spawn(run(Arc::clone(&agent)));
        sleep(Duration::from_secs(2 * 60)).await;
        // Dispatch hadn't heard of the batching yet, and moved the load to later.
        agent
            .plan
            .send_replace(plan(2, vec![load("a", minutes(30.0))]));
        sleep(Duration::from_secs(60 * 60)).await;
        assert_eq!(of(&drain(&agent), "a"), lifecycle(1.0));
        computer.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn a_restart_resumes_a_load_where_it_stopped() {
        let file = TempFile::new("resume");
        let first = agent(&file, Clock::start(t0()));
        first
            .plan
            .send_replace(plan(1, vec![load("a", minutes(1.0))]));
        let computer = tokio::spawn(run(Arc::clone(&first)));
        sleep(Duration::from_secs(2 * 60)).await;
        computer.abort();
        let _ = computer.await;
        let batching: Vec<PlantTicket> = drain(&first);
        assert_eq!(of(&batching, "a"), lifecycle(1.0)[..1]);

        // The new agent has no plan at all; it finishes the load from the file.
        let second = agent(&file, Clock::start(first.clock.now()));
        let computer = tokio::spawn(run(Arc::clone(&second)));
        sleep(Duration::from_secs(20 * 60)).await;
        assert_eq!(of(&drain(&second), "a"), lifecycle(1.0)[1..]);
        computer.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn frozen_dropped_and_long_overdue_loads_are_skipped() {
        let file = TempFile::new("skipped");
        let agent = agent(&file, Clock::start(t0()));
        let frozen = PlantLoad {
            load: Load {
                frozen: true,
                ..load("frozen", minutes(1.0)).load
            },
            ..load("frozen", minutes(1.0))
        };
        agent.plan.send_replace(plan(
            1,
            vec![
                frozen,
                load("overdue", minutes(-20.0)),
                load("late", minutes(-10.0)),
                load("dropped", minutes(10.0)),
            ],
        ));
        let computer = tokio::spawn(run(Arc::clone(&agent)));
        sleep(Duration::from_secs(5 * 60)).await;
        agent.plan.send_replace(plan(2, Vec::new()));
        sleep(Duration::from_secs(30 * 60)).await;

        let tickets = drain(&agent);
        assert_eq!(of(&tickets, "frozen"), []);
        assert_eq!(of(&tickets, "overdue"), [], "20 minutes is past the grace");
        assert_eq!(of(&tickets, "dropped"), []);
        assert_eq!(
            of(&tickets, "late"),
            lifecycle(0.0),
            "10 minutes late still starts, at once"
        );
        computer.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn without_the_cloud_it_batches_from_the_stored_plan() {
        let file = TempFile::new("stored");
        let synced = Synced {
            at: t0() - TimeDelta::hours(1),
            ..plan(7, vec![load("a", minutes(1.0))]).unwrap()
        };
        Store::open(&file.0).unwrap().save_plan(&synced).unwrap();

        let agent = agent(&file, Clock::start(t0()));
        assert!(agent.ready(), "a stored plan is enough to work");
        let computer = tokio::spawn(run(Arc::clone(&agent)));
        sleep(Duration::from_secs(6 * 60)).await;

        let status = agent.status().unwrap();
        assert_eq!(status.plan_version, Some(7));
        assert_eq!(
            status.plan_age_seconds,
            Some(66 * 60),
            "and it is 66 minutes stale"
        );
        assert!(!status.dispatch && !status.mqtt);
        assert_eq!(
            (status.outbox, status.queued),
            (2, 2),
            "batching and loading"
        );
        assert_eq!(
            status.sync_lag_seconds,
            5 * 60,
            "batching has waited since 06:01"
        );
        assert_eq!(of(&drain(&agent), "a"), lifecycle(1.0)[..2]);
        computer.abort();
    }
}
