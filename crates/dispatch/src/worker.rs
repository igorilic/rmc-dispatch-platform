//! The re-dispatch worker: every trigger, whatever its source, goes through here, one re-dispatch
//! at a time, so plan versions never race.
//!
//! The intakes and the API send [`Trigger`]s through [`Jobs`]. The worker takes the first job,
//! then every job that queued up meanwhile, and runs a single re-dispatch for all their triggers,
//! so a burst (an SFTP snapshot with twenty changed orders) becomes one version. It loads the
//! world, the orders, the deliveries (as frozen loads) and the latest version from the store,
//! repairs on a blocking thread because local search is CPU-bound, and stores the result as the
//! next version, unless nothing changed and no one asked for a full re-plan. The queue is bounded: a burst makes senders wait instead
//! of growing memory.
//!
//! Tracing (SPEC §8): each job carries the span it was sent from. The `redispatch` span is a child
//! of the first job's span, so it joins that trace; the other jobs of the batch become links.
//! Each run is timed, and each new version sets the plan gauges ([`crate::metrics`]) and is
//! announced on core NATS ([`PLANS_UPDATED`]) in the re-dispatch's trace, for the map console.

use std::time::{Duration, Instant};

use chrono::{DateTime, SubsecRound, TimeDelta, Utc};
use domain::{Load, PLANS_UPDATED, PlanUpdated};
use tokio::sync::{mpsc, oneshot};
use tracing::field::Empty;
use tracing::{Instrument, Span, error, info, info_span, warn};

use crate::metrics;
use crate::planner::delivered_load;
use crate::redispatch::{Request, Settings, Trigger, redispatch};
use crate::store::{DELIVERY_WINDOW, Store, StoreError};
use crate::travel::HaversineTravelTime;

/// How many triggers can wait before senders have to.
const QUEUE: usize = 1024;

/// The new version, or `None` when the re-dispatch changed nothing. `Err` says why there is no
/// new plan; the previous version stays.
pub type Reply = Result<Option<i64>, String>;

pub struct Job {
    trigger: Trigger,
    reply: Option<oneshot::Sender<Reply>>,
    /// Where the trigger came from: the re-dispatch continues its trace.
    span: Span,
}

/// Where triggers go.
#[derive(Clone)]
pub struct Jobs(mpsc::Sender<Job>);

impl Jobs {
    /// Queues a trigger without waiting for its re-dispatch.
    pub async fn send(&self, trigger: Trigger) {
        // Only fails once the worker has stopped, which only happens at shutdown.
        let _ = self
            .0
            .send(Job {
                trigger,
                reply: None,
                span: Span::current(),
            })
            .await;
    }

    /// Queues a trigger and waits for the re-dispatch it becomes part of.
    pub async fn run(&self, trigger: Trigger) -> Reply {
        let (reply, answer) = oneshot::channel();
        let job = Job {
            trigger,
            reply: Some(reply),
            span: Span::current(),
        };
        self.0
            .send(job)
            .await
            .map_err(|_| "the re-dispatch worker has stopped".to_owned())?;
        answer
            .await
            .map_err(|_| "the re-dispatch worker has stopped".to_owned())?
    }
}

pub fn channel() -> (Jobs, mpsc::Receiver<Job>) {
    let (tx, rx) = mpsc::channel(QUEUE);
    (Jobs(tx), rx)
}

#[derive(Debug, thiserror::Error)]
enum WorkerError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("no valid plan: {0:?}")]
    Invalid(Vec<String>),
    #[error("planning thread: {0}")]
    Join(#[from] tokio::task::JoinError),
}

/// Re-dispatches until every [`Jobs`] handle is dropped. `clock` gives the planning time; the
/// service passes the world clock. With `nats`, each stored version is announced on
/// [`PLANS_UPDATED`] for the map console.
pub async fn run(
    store: Store,
    mut jobs: mpsc::Receiver<Job>,
    settings: Settings,
    clock: fn() -> DateTime<Utc>,
    nats: Option<async_nats::Client>,
) {
    while let Some(first) = jobs.recv().await {
        let mut batch = vec![first];
        while let Ok(job) = jobs.try_recv() {
            batch.push(job);
        }
        let mut triggers: Vec<Trigger> = Vec::new();
        for job in &batch {
            if !triggers.contains(&job.trigger) {
                triggers.push(job.trigger.clone());
            }
        }
        let kinds: Vec<&str> = triggers.iter().map(Trigger::kind).collect();
        let span = info_span!(
            parent: &batch[0].span,
            "redispatch",
            triggers = %kinds.join(", "),
            previous_version = Empty,
            version = Empty,
            changes = Empty,
            outcome = Empty,
            otel.status_code = Empty,
        );
        for job in &batch[1..] {
            span.follows_from(&job.span);
        }
        let started = Instant::now();
        let now = clock().trunc_subsecs(0);
        let reply = once(&store, &triggers, &settings, now, nats.as_ref())
            .instrument(span.clone())
            .await;
        let outcome = match reply {
            Ok(Some(_)) => "new_version",
            Ok(None) => "unchanged",
            Err(_) => "failed",
        };
        span.record("outcome", outcome);
        metrics::redispatched(outcome, started.elapsed(), &triggers);
        let reply = reply.map_err(|e| {
            span.record("otel.status_code", "ERROR");
            error!(error = %e, ?triggers, "re-dispatch failed, the previous plan stays");
            e.to_string()
        });
        for job in batch {
            if let Some(tx) = job.reply {
                let _ = tx.send(reply.clone());
            }
        }
    }
}

async fn once(
    store: &Store,
    triggers: &[Trigger],
    settings: &Settings,
    now: DateTime<Utc>,
    nats: Option<&async_nats::Client>,
) -> Result<Option<i64>, WorkerError> {
    let travel = HaversineTravelTime::default();
    let (world, previous, orders, frozen) = async {
        let world = store.world().await?;
        let previous = store.plan(None).await?;
        let deliveries = store.deliveries(now - DELIVERY_WINDOW).await?;
        // Orders with deliveries or in the previous plan are needed even outside the window:
        // their frozen loads, and the reasons their loads left the plan, depend on them.
        let mut also: Vec<String> = deliveries.iter().map(|d| d.order.0.clone()).collect();
        if let Some(p) = &previous {
            also.extend(p.plan.loads.iter().map(|l| l.order.0.clone()));
            also.extend(p.plan.unplanned.iter().map(|u| u.order.0.clone()));
        }
        also.sort_unstable();
        also.dedup();
        let orders = store
            .planning_orders(now - TimeDelta::hours(1), now + settings.horizon, &also)
            .await?;
        let frozen: Vec<Load> = deliveries
            .iter()
            .filter_map(|d| {
                let order = orders.iter().find(|o| o.id == d.order)?;
                delivered_load(d, order, &world, &travel)
            })
            .collect();
        let span = Span::current();
        span.record("orders", orders.len());
        span.record("frozen", frozen.len());
        Ok::<_, WorkerError>((world, previous, orders, frozen))
    }
    .instrument(info_span!("load world", orders = Empty, frozen = Empty))
    .await?;
    let previous_version = previous.as_ref().map(|p| p.version);
    if let Some(v) = previous_version {
        Span::current().record("previous_version", v);
    }
    let previous_plan = previous.map(|p| p.plan);
    // Seeded by version, so a re-dispatch can be replayed.
    let seed = previous_version.map_or(0, |v| v.unsigned_abs() + 1);
    let (owned_triggers, owned_settings) = (triggers.to_vec(), settings.clone());
    // Created and closed here, on the async side. The blocking thread only enters a clone: closing
    // a span there would release its parent through that thread's default subscriber.
    let plan_span = info_span!("plan", loads = Empty, unplanned = Empty);
    let in_thread = plan_span.clone();
    let outcome = tokio::task::spawn_blocking(move || {
        let _entered = in_thread.enter();
        let req = Request {
            world: &world,
            orders: &orders,
            frozen: &frozen,
            previous: previous_plan.as_ref(),
            triggers: &owned_triggers,
            now,
        };
        redispatch(&req, &travel, &owned_settings, seed)
    })
    .await?
    .map_err(WorkerError::Invalid)?;
    plan_span.record("loads", outcome.plan.loads.len());
    plan_span.record("unplanned", outcome.plan.unplanned.len());
    drop(plan_span);

    // An explicit full re-plan always gets its version; anything else only when something changed.
    let asked = triggers.contains(&Trigger::FullReplan);
    if previous_version.is_some() && outcome.diff.is_empty() && !asked {
        info!(?triggers, "re-dispatch changed nothing");
        return Ok(None);
    }
    let version = store
        .save_plan(&outcome.plan, previous_version, triggers, &outcome.diff)
        .instrument(info_span!("save plan"))
        .await?;
    let span = Span::current();
    span.record("version", version);
    span.record("changes", outcome.diff.changes.len());
    metrics::current_plan(version, &outcome.plan);
    info!(
        version,
        changes = outcome.diff.changes.len(),
        triggers = %outcome.diff.triggers.join("; "),
        "re-dispatched"
    );
    if let Some(nats) = nats {
        announce(nats, version, outcome.plan.created_at).await;
    }
    Ok(Some(version))
}

/// How long an announcement may wait for the NATS client, so a broker outage never holds up
/// the worker.
const ANNOUNCE_TIMEOUT: Duration = Duration::from_secs(1);

/// Announces a stored version on core NATS, in the current (re-dispatch) span's trace. Best
/// effort: the version is stored either way, and a console that missed it reads the latest
/// version from `/world`, so a failure is only logged.
async fn announce(nats: &async_nats::Client, version: i64, created_at: DateTime<Utc>) {
    let payload = match serde_json::to_vec(&PlanUpdated {
        version,
        created_at,
    }) {
        Ok(payload) => payload,
        Err(error) => {
            warn!(%error, version, "couldn't encode the plan announcement");
            return;
        }
    };
    let mut headers = async_nats::HeaderMap::new();
    telemetry::inject(&mut headers);
    let publish = nats.publish_with_headers(PLANS_UPDATED, headers, payload.into());
    match tokio::time::timeout(ANNOUNCE_TIMEOUT, publish).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!(%error, version, "announcing the plan failed"),
        Err(_) => warn!(version, "announcing the plan timed out"),
    }
}
