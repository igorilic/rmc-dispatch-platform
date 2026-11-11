//! Event intake: durable JetStream consumers that keep Postgres in step with `EVENTS` and tell the
//! re-dispatch worker what changed.
//!
//! - [`run`] reads `events.orders.*`. It upserts and cancels orders, keeping the newest revision of
//!   each by `occurred_at`. A new order, a cancellation, or a change to anything planning uses
//!   (volume, start, pour rate, gap, ship-to, mix) is a trigger.
//! - [`run_fleet`] reads `events.trucks.*` and `events.plants.*`. It keeps each truck's newest
//!   status and position, and turns batch tickets for planned loads into deliveries, which
//!   freeze those loads. A truck going out of service is a trigger.
//!
//! Each event is applied inside an `apply event` span that continues the trace ingest started, from
//! the message's `traceparent`. A trigger carries that span to the worker, so the re-dispatch it
//! causes joins the same trace (SPEC §8).
//!
//! Each consumer applies its events one at a time, in stream order (`max_ack_pending = 1`), so a
//! cancellation can't overtake the order it cancels. A redelivered or stale event changes nothing.
//! An event that can never apply (unreadable, or naming something we don't have) is logged and
//! acked, so it doesn't block the ones behind it. Any other failure, such as Postgres being down,
//! is retried after [`RETRY`].

use std::time::Duration;

use async_nats::jetstream::{self, AckKind, consumer::pull, stream::Stream};
use chrono_tz::Europe::Berlin;
use domain::{Event, EventEnvelope, Order, OrderStatus, TruckStatus};
use futures::StreamExt;
use sqlx::error::ErrorKind;
use tracing::field::Empty;
use tracing::{Instrument, Span, debug, info, info_span, warn};

use crate::redispatch::Trigger;
use crate::store::{Store, StoreError, Ticket, Upsert};
use crate::worker::Jobs;

const STREAM: &str = "EVENTS";
const RETRY: Duration = Duration::from_secs(2);

/// Applies order events until the task is dropped. Waits for ingest to create the `EVENTS`
/// stream, and resumes where the durable consumer left off after a restart.
pub async fn run(js: jetstream::Context, store: Store, jobs: Jobs) {
    consume(js, store, jobs, "dispatch-orders", &["events.orders.>"]).await;
}

/// Applies truck and plant events, like [`run`] does for orders.
pub async fn run_fleet(js: jetstream::Context, store: Store, jobs: Jobs) {
    let subjects = ["events.trucks.>", "events.plants.>"];
    consume(js, store, jobs, "dispatch-fleet", &subjects).await;
}

async fn consume(js: jetstream::Context, store: Store, jobs: Jobs, name: &str, subjects: &[&str]) {
    loop {
        let consumer = match consumer(&js, name, subjects).await {
            Ok(consumer) => consumer,
            Err(e) => {
                warn!(consumer = name, error = %e, "intake waiting for the EVENTS stream");
                tokio::time::sleep(RETRY).await;
                continue;
            }
        };
        let mut messages = match consumer.messages().await {
            Ok(messages) => messages,
            Err(e) => {
                warn!(consumer = name, error = %e, "intake can't pull messages");
                tokio::time::sleep(RETRY).await;
                continue;
            }
        };
        info!(consumer = name, "intake running");
        while let Some(message) = messages.next().await {
            let message = match message {
                Ok(message) => message,
                Err(e) => {
                    warn!(consumer = name, error = %e, "intake pull failed");
                    continue;
                }
            };
            let span = info_span!(
                "apply event",
                consumer = name,
                subject = %message.subject,
                event = Empty,
                order = Empty,
                truck = Empty,
                outcome = Empty,
                trigger = Empty,
            );
            telemetry::continue_trace(&span, message.headers.as_ref());
            let ack = async {
                match apply(&store, &message.payload).await {
                    Ok(trigger) => {
                        if let Some(trigger) = trigger {
                            Span::current().record("trigger", trigger.kind());
                            jobs.send(trigger).await;
                        }
                        AckKind::Ack
                    }
                    Err(e) if poison(&e) => {
                        Span::current().record("outcome", "skipped");
                        warn!(subject = %message.subject, error = %e, "event can never apply, skipped");
                        AckKind::Ack
                    }
                    Err(e) => {
                        Span::current().record("outcome", "retried");
                        warn!(subject = %message.subject, error = %e, "event failed, retrying");
                        AckKind::Nak(Some(RETRY))
                    }
                }
            }
            .instrument(span)
            .await;
            if let Err(e) = message.ack_with(ack).await {
                // Unacked, it's redelivered after the ack wait; applying it twice is harmless.
                warn!(consumer = name, error = %e, "intake ack failed");
            }
        }
    }
}

async fn consumer(
    js: &jetstream::Context,
    name: &str,
    subjects: &[&str],
) -> Result<jetstream::consumer::PullConsumer, String> {
    let stream: Stream = js.get_stream(STREAM).await.map_err(|e| e.to_string())?;
    stream
        .get_or_create_consumer(
            name,
            pull::Config {
                durable_name: Some(name.into()),
                filter_subjects: subjects.iter().map(|s| (*s).to_owned()).collect(),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                max_ack_pending: 1,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| e.to_string())
}

#[derive(Debug, thiserror::Error)]
enum IntakeError {
    #[error("unreadable event: {0}")]
    Unreadable(#[from] serde_json::Error),
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// Applies one event and returns the trigger it amounts to, if any.
async fn apply(store: &Store, payload: &[u8]) -> Result<Option<Trigger>, IntakeError> {
    let envelope: EventEnvelope = serde_json::from_slice(payload)?;
    let span = Span::current();
    span.record("event", envelope.id.as_str());
    match &envelope.event {
        Event::OrderUpserted { order, .. } => span.record("order", order.id.0.as_str()),
        Event::OrderCancelled { order, .. } => span.record("order", order.0.as_str()),
        Event::TruckStatusReported { truck, .. }
        | Event::TruckPositionReported { truck, .. }
        | Event::DrumRotationReported { truck, .. }
        | Event::BatchTicketIssued { truck, .. } => span.record("truck", truck.0.as_str()),
    };
    let at = envelope.occurred_at;
    let trigger = match envelope.event {
        Event::OrderUpserted {
            order,
            source_revision,
        } => match store
            .upsert_order(&order, at, source_revision.as_deref())
            .await?
        {
            Upsert::New => Some(Trigger::NewOrder { order: order.id }),
            Upsert::Changed(before) => changed(&before, &order),
            Upsert::Stale => {
                debug!(event = %envelope.id, "order event is stale");
                span.record("outcome", "stale");
                return Ok(None);
            }
        },
        Event::OrderCancelled { order, .. } => store
            .cancel_order(&order.0, at)
            .await?
            .then_some(Trigger::OrderCancelled { order }),
        Event::TruckStatusReported { truck, status } => {
            let before = store.report_truck_status(&truck, status, at).await?;
            let broke_down = status == TruckStatus::OutOfService
                && before.is_some_and(|b| b != TruckStatus::OutOfService);
            broke_down.then_some(Trigger::TruckOutOfService { truck })
        }
        Event::TruckPositionReported {
            truck, location, ..
        } => {
            store.report_truck_position(&truck, location, at).await?;
            None
        }
        Event::BatchTicketIssued {
            plant,
            ticket_no,
            truck,
            event,
            volume_m3,
            load: Some(load),
            ..
        } => {
            let ticket = Ticket {
                load: &load,
                plant: &plant,
                truck: &truck,
                volume_m3,
                ticket_no: &ticket_no,
                event,
                at,
            };
            if store.record_ticket(&ticket).await? {
                info!(order = %load.order.0, sequence = load.sequence, %ticket_no, "batching started, load frozen");
            }
            None
        }
        // Tickets for trips outside the plan, and drum rotation, don't concern planning.
        Event::BatchTicketIssued { load: None, .. } | Event::DrumRotationReported { .. } => None,
    };
    span.record("outcome", "applied");
    Ok(trigger)
}

/// The trigger for a changed order, if anything planning uses changed.
fn changed(before: &Order, after: &Order) -> Option<Trigger> {
    use OrderStatus::Cancelled;
    match (before.status, after.status) {
        (b, Cancelled) if b != Cancelled => {
            return Some(Trigger::OrderCancelled {
                order: after.id.clone(),
            });
        }
        (Cancelled, a) if a != Cancelled => {
            return Some(Trigger::NewOrder {
                order: after.id.clone(),
            });
        }
        _ => {}
    }
    let time = |o: &Order| {
        o.requested_start
            .with_timezone(&Berlin)
            .format("%a %H:%M")
            .to_string()
    };
    let mut parts = Vec::new();
    if before.total_volume_m3 != after.total_volume_m3 {
        parts.push(format!(
            "volume {} → {} m³",
            before.total_volume_m3, after.total_volume_m3
        ));
    }
    if before.requested_start != after.requested_start {
        parts.push(format!("start {} → {}", time(before), time(after)));
    }
    if before.pour_rate_m3_per_h != after.pour_rate_m3_per_h {
        parts.push(format!(
            "pour rate {} → {} m³/h",
            before.pour_rate_m3_per_h, after.pour_rate_m3_per_h
        ));
    }
    if before.max_gap_minutes != after.max_gap_minutes {
        parts.push(format!(
            "max gap {} → {} min",
            before.max_gap_minutes, after.max_gap_minutes
        ));
    }
    if before.site != after.site {
        parts.push(format!("ship-to {} → {}", before.site.0, after.site.0));
    }
    if before.mix != after.mix {
        parts.push(format!("mix {} → {}", before.mix.0, after.mix.0));
    }
    (!parts.is_empty()).then(|| Trigger::OrderChanged {
        order: after.id.clone(),
        change: parts.join(", "),
    })
}

/// Retrying won't help: the event itself is wrong, not the database's state of health.
fn poison(error: &IntakeError) -> bool {
    match error {
        IntakeError::Unreadable(_) | IntakeError::Store(StoreError::Invalid(_)) => true,
        IntakeError::Store(StoreError::Db(sqlx::Error::Database(e))) => {
            e.kind() != ErrorKind::Other
        }
        IntakeError::Store(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeDelta, TimeZone, Utc};
    use domain::{CustomerId, MixCode, OrderId, SiteId};

    use super::*;

    fn order() -> Order {
        Order {
            id: OrderId("10001001/BW-1".into()),
            customer: CustomerId("10001001".into()),
            site: SiteId("50001007".into()),
            mix: MixCode("C30/37-XC4".into()),
            total_volume_m3: 40.0,
            requested_start: Utc.with_ymd_and_hms(2026, 11, 10, 6, 0, 0).unwrap(),
            pour_rate_m3_per_h: 30.0,
            max_gap_minutes: 20,
            status: OrderStatus::Pending,
        }
    }

    #[test]
    fn only_changes_planning_uses_are_triggers() {
        let before = order();
        let confirmed = Order {
            status: OrderStatus::Confirmed,
            ..before.clone()
        };
        assert_eq!(changed(&before, &confirmed), None, "confirmation alone");

        let bigger_later = Order {
            total_volume_m3: 49.5,
            requested_start: before.requested_start + TimeDelta::hours(1),
            ..before.clone()
        };
        assert_eq!(
            changed(&before, &bigger_later),
            Some(Trigger::OrderChanged {
                order: before.id.clone(),
                change: "volume 40 → 49.5 m³, start Tue 07:00 → Tue 08:00".into()
            })
        );

        let cancelled = Order {
            status: OrderStatus::Cancelled,
            ..before.clone()
        };
        assert_eq!(
            changed(&before, &cancelled),
            Some(Trigger::OrderCancelled {
                order: before.id.clone()
            })
        );
        assert_eq!(
            changed(&cancelled, &before),
            Some(Trigger::NewOrder {
                order: before.id.clone()
            }),
            "reinstated"
        );
    }
}
