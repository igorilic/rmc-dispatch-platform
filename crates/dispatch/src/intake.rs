//! Order intake: a durable JetStream consumer that applies order events from `EVENTS` to Postgres.
//!
//! The store keeps the newest revision of each order by `occurred_at`, so a redelivered or stale
//! event changes nothing. Events are applied one at a time (`max_ack_pending = 1`), in stream
//! order: a cancellation can't overtake the order it cancels while that order waits for a retry.
//!
//! An event that can never apply (unreadable, or naming a ship-to we don't have) is logged and
//! acked, so it doesn't block the ones behind it. Any other failure, such as Postgres being down,
//! is retried after [`RETRY`].

use std::time::Duration;

use async_nats::jetstream::{self, AckKind, consumer::pull, stream::Stream};
use domain::{Event, EventEnvelope};
use futures::StreamExt;
use sqlx::error::ErrorKind;
use tracing::{debug, info, warn};

use crate::store::{Store, StoreError};

const STREAM: &str = "EVENTS";
const CONSUMER: &str = "dispatch-orders";
const RETRY: Duration = Duration::from_secs(2);

/// Applies order events until the task is dropped. Waits for ingest to create the `EVENTS`
/// stream, and resumes where the durable consumer left off after a restart.
pub async fn run(js: jetstream::Context, store: Store) {
    loop {
        let consumer = match consumer(&js).await {
            Ok(consumer) => consumer,
            Err(e) => {
                warn!(error = %e, "order intake waiting for the EVENTS stream");
                tokio::time::sleep(RETRY).await;
                continue;
            }
        };
        let mut messages = match consumer.messages().await {
            Ok(messages) => messages,
            Err(e) => {
                warn!(error = %e, "order intake can't pull messages");
                tokio::time::sleep(RETRY).await;
                continue;
            }
        };
        info!("order intake running");
        while let Some(message) = messages.next().await {
            let message = match message {
                Ok(message) => message,
                Err(e) => {
                    warn!(error = %e, "order intake pull failed");
                    continue;
                }
            };
            let ack = match apply(&store, &message.payload).await {
                Ok(()) => AckKind::Ack,
                Err(e) if poison(&e) => {
                    warn!(subject = %message.subject, error = %e, "order event can never apply, skipped");
                    AckKind::Ack
                }
                Err(e) => {
                    warn!(subject = %message.subject, error = %e, "order event failed, retrying");
                    AckKind::Nak(Some(RETRY))
                }
            };
            if let Err(e) = message.ack_with(ack).await {
                // Unacked, it's redelivered after the ack wait; applying it twice is harmless.
                warn!(error = %e, "order intake ack failed");
            }
        }
    }
}

async fn consumer(js: &jetstream::Context) -> Result<jetstream::consumer::PullConsumer, String> {
    let stream: Stream = js.get_stream(STREAM).await.map_err(|e| e.to_string())?;
    stream
        .get_or_create_consumer(
            CONSUMER,
            pull::Config {
                durable_name: Some(CONSUMER.into()),
                filter_subject: "events.orders.>".into(),
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

async fn apply(store: &Store, payload: &[u8]) -> Result<(), IntakeError> {
    let envelope: EventEnvelope = serde_json::from_slice(payload)?;
    let applied = match &envelope.event {
        Event::OrderUpserted {
            order,
            source_revision,
        } => {
            store
                .upsert_order(order, envelope.occurred_at, source_revision.as_deref())
                .await?
        }
        Event::OrderCancelled { order, .. } => {
            store.cancel_order(&order.0, envelope.occurred_at).await?
        }
        _ => true,
    };
    if !applied {
        debug!(event = %envelope.id, "order event is stale or cancels an unknown order");
    }
    Ok(())
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
