//! NATS JetStream: the `EVENTS` stream for canonical events, the `DLQ` stream for dead letters,
//! and the `ingest-dedup` KV bucket for order idempotency keys.
//!
//! Deduplication works in two layers:
//! - Every event is published with its id as `Nats-Msg-Id`, so JetStream drops a repeat within
//!   its 2-minute window. That is enough for telematics, whose duplicates arrive seconds apart.
//! - Order keys are also kept in the KV for `DEDUP_TTL_DAYS`, because rheinbau's snapshots repeat
//!   the same orders for days. The key is checked first and written only after the publish is
//!   acknowledged; a crash between the two is covered by the message-id window.
//!
//! Every NATS call retries with exponential backoff: 0.1 s doubling to 3.2 s, 6 attempts. The
//! caller then decides: a webhook answers 503, MQTT doesn't ack, an SFTP file stays put.
//!
//! Each event is published inside a `publish` span, whose trace context goes along in the
//! `traceparent` header, so dispatch continues the trace (SPEC §8).

use std::fmt::Display;
use std::future::Future;
use std::time::Duration;

use async_nats::HeaderMap;
use async_nats::header::NATS_MESSAGE_ID;
use async_nats::jetstream::{self, kv, stream};
use chrono::{DateTime, Utc};
use domain::{Channel, Event, EventEnvelope, Seed};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::field::Empty;
use tracing::{Instrument, info_span, warn};

use crate::mapping::{Delivery, Mapping};

pub const EVENTS: &str = "EVENTS";
pub const DLQ: &str = "DLQ";
const DEDUP_BUCKET: &str = "ingest-dedup";
const ATTEMPTS: u32 = 6;

#[derive(Debug, thiserror::Error)]
#[error("NATS {action} failed: {message}")]
pub struct BusError {
    action: &'static str,
    message: String,
}

fn err<E: Display>(action: &'static str) -> impl FnOnce(E) -> BusError {
    move |e| BusError {
        action,
        message: e.to_string(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Published {
    New,
    Duplicate,
}

/// What became of one delivery's records.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Summary {
    pub published: usize,
    pub duplicates: usize,
    pub dead_lettered: usize,
}

/// A record that can't be mapped as it is, kept with every error so it can be replayed after the
/// mapping is fixed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadLetter {
    pub integration: String,
    pub channel: Channel,
    pub reference: String,
    pub record: String,
    pub received_at: DateTime<Utc>,
    pub errors: Vec<String>,
    /// ponytail: kept as text, since every customer format is UTF-8 text. Non-UTF-8 bytes are
    /// replaced; store bytes instead if a binary format ever arrives.
    pub raw: String,
}

#[derive(Debug)]
pub struct StoredLetter {
    pub sequence: u64,
    pub letter: DeadLetter,
}

#[derive(Clone)]
pub struct Bus {
    js: jetstream::Context,
    dedup: kv::Store,
}

impl Bus {
    /// Connects and creates the streams and the KV bucket if they don't exist yet.
    pub async fn connect(url: &str, dedup_ttl: Duration) -> Result<Bus, BusError> {
        let client = retry("connect", || async_nats::connect(url)).await?;
        let js = jetstream::new(client);
        js.get_or_create_stream(stream::Config {
            name: EVENTS.into(),
            subjects: vec!["events.>".into()],
            max_age: Duration::from_secs(7 * 24 * 3600),
            duplicate_window: Duration::from_secs(120),
            ..Default::default()
        })
        .await
        .map_err(err("create the EVENTS stream"))?;
        js.get_or_create_stream(stream::Config {
            name: DLQ.into(),
            subjects: vec!["dlq.>".into()],
            allow_direct: true,
            ..Default::default()
        })
        .await
        .map_err(err("create the DLQ stream"))?;
        let dedup = js
            .create_or_update_key_value(kv::Config {
                bucket: DEDUP_BUCKET.into(),
                max_age: dedup_ttl,
                history: 1,
                ..Default::default()
            })
            .await
            .map_err(err("create the dedup KV"))?;
        Ok(Bus { js, dedup })
    }

    /// Publishes one event, or reports it as a duplicate without publishing it again.
    pub async fn publish(&self, event: &EventEnvelope) -> Result<Published, BusError> {
        let subject = event.subject();
        let span = info_span!("publish", event = %event.id, %subject, duplicate = Empty);
        let published = self
            .publish_in_span(event, subject)
            .instrument(span.clone())
            .await;
        if let Ok(outcome) = published {
            span.record("duplicate", outcome == Published::Duplicate);
        }
        published
    }

    async fn publish_in_span(
        &self,
        event: &EventEnvelope,
        subject: String,
    ) -> Result<Published, BusError> {
        let is_order = matches!(
            event.event,
            Event::OrderUpserted { .. } | Event::OrderCancelled { .. }
        );
        // KV keys allow only a few characters, so store a hash of the id.
        let kv_key = is_order.then(|| {
            Sha256::digest(event.id.as_bytes())
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        });
        if let Some(key) = &kv_key
            && retry("read the dedup KV", || self.dedup.get(key.as_str()))
                .await?
                .is_some()
        {
            return Ok(Published::Duplicate);
        }

        let payload = serde_json::to_vec(event).map_err(err("encode an event"))?;
        let mut headers = HeaderMap::new();
        headers.insert(NATS_MESSAGE_ID, event.id.as_str());
        telemetry::inject(&mut headers);
        let ack = retry("publish an event", || async {
            self.js
                .publish_with_headers(subject.clone(), headers.clone(), payload.clone().into())
                .await?
                .await
        })
        .await?;
        if let Some(key) = &kv_key {
            retry("write the dedup KV", || {
                self.dedup.put(key.as_str(), b"1".to_vec().into())
            })
            .await?;
        }
        Ok(if ack.duplicate {
            Published::Duplicate
        } else {
            Published::New
        })
    }

    /// Stores a record that can't be mapped. Its span is marked as an error, so it shows in red.
    pub async fn dead_letter(&self, letter: &DeadLetter) -> Result<(), BusError> {
        let span = info_span!(
            "dead letter",
            integration = %letter.integration,
            record = %letter.record,
            errors = letter.errors.len(),
            otel.status_code = "ERROR",
        );
        let payload = serde_json::to_vec(letter).map_err(err("encode a dead letter"))?;
        let subject = format!("dlq.{}", letter.integration);
        retry("dead-letter a record", || async {
            self.js
                .publish(subject.clone(), payload.clone().into())
                .await?
                .await
        })
        .instrument(span)
        .await?;
        Ok(())
    }

    /// Maps a delivery and publishes or dead-letters every record. On a NATS failure, records
    /// before it are already published; delivering again is safe because duplicates are skipped.
    pub async fn deliver(
        &self,
        mapping: &Mapping,
        seed: &Seed,
        delivery: &Delivery<'_>,
    ) -> Result<Summary, BusError> {
        let mut summary = Summary::default();
        for outcome in mapping.map(delivery, seed) {
            match outcome.result {
                Ok(event) => match self.publish(&event).await? {
                    Published::New => summary.published += 1,
                    Published::Duplicate => summary.duplicates += 1,
                },
                Err(errors) => {
                    let letter = DeadLetter {
                        integration: mapping.integration.clone(),
                        channel: delivery.channel,
                        reference: delivery.reference.to_owned(),
                        record: outcome.record,
                        received_at: delivery.received_at,
                        errors: errors.iter().map(ToString::to_string).collect(),
                        raw: String::from_utf8_lossy(&outcome.raw).into_owned(),
                    };
                    warn!(integration = %letter.integration, record = %letter.record, errors = ?letter.errors, "dead-lettered");
                    self.dead_letter(&letter).await?;
                    summary.dead_lettered += 1;
                }
            }
        }
        Ok(summary)
    }

    pub async fn dead_letters(&self) -> Result<Vec<StoredLetter>, BusError> {
        let mut dlq = self.js.get_stream(DLQ).await.map_err(err("open the DLQ"))?;
        let state = dlq.info().await.map_err(err("read the DLQ"))?.state.clone();
        let mut letters = Vec::new();
        if state.messages == 0 {
            return Ok(letters);
        }
        for sequence in state.first_sequence..=state.last_sequence {
            match dlq.direct_get(sequence).await {
                Ok(message) => {
                    let letter = serde_json::from_slice(&message.payload)
                        .map_err(err("decode a dead letter"))?;
                    letters.push(StoredLetter { sequence, letter });
                }
                // A gap: that letter was replayed and removed.
                Err(e) if matches!(e.kind(), stream::DirectGetErrorKind::NotFound) => {}
                Err(e) => return Err(err("read a dead letter")(e)),
            }
        }
        Ok(letters)
    }

    pub async fn remove_dead_letter(&self, sequence: u64) -> Result<(), BusError> {
        let dlq = self.js.get_stream(DLQ).await.map_err(err("open the DLQ"))?;
        dlq.delete_message(sequence)
            .await
            .map_err(err("remove a dead letter"))?;
        Ok(())
    }

    /// How many messages a stream holds.
    pub async fn count(&self, stream: &str) -> Result<u64, BusError> {
        let mut stream = self
            .js
            .get_stream(stream)
            .await
            .map_err(err("open a stream"))?;
        Ok(stream
            .info()
            .await
            .map_err(err("read a stream"))?
            .state
            .messages)
    }
}

async fn retry<T, E, F, Fut>(action: &'static str, mut call: F) -> Result<T, BusError>
where
    E: Display,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut delay = Duration::from_millis(100);
    let mut attempt = 1;
    loop {
        match call().await {
            Ok(value) => return Ok(value),
            Err(e) if attempt == ATTEMPTS => return Err(err(action)(e)),
            Err(e) => {
                warn!(action, attempt, error = %e, "NATS call failed, retrying");
                tokio::time::sleep(delay).await;
                delay *= 2;
                attempt += 1;
            }
        }
    }
}
