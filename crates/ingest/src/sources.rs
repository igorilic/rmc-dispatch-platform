//! Where data comes in: webhooks (bauwerk-ag, neckar-infra), an SFTP poller (rheinbau-gmbh) and
//! an MQTT subscriber (telematics, batch tickets).
//!
//! Each source acknowledges only what is safely in JetStream:
//! - A webhook answers `202` after publishing, and `503` + `Retry-After` when it is full or NATS is down.
//! - MQTT uses QoS 1 with a persistent session and manual acks. A message is acked once NATS has it
//!   and is held, retrying, while NATS is down. That is the backpressure: polling stops and the
//!   broker queues.
//! - An SFTP file moves to `processed/` after its last row. A crash means the file is processed
//!   again, and dedup absorbs the repeats.
//!
//! Each delivery (a webhook request, an SFTP file, an MQTT message) is the root span of a trace
//! that continues into dispatch. A customer's own `traceparent` is ignored: callers are untrusted,
//! and their trace ids mean nothing in our Jaeger.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{SecondsFormat, Utc};
use domain::{Channel, Event, Seed};
use rumqttc::{AsyncClient, Event as MqttEvent, MqttOptions, Packet, Publish, QoS};
use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio::sync::{Semaphore, mpsc};
use tracing::field::Empty;
use tracing::{Instrument, Span, info, info_span, instrument, warn};

use crate::mapping::{Delivery, Mapping};
use crate::nats::{Bus, BusError, DeadLetter, Summary};
use crate::telematics;

const BODY_LIMIT: usize = 1 << 20;

#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    #[error("MQTT: {0}")]
    Mqtt(String),
    #[error("SFTP: {0}")]
    Sftp(String),
    #[error(transparent)]
    Bus(#[from] BusError),
}

fn sftp_error<E: std::fmt::Display>(e: E) -> SourceError {
    SourceError::Sftp(e.to_string())
}

/// The webhook integrations, each with its bearer token, and a limit on requests in flight.
#[derive(Clone)]
pub struct Webhooks {
    bus: Bus,
    seed: Arc<Seed>,
    integrations: Arc<HashMap<String, (Mapping, String)>>,
    in_flight: Arc<Semaphore>,
}

impl Webhooks {
    /// Keeps only the mappings whose channel is `webhook`.
    pub fn new(
        bus: Bus,
        seed: Arc<Seed>,
        mappings: Vec<Mapping>,
        token: impl Fn(&str) -> String,
        max_in_flight: usize,
    ) -> Self {
        let integrations = mappings
            .into_iter()
            .filter(|m| m.channel == Channel::Webhook)
            .map(|m| {
                let token = token(&m.integration);
                (m.integration.clone(), (m, token))
            })
            .collect();
        Webhooks {
            bus,
            seed,
            integrations: Arc::new(integrations),
            in_flight: Arc::new(Semaphore::new(max_in_flight)),
        }
    }
}

/// `POST /webhooks/{integration}` (at most 1 MiB) and `GET /healthz`.
pub fn webhook_router(webhooks: Webhooks) -> Router {
    Router::new()
        .route("/webhooks/{integration}", post(receive))
        .route("/healthz", get(|| async { "ok" }))
        .layer(DefaultBodyLimit::max(BODY_LIMIT))
        .with_state(webhooks)
}

/// A webhook's `202` body: what became of its records, and the trace to find them by.
#[derive(Serialize)]
struct Accepted {
    #[serde(flatten)]
    summary: Summary,
    #[serde(skip_serializing_if = "Option::is_none")]
    trace_id: Option<String>,
}

fn record_summary(span: &Span, summary: &Summary) {
    span.record("published", summary.published);
    span.record("duplicates", summary.duplicates);
    span.record("dead_lettered", summary.dead_lettered);
}

#[instrument(name = "webhook", skip_all, fields(%integration, published = Empty, duplicates = Empty, dead_lettered = Empty))]
async fn receive(
    State(hooks): State<Webhooks>,
    Path(integration): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // An unknown integration gets the same 401 as a wrong token, after the same comparison, so
    // neither the status nor the timing reveals which integration names exist.
    // ponytail: compared against a placeholder no header can carry (NUL isn't allowed in one).
    const NO_SUCH_TOKEN: &str = "\0";
    let known = hooks.integrations.get(&integration);
    let expected = known.map_or(NO_SUCH_TOKEN, |(_, token)| token.as_str());
    let presented = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    let authorised = presented.is_some_and(|p| same(p.as_bytes(), expected.as_bytes()));
    let Some((mapping, _)) = known.filter(|_| authorised) else {
        return (StatusCode::UNAUTHORIZED, "missing or wrong bearer token").into_response();
    };
    let Ok(_permit) = hooks.in_flight.try_acquire() else {
        return busy(2);
    };
    let received_at = Utc::now();
    let reference = format!(
        "webhook {}",
        received_at.to_rfc3339_opts(SecondsFormat::Millis, true)
    );
    let delivery = Delivery {
        body: &body,
        channel: Channel::Webhook,
        reference: &reference,
        received_at,
    };
    match hooks.bus.deliver(mapping, &hooks.seed, &delivery).await {
        Ok(summary) => {
            info!(
                integration,
                published = summary.published,
                duplicates = summary.duplicates,
                dead_lettered = summary.dead_lettered,
                "webhook"
            );
            let span = Span::current();
            record_summary(&span, &summary);
            let trace_id = telemetry::trace_id(&span);
            (StatusCode::ACCEPTED, Json(Accepted { summary, trace_id })).into_response()
        }
        Err(error) => {
            warn!(integration, %error, "webhook not accepted, NATS is unavailable");
            busy(5)
        }
    }
}

fn busy(retry_after_secs: u32) -> Response {
    let retry_after = [(header::RETRY_AFTER, retry_after_secs.to_string())];
    (
        StatusCode::SERVICE_UNAVAILABLE,
        retry_after,
        "busy, retry later",
    )
        .into_response()
}

/// Constant-time comparison, so response timing doesn't reveal how much of a token matched.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Subscribes to telematics and batch tickets and publishes them as events. Runs until the worker stops.
pub async fn run_mqtt(url: &str, bus: Bus, seed: Arc<Seed>) -> Result<(), SourceError> {
    // A stable client id with clean_session off keeps the session, and its queued messages,
    // across ingest restarts.
    let mut options = MqttOptions::parse_url(format!("{url}?client_id=pourline-ingest"))
        .map_err(|e| SourceError::Mqtt(e.to_string()))?;
    options.set_clean_session(false).set_manual_acks(true);
    let (client, mut eventloop) = AsyncClient::new(options, 1024);
    let (tx, mut rx) = mpsc::channel::<Publish>(1024);
    let acker = client.clone();
    tokio::spawn(async move {
        while let Some(publish) = rx.recv().await {
            handle_mqtt(&bus, &seed, &acker, publish).await;
        }
    });
    loop {
        match eventloop.poll().await {
            Ok(MqttEvent::Incoming(Packet::ConnAck(_))) => {
                info!("connected to the MQTT broker");
                for topic in ["trucks/+/+", "plants/+/tickets"] {
                    client
                        .subscribe(topic, QoS::AtLeastOnce)
                        .await
                        .map_err(|e| SourceError::Mqtt(e.to_string()))?;
                }
            }
            // A full channel blocks here, so polling stops and the broker holds the rest.
            Ok(MqttEvent::Incoming(Packet::Publish(publish))) => {
                tx.send(publish)
                    .await
                    .map_err(|_| SourceError::Mqtt("the worker stopped".into()))?;
            }
            Ok(_) => {}
            Err(error) => {
                warn!(%error, "MQTT connection failed, retrying in 2 s");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

async fn handle_mqtt(bus: &Bus, seed: &Seed, client: &AsyncClient, publish: Publish) {
    let received_at = Utc::now();
    let topic = publish.topic.as_str();
    let outcome = telematics::map(topic, &publish.payload, received_at, seed);
    // GPS and drum readings, about two a second, would bury every other trace. Dead letters are
    // always traced.
    let noise = matches!(&outcome, Ok(e) if matches!(
        e.event,
        Event::TruckPositionReported { .. } | Event::DrumRotationReported { .. }
    ));
    let span = info_span!("mqtt message", topic, noise);
    store_mqtt(bus, &publish, &outcome, received_at)
        .instrument(span)
        .await;
    if let Err(error) = client.ack(&publish).await {
        warn!(%error, topic, "MQTT ack failed, the broker will redeliver");
    }
}

/// Publishes or dead-letters one MQTT message, holding it while NATS is down.
async fn store_mqtt(
    bus: &Bus,
    publish: &Publish,
    outcome: &Result<domain::EventEnvelope, Vec<String>>,
    received_at: chrono::DateTime<Utc>,
) {
    let topic = publish.topic.as_str();
    loop {
        let stored = match &outcome {
            Ok(event) => bus.publish(event).await.map(drop),
            Err(errors) => {
                let integration = if topic.starts_with("plants/") {
                    "plants"
                } else {
                    "telematics"
                };
                warn!(topic, ?errors, "dead-lettered");
                let letter = DeadLetter {
                    integration: integration.into(),
                    channel: Channel::Mqtt,
                    reference: topic.into(),
                    record: topic.into(),
                    received_at,
                    errors: errors.clone(),
                    raw: String::from_utf8_lossy(&publish.payload).into_owned(),
                };
                bus.dead_letter(&letter).await
            }
        };
        match stored {
            Ok(()) => break,
            Err(error) => {
                warn!(%error, topic, "holding the MQTT message until NATS is back");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

/// An SFTP account to poll and how often.
#[derive(Clone)]
pub struct SftpSource {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub every: Duration,
}

/// Polls `<integration>/*.csv` forever. A failed poll is logged and retried on the next tick.
pub async fn run_sftp(source: SftpSource, bus: Bus, seed: Arc<Seed>, mapping: Arc<Mapping>) {
    let mut ticks = tokio::time::interval(source.every);
    loop {
        ticks.tick().await;
        if let Err(error) = poll_sftp(&source, &bus, &seed, &mapping).await {
            warn!(integration = %mapping.integration, %error, "SFTP poll failed, retrying at the next poll");
        }
    }
}

struct TrustAnyHostKey;

impl russh::client::Handler for TrustAnyHostKey {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _key: &russh::keys::PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        // ponytail: trusts any host key, fine for the local compose SFTP server; pin the key before pointing it at a real one.
        Ok(true)
    }
}

async fn poll_sftp(
    source: &SftpSource,
    bus: &Bus,
    seed: &Seed,
    mapping: &Mapping,
) -> Result<usize, SourceError> {
    let config = Arc::new(russh::client::Config::default());
    // The session handle must outlive the SFTP channel, so keep it in scope.
    let mut session =
        russh::client::connect(config, (source.host.as_str(), source.port), TrustAnyHostKey)
            .await
            .map_err(sftp_error)?;
    let auth = session
        .authenticate_password(&source.user, &source.password)
        .await
        .map_err(sftp_error)?;
    if !auth.success() {
        return Err(SourceError::Sftp(format!(
            "login rejected for user {}",
            source.user
        )));
    }
    let channel = session.channel_open_session().await.map_err(sftp_error)?;
    channel
        .request_subsystem(true, "sftp")
        .await
        .map_err(sftp_error)?;
    let sftp = russh_sftp::client::SftpSession::new(channel.into_stream())
        .await
        .map_err(sftp_error)?;

    let dir = mapping.integration.as_str();
    if !sftp.try_exists(dir).await.map_err(sftp_error)? {
        return Ok(0);
    }
    // `.part` files are still being written by the customer; only finished `.csv` files count.
    let mut names: Vec<String> = sftp
        .read_dir(dir)
        .await
        .map_err(sftp_error)?
        .map(|entry| entry.file_name())
        .filter(|name| name.ends_with(".csv"))
        .collect();
    names.sort();
    let processed = format!("{dir}/processed");
    if !names.is_empty()
        && !sftp
            .try_exists(processed.as_str())
            .await
            .map_err(sftp_error)?
    {
        sftp.create_dir(processed.as_str())
            .await
            .map_err(sftp_error)?;
    }
    for name in &names {
        let span = info_span!("sftp file", integration = %mapping.integration, file = %name, published = Empty, duplicates = Empty, dead_lettered = Empty);
        async {
            let path = format!("{dir}/{name}");
            let mut body = Vec::new();
            let mut file = sftp.open(path.as_str()).await.map_err(sftp_error)?;
            file.read_to_end(&mut body).await.map_err(sftp_error)?;
            let delivery = Delivery {
                body: &body,
                channel: Channel::Sftp,
                reference: name,
                received_at: Utc::now(),
            };
            let summary = bus.deliver(mapping, seed, &delivery).await?;
            record_summary(&Span::current(), &summary);
            sftp.rename(path, format!("{processed}/{name}"))
                .await
                .map_err(sftp_error)?;
            info!(integration = %mapping.integration, file = %name, published = summary.published, duplicates = summary.duplicates, dead_lettered = summary.dead_lettered, "SFTP file ingested");
            Ok::<_, SourceError>(())
        }
        .instrument(span)
        .await?;
    }
    Ok(names.len())
}
