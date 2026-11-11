//! The outbox forwarder: sends queued tickets to the broker, oldest first, with one ticket in
//! flight at a time, so they arrive in the order they were issued.
//!
//! A ticket leaves the outbox only once the broker has acknowledged it (QoS 1 PUBACK). If no PUBACK
//! arrives within the timeout, or the connection fails, the forwarder drops the connection, opens a
//! new one and sends the same row again. The session is clean, so rumqttc never replays anything
//! itself: the outbox is the only thing that resends. A resend carries the stored bytes, so it is
//! the same message, and ingest drops the copy by its event id.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use rumqttc::{AsyncClient, ConnectionError, Event, MqttOptions, Packet, QoS};
use tokio::sync::{Notify, mpsc};
use tokio::time::{Instant, sleep, timeout_at};
use tracing::{info, warn};

use crate::metrics;
use crate::store::{Store, StoreError};

/// How the forwarder reaches the broker, and how long it waits.
#[derive(Debug, Clone)]
pub struct Settings {
    pub mqtt_url: String,
    pub client_id: String,
    /// How long to wait for a PUBACK, and for the broker to accept a connection.
    pub ack_timeout: Duration,
    /// MQTT keep-alive, which also notices a dead link while the outbox is empty.
    pub keep_alive: Duration,
    /// The pause before reconnecting.
    pub retry: Duration,
}

impl Settings {
    /// The agent's defaults: a dead link is noticed within about 15 s.
    pub fn new(mqtt_url: impl Into<String>, client_id: impl Into<String>) -> Settings {
        // ponytail: fixed timings; make them settings if a real cellular link needs other ones.
        Settings {
            mqtt_url: mqtt_url.into(),
            client_id: client_id.into(),
            ack_timeout: Duration::from_secs(10),
            keep_alive: Duration::from_secs(10),
            retry: Duration::from_secs(2),
        }
    }
}

/// What the rest of the agent sees of the link to the broker.
#[derive(Debug, Default)]
pub struct Link {
    up: AtomicBool,
    sent: AtomicU64,
}

impl Link {
    /// Whether the broker accepted the connection and nothing has failed since.
    pub fn up(&self) -> bool {
        self.up.load(Ordering::Relaxed)
    }

    /// Tickets the broker has acknowledged since the agent started.
    pub fn sent(&self) -> u64 {
        self.sent.load(Ordering::Relaxed)
    }

    fn set_up(&self, up: bool) {
        if self.up.swap(up, Ordering::Relaxed) != up {
            metrics::connectivity("mqtt", up);
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ForwardError {
    #[error("invalid MQTT URL: {0}")]
    Url(String),
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// Why a connection ended.
enum End {
    Link(String),
    Store(StoreError),
}

impl From<StoreError> for End {
    fn from(e: StoreError) -> Self {
        End::Store(e)
    }
}

/// Sends the outbox until the outbox itself fails. `wake` says that a ticket was queued.
pub async fn run(
    store: Arc<Store>,
    settings: Settings,
    wake: Arc<Notify>,
    link: Arc<Link>,
) -> Result<(), ForwardError> {
    let mut options = MqttOptions::parse_url(format!(
        "{}?client_id={}",
        settings.mqtt_url, settings.client_id
    ))
    .map_err(|e| ForwardError::Url(e.to_string()))?;
    // The wait for the broker's ConnAck has its own deadline below, so no connection timeout here.
    options
        .set_clean_session(true)
        .set_keep_alive(settings.keep_alive);
    metrics::connectivity("mqtt", false);
    loop {
        match connection(&store, &options, &settings, &wake, &link).await {
            End::Store(e) => return Err(e.into()),
            End::Link(reason) => {
                link.set_up(false);
                warn!(%reason, "MQTT link down, reconnecting");
                sleep(settings.retry).await;
            }
        }
    }
}

/// One connection: sends until it fails. Dropping it closes the socket.
async fn connection(
    store: &Store,
    options: &MqttOptions,
    settings: &Settings,
    wake: &Notify,
    link: &Link,
) -> End {
    let (client, mut eventloop) = AsyncClient::new(options.clone(), 10);
    // The event loop runs in its own task and hands its events over a channel, whose receive is
    // safe to cancel in `select!`. The task stops at the first error or when this function returns.
    let (tx, mut events) = mpsc::channel::<Result<Event, ConnectionError>>(16);
    let driver = tokio::spawn(async move {
        loop {
            let event = eventloop.poll().await;
            let failed = event.is_err();
            if tx.send(event).await.is_err() || failed {
                break;
            }
        }
    });
    let _stop_driver = AbortOnDrop(driver);
    let deadline = Instant::now() + settings.ack_timeout;

    loop {
        match timeout_at(deadline, events.recv()).await {
            Err(_) => return End::Link("the broker didn't accept the connection in time".into()),
            Ok(None) => return End::Link("the MQTT event loop stopped".into()),
            Ok(Some(Err(e))) => return End::Link(e.to_string()),
            Ok(Some(Ok(Event::Incoming(Packet::ConnAck(_))))) => break,
            Ok(Some(Ok(_))) => {}
        }
    }
    info!(broker = %settings.mqtt_url, "MQTT link up");
    link.set_up(true);

    loop {
        let Some(next) = (match store.oldest() {
            Ok(next) => next,
            Err(e) => return e.into(),
        }) else {
            // Idle: keep draining events (pings) until a ticket is queued or the link fails.
            tokio::select! {
                () = wake.notified() => continue,
                event = events.recv() => match event {
                    None => return End::Link("the MQTT event loop stopped".into()),
                    Some(Err(e)) => return End::Link(e.to_string()),
                    Some(Ok(_)) => continue,
                },
            }
        };
        if let Err(e) = client
            .publish(next.topic, QoS::AtLeastOnce, false, next.payload)
            .await
        {
            return End::Link(e.to_string());
        }
        // One ticket is in flight, so the next PUBACK is its own.
        let deadline = Instant::now() + settings.ack_timeout;
        loop {
            match timeout_at(deadline, events.recv()).await {
                Err(_) => return End::Link("no PUBACK in time".into()),
                Ok(None) => return End::Link("the MQTT event loop stopped".into()),
                Ok(Some(Err(e))) => return End::Link(e.to_string()),
                Ok(Some(Ok(Event::Incoming(Packet::PubAck(_))))) => break,
                Ok(Some(Ok(_))) => {}
            }
        }
        if let Err(e) = store.remove(next.id) {
            return e.into();
        }
        link.sent.fetch_add(1, Ordering::Relaxed);
        metrics::ticket_sent();
    }
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}
