//! The forwarder against a real Mosquitto. Pausing the container stands in for a cut link: the
//! broker's sockets stay open, but nothing answers, as on a dead cellular link.

// Integration tests are test code; SPEC §15 allows unwrap there.
#![allow(clippy::unwrap_used)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use domain::PlantId;
use plant_agent::forward::{self, Link, Settings};
use plant_agent::store::Store;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::sync::{Notify, mpsc};
use tokio::time::{Instant, sleep, timeout};

const TOPIC: &str = "plants/plant-walldorf/tickets";

/// Same image as deploy/compose, with the image's own anonymous-access config.
async fn mosquitto() -> (ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("eclipse-mosquitto", "2.1.2-alpine")
        .with_exposed_port(1883.tcp())
        .with_wait_for(WaitFor::message_on_stderr("running"))
        .with_cmd(["mosquitto", "-c", "/mosquitto-no-auth.conf"])
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(1883).await.unwrap();
    (container, format!("mqtt://127.0.0.1:{port}"))
}

/// Every payload published on the tickets topic, in the order the broker delivers them. Like
/// ingest, it keeps a persistent session and reconnects, so the broker queues for it meanwhile.
async fn subscribe(url: &str) -> mpsc::UnboundedReceiver<Vec<u8>> {
    let mut options = MqttOptions::parse_url(format!("{url}?client_id=test-ingest")).unwrap();
    options.set_clean_session(false);
    let (client, mut eventloop) = AsyncClient::new(options, 10);
    let (tx, rx) = mpsc::unbounded_channel();
    let (subscribed_tx, subscribed) = tokio::sync::oneshot::channel();
    let mut subscribed_tx = Some(subscribed_tx);
    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    client.try_subscribe(TOPIC, QoS::AtLeastOnce).unwrap();
                }
                Ok(Event::Incoming(Packet::SubAck(_))) => {
                    if let Some(tx) = subscribed_tx.take() {
                        let _ = tx.send(());
                    }
                }
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    let _ = tx.send(p.payload.to_vec());
                }
                Ok(_) => {}
                // The port forward may not be ready yet, or the broker is paused: try again.
                Err(_) => sleep(Duration::from_millis(200)).await,
            }
        }
    });
    timeout(Duration::from_secs(30), subscribed)
        .await
        .unwrap()
        .unwrap();
    rx
}

fn sync_topic() -> String {
    domain::sync_topic(&PlantId("plant-walldorf".into()))
}

fn settings(url: &str) -> Settings {
    Settings {
        ack_timeout: Duration::from_secs(2),
        keep_alive: Duration::from_secs(2),
        retry: Duration::from_millis(300),
        ..Settings::new(url, "test-plant-agent", sync_topic())
    }
}

/// Publishes `payload` on `topic` at QoS 1, as the console does, and waits for the broker's PUBACK.
async fn publish(url: &str, topic: &str, payload: &str) {
    let options = MqttOptions::parse_url(format!("{url}?client_id=test-console")).unwrap();
    let (client, mut eventloop) = AsyncClient::new(options, 10);
    client
        .publish(topic, QoS::AtLeastOnce, false, payload.as_bytes().to_vec())
        .await
        .unwrap();
    timeout(Duration::from_secs(10), async {
        while !matches!(
            eventloop.poll().await,
            Ok(Event::Incoming(Packet::PubAck(_)))
        ) {}
    })
    .await
    .unwrap();
}

fn temp_db(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "plant-agent-forward-{}-{name}.sqlite",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    path
}

fn ticket(n: u32) -> Vec<u8> {
    format!(r#"{{"ticket_no":"WAL-{n:06}"}}"#).into_bytes()
}

/// Receives until `expected` payloads have each arrived at least once, and returns every payload
/// in arrival order, duplicates included.
async fn receive_all(
    rx: &mut mpsc::UnboundedReceiver<Vec<u8>>,
    expected: &[Vec<u8>],
) -> Vec<Vec<u8>> {
    let mut received = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    while !expected.iter().all(|e| received.contains(e)) {
        let left = deadline.saturating_duration_since(Instant::now());
        let payload = timeout(left, rx.recv()).await.unwrap_or_else(|_| {
            panic!("only got {} of {} tickets", received.len(), expected.len())
        });
        received.push(payload.unwrap());
    }
    received
}

/// The first arrival of each payload, in order.
fn first_arrivals(received: &[Vec<u8>]) -> Vec<Vec<u8>> {
    let mut seen: Vec<Vec<u8>> = Vec::new();
    for p in received {
        if !seen.contains(p) {
            seen.push(p.clone());
        }
    }
    seen
}

async fn until(what: &str, check: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !check() {
        assert!(Instant::now() < deadline, "gave up waiting for {what}");
        sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn tickets_queued_while_the_broker_is_unreachable_arrive_in_order() {
    let (broker, url) = mosquitto().await;
    let mut received = subscribe(&url).await;
    let path = temp_db("cut");
    let store = Arc::new(Store::open(&path).unwrap());
    let (wake, link) = (Arc::new(Notify::new()), Arc::new(Link::default()));
    tokio::spawn(forward::run(
        Arc::clone(&store),
        settings(&url),
        Arc::clone(&wake),
        Arc::clone(&link),
    ));

    store.enqueue(TOPIC, &ticket(1), Utc::now()).unwrap();
    wake.notify_one();
    assert_eq!(receive_all(&mut received, &[ticket(1)]).await, [ticket(1)]);
    until("the link to be up", || link.up()).await;

    broker.pause().await.unwrap();
    let queued: Vec<Vec<u8>> = (2..=6).map(ticket).collect();
    for payload in &queued {
        store.enqueue(TOPIC, payload, Utc::now()).unwrap();
        wake.notify_one();
    }
    until("the forwarder to notice the cut", || !link.up()).await;
    assert_eq!(
        store.waiting().unwrap(),
        5,
        "nothing leaves the outbox unacknowledged"
    );

    broker.unpause().await.unwrap();
    let all = receive_all(&mut received, &queued).await;
    assert_eq!(
        first_arrivals(&all),
        queued,
        "in the order they were queued"
    );
    until("the outbox to drain", || store.oldest().unwrap().is_none()).await;
    until("the link to be up again", || link.up()).await;
    assert_eq!(link.sent(), 6);
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn a_restarted_agent_sends_what_the_last_one_queued() {
    let (broker, url) = mosquitto().await;
    let mut received = subscribe(&url).await;
    let path = temp_db("restart");
    broker.pause().await.unwrap();

    // The first agent queues three tickets but can't send them, and then dies.
    let queued: Vec<Vec<u8>> = (1..=3).map(ticket).collect();
    {
        let store = Arc::new(Store::open(&path).unwrap());
        for payload in &queued {
            store.enqueue(TOPIC, payload, Utc::now()).unwrap();
        }
        let first = tokio::spawn(forward::run(
            store,
            settings(&url),
            Arc::new(Notify::new()),
            Arc::new(Link::default()),
        ));
        sleep(Duration::from_secs(3)).await;
        first.abort();
        let _ = first.await;
    }

    // The second agent opens the same file and finds them.
    let store = Arc::new(Store::open(&path).unwrap());
    tokio::spawn(forward::run(
        Arc::clone(&store),
        settings(&url),
        Arc::new(Notify::new()),
        Arc::new(Link::default()),
    ));
    broker.unpause().await.unwrap();
    let all = receive_all(&mut received, &queued).await;
    assert_eq!(first_arrivals(&all), queued);
    assert!(
        all.iter().all(|p| queued.contains(p)),
        "a resend carries exactly the stored bytes"
    );
    until("the outbox to drain", || store.oldest().unwrap().is_none()).await;
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn a_sync_command_reaches_the_agent_even_while_tickets_are_in_flight() {
    let (_broker, url) = mosquitto().await;
    let mut received = subscribe(&url).await;
    let path = temp_db("sync");
    let store = Arc::new(Store::open(&path).unwrap());
    let (wake, link) = (Arc::new(Notify::new()), Arc::new(Link::default()));
    tokio::spawn(forward::run(
        Arc::clone(&store),
        settings(&url),
        Arc::clone(&wake),
        Arc::clone(&link),
    ));
    until("the link to be up, subscription included", || link.up()).await;

    // A burst keeps tickets waiting for their PUBACKs while the command comes in.
    let burst: Vec<Vec<u8>> = (1..=50).map(ticket).collect();
    for payload in &burst {
        store.enqueue(TOPIC, payload, Utc::now()).unwrap();
    }
    wake.notify_one();
    let traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    publish(&url, &sync_topic(), traceparent).await;

    let requested = timeout(Duration::from_secs(10), link.sync_requested())
        .await
        .unwrap();
    assert_eq!(requested.as_deref(), Some(traceparent));
    let all = receive_all(&mut received, &burst).await;
    assert_eq!(first_arrivals(&all), burst, "the outbox drains as before");
    let _ = std::fs::remove_file(&path);
}
