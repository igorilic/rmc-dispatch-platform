//! The console against real NATS (fed with ingest's own Bus) and Mosquitto. Dispatch, the plant's
//! agent, its uplink (Toxiproxy) and the customers' webhooks are one small stand-in server.

// Integration tests are test code; SPEC §15 allows unwrap there.
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use chrono::{DateTime, TimeDelta, Utc};
use console::state::Step;
use console::{Console, Settings, Update, Webhook};
use domain::{
    Channel, Event, EventEnvelope, EventSource, Location, PLANS_UPDATED, PlanUpdated, PlantId,
    TruckId, TruckStatus,
};
use ingest::nats::Bus;
use rumqttc::{AsyncClient, Event as MqttEvent, MqttOptions, Packet, QoS};
use serde_json::{Value, json};
use telemetry::testing::capture;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::sync::{broadcast, mpsc};
use tokio::time::{Instant, sleep, timeout};
use tracing::info_span;

async fn nats() -> (ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("nats", "2.15.3-alpine")
        .with_exposed_port(4222.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
        .with_cmd(["--jetstream"])
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(4222).await.unwrap();
    (container, format!("nats://127.0.0.1:{port}"))
}

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

/// What the stand-in serves, and what it was asked.
#[derive(Default)]
struct Fake {
    world: Value,
    plans: Vec<Value>,
    seen: Vec<Seen>,
    /// Ids of the orders posted to the webhooks.
    orders: Vec<String>,
    /// The agent's link to the broker, as its `/status` reports it, while the uplink is up.
    agent_mqtt: bool,
    proxies: BTreeMap<String, bool>,
    resets: usize,
    patches: Vec<(String, bool)>,
}

#[derive(Debug, Clone)]
struct Seen {
    method: String,
    path: String,
    traceparent: Option<String>,
}

type Shared = Arc<Mutex<Fake>>;

fn at(minute: i64) -> DateTime<Utc> {
    "2026-11-10T09:00:00Z".parse::<DateTime<Utc>>().unwrap() + TimeDelta::minutes(minute)
}

impl Fake {
    fn new() -> Fake {
        let plants = ["plant-mannheim", "plant-walldorf", "plant-weinheim"]
            .map(|id| json!({ "id": id, "status": "online" }));
        Fake {
            world: json!({ "now": at(0), "plan_version": null, "plants": plants, "trucks": [] }),
            agent_mqtt: true,
            proxies: [("dispatch".to_owned(), true), ("mqtt".to_owned(), true)].into(),
            ..Fake::default()
        }
    }

    fn cut(&self) -> bool {
        self.proxies.values().any(|enabled| !enabled)
    }

    /// The agent's `/status`: tickets wait in the outbox while the uplink is cut.
    fn status(&self) -> Value {
        json!({
            "plant": "plant-walldorf",
            "mqtt": self.agent_mqtt && !self.cut(),
            "dispatch": !self.cut(),
            "outbox": if self.cut() { 2 } else { 0 },
            "sync_lag_seconds": 0,
            "plan_version": self.plans.last().map(|p| p["version"].clone()),
            "plan_age_seconds": 1,
            "queued": 0,
            "sent": 0,
        })
    }

    fn plan(&mut self, loads: Vec<Value>) -> Value {
        let plan = json!({
            "version": self.plans.len() + 1,
            "created_at": at(0),
            "loads": loads,
            "unplanned": [],
        });
        self.plans.push(plan.clone());
        plan
    }
}

async fn handle(
    State(fake): State<Shared>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: String,
) -> (StatusCode, Json<Value>) {
    let path = uri.path().to_owned();
    let mut f = fake.lock().unwrap();
    f.seen.push(Seen {
        method: method.to_string(),
        path: path.clone(),
        traceparent: headers
            .get("traceparent")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned),
    });
    let ok = |value: Value| (StatusCode::OK, Json(value));
    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    match (method.as_str(), segments.as_slice()) {
        ("GET", ["world"]) => ok(f.world.clone()),
        ("GET", ["plans", "latest"]) => match f.plans.last() {
            Some(plan) => ok(plan.clone()),
            None => (StatusCode::NOT_FOUND, Json(Value::Null)),
        },
        ("GET", ["plans", _, "diff"]) => ok(json!({ "triggers": ["test"], "changes": [] })),
        ("GET", ["plans", version]) => {
            let version: usize = version.parse().unwrap();
            ok(f.plans[version - 1].clone())
        }
        ("POST", ["plans"]) => {
            let plan = f.plan(Vec::new());
            (StatusCode::CREATED, Json(plan))
        }
        ("PUT", ["plants", _, "status"]) | ("POST", ["sites", _, "delays"]) => {
            ok(json!({ "version": null, "diff": null }))
        }
        ("POST", ["webhooks", integration]) => {
            let customer = match *integration {
                "bauwerk-ag" => "10001001",
                _ => "10001003",
            };
            let at = body.find("EDGE-").unwrap();
            f.orders.push(format!("{customer}/{}", &body[at..at + 13]));
            // Dispatch plans the three together.
            if f.orders.len() % 3 == 0 {
                let loads = f.orders[f.orders.len() - 3..]
                    .iter()
                    .map(|order| json!({ "order": order, "planned_plant": "plant-walldorf" }))
                    .collect();
                f.plan(loads);
            }
            (StatusCode::ACCEPTED, Json(json!({ "trace_id": "0" })))
        }
        ("GET", ["status"]) => ok(f.status()),
        ("GET", ["proxies"]) => {
            let proxies: serde_json::Map<String, Value> = f
                .proxies
                .iter()
                .map(|(name, enabled)| (name.clone(), json!({ "name": name, "enabled": enabled })))
                .collect();
            ok(Value::Object(proxies))
        }
        ("PATCH", ["proxies", name]) => {
            let enabled = serde_json::from_str::<Value>(&body).unwrap()["enabled"] == true;
            f.proxies.insert((*name).to_owned(), enabled);
            f.patches.push(((*name).to_owned(), enabled));
            ok(json!({ "name": name, "enabled": enabled }))
        }
        ("POST", ["reset"]) => {
            f.resets += 1;
            f.proxies.values_mut().for_each(|enabled| *enabled = true);
            (StatusCode::NO_CONTENT, Json(Value::Null))
        }
        _ => (StatusCode::NOT_FOUND, Json(Value::Null)),
    }
}

struct Setup {
    _containers: (ContainerAsync<GenericImage>, ContainerAsync<GenericImage>),
    bus: Bus,
    nats: async_nats::Client,
    mqtt_url: String,
    fake: Shared,
    console: Arc<Console>,
}

impl Setup {
    fn fake(&self) -> std::sync::MutexGuard<'_, Fake> {
        self.fake.lock().unwrap()
    }

    fn seen(&self, method: &str, path: &str) -> Option<Seen> {
        self.fake()
            .seen
            .iter()
            .find(|s| s.method == method && s.path == path)
            .cloned()
    }

    fn count(&self, method: &str, path: &str) -> usize {
        let fake = self.fake();
        fake.seen
            .iter()
            .filter(|s| s.method == method && s.path == path)
            .count()
    }
}

async fn setup(cut: Duration) -> Setup {
    let ((nats_container, nats_url), (mqtt_container, mqtt_url)) =
        tokio::join!(nats(), mosquitto());
    // Creates the EVENTS stream, as ingest does.
    let bus = Bus::connect(&nats_url, Duration::from_secs(3600))
        .await
        .unwrap();
    let fake: Shared = Arc::new(Mutex::new(Fake::new()));
    let app = Router::new().fallback(handle).with_state(Arc::clone(&fake));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });

    let webhook = |integration: &str| Webhook {
        url: format!("{url}/webhooks/{integration}"),
        token: "test".into(),
    };
    let settings = Settings {
        dispatch_url: url.clone(),
        plant: PlantId("plant-walldorf".into()),
        plant_agent_url: Some(url.clone()),
        uplink_url: Some(url.clone()),
        cut,
        poll: Duration::from_millis(100),
        public_url: "http://localhost:8080".into(),
        bauwerk: webhook("bauwerk-ag"),
        neckar: webhook("neckar-infra"),
    };
    let options = MqttOptions::parse_url(format!("{mqtt_url}?client_id=test-console")).unwrap();
    let (mqtt, eventloop) = AsyncClient::new(options, 10);
    let console = Console::new(settings, mqtt).unwrap();
    let nats = async_nats::connect(&nats_url).await.unwrap();
    console.start(nats.clone(), eventloop);
    Setup {
        _containers: (nats_container, mqtt_container),
        bus,
        nats,
        mqtt_url,
        fake,
        console,
    }
}

async fn until(what: &str, check: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !check() {
        assert!(Instant::now() < deadline, "gave up waiting for {what}");
        sleep(Duration::from_millis(50)).await;
    }
}

/// The next `event` the console pushes that `wanted` accepts.
async fn next(
    updates: &mut broadcast::Receiver<Update>,
    event: &str,
    wanted: impl Fn(&Value) -> bool,
) -> Value {
    timeout(Duration::from_secs(20), async {
        loop {
            let update = updates.recv().await.unwrap();
            if update.event == event {
                let data: Value = serde_json::from_str(&update.data).unwrap();
                if wanted(&data) {
                    return data;
                }
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no {event} pushed"))
}

fn envelope(event: Event, occurred_at: DateTime<Utc>) -> EventEnvelope {
    EventEnvelope {
        id: format!("test:{}", occurred_at.timestamp_nanos_opt().unwrap()),
        occurred_at,
        received_at: domain::clock::now(),
        source: EventSource {
            integration: "telematics".into(),
            channel: Channel::Mqtt,
            schema_version: None,
            reference: "test".into(),
        },
        event,
    }
}

fn status(truck: &str, status: TruckStatus, at: DateTime<Utc>) -> EventEnvelope {
    let truck = TruckId(truck.into());
    envelope(Event::TruckStatusReported { truck, status }, at)
}

/// Every message on `filters`, as (topic, payload).
async fn listen(url: &str, filters: &[&'static str]) -> mpsc::UnboundedReceiver<(String, String)> {
    let options = MqttOptions::parse_url(format!("{url}?client_id=test-listener")).unwrap();
    let (client, mut eventloop) = AsyncClient::new(options, 10);
    let (tx, rx) = mpsc::unbounded_channel();
    let (subscribed_tx, subscribed) = tokio::sync::oneshot::channel();
    let mut subscribed_tx = Some(subscribed_tx);
    let filters = filters.to_vec();
    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(MqttEvent::Incoming(Packet::ConnAck(_))) => {
                    for filter in &filters {
                        client.try_subscribe(*filter, QoS::AtLeastOnce).unwrap();
                    }
                }
                Ok(MqttEvent::Incoming(Packet::SubAck(_))) => {
                    if let Some(tx) = subscribed_tx.take() {
                        let _ = tx.send(());
                    }
                }
                Ok(MqttEvent::Incoming(Packet::Publish(p))) => {
                    let payload = String::from_utf8_lossy(&p.payload).into_owned();
                    let _ = tx.send((p.topic, payload));
                }
                Ok(_) => {}
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

#[tokio::test]
async fn a_page_gets_the_world_then_live_events_and_the_newest_report_wins() {
    let s = setup(Duration::from_secs(1)).await;
    s.fake().world["trucks"] = json!([{
        "id": "truck-07", "status": "en_route", "status_at": at(1),
        "position": null, "position_at": null,
    }]);
    s.console.resync();
    let status_of = |s: &Setup| {
        let snapshot = s.console.snapshot();
        let truck = snapshot.trucks.iter().find(|t| t.id == "truck-07").unwrap();
        truck.status
    };
    until("/world's report", || status_of(&s) == TruckStatus::EnRoute).await;

    let (_, mut updates) = s.console.subscribe();
    s.bus
        .publish(&status("truck-07", TruckStatus::OutOfService, at(2)))
        .await
        .unwrap();
    let truck = next(&mut updates, "truck", |t| t["id"] == "truck-07").await;
    assert_eq!(truck["status"], "out_of_service");
    let fix = Location {
        lat: 49.42,
        lon: 8.6,
    };
    let gps = Event::TruckPositionReported {
        truck: TruckId("truck-07".into()),
        location: fix,
        speed_kmh: 0.0,
    };
    s.bus.publish(&envelope(gps, at(3))).await.unwrap();
    let truck = next(&mut updates, "truck", |t| !t["position"].is_null()).await;
    assert_eq!(truck["position"], json!({ "lat": 49.42, "lon": 8.6 }));

    // A /world that hasn't caught up changes nothing.
    let reads = s.count("GET", "/world");
    s.console.resync();
    until("another /world", || s.count("GET", "/world") > reads).await;
    sleep(Duration::from_millis(200)).await;
    assert_eq!(status_of(&s), TruckStatus::OutOfService);
}

#[tokio::test]
async fn each_plan_version_is_pushed_once_in_the_trace_that_made_it() {
    let (_guard, exporter) = capture();
    let s = setup(Duration::from_secs(1)).await;
    let (_, mut updates) = s.console.subscribe();
    let redispatch = info_span!("redispatch");
    let mut headers = async_nats::HeaderMap::new();
    redispatch.in_scope(|| telemetry::inject(&mut headers));
    let trace = telemetry::trace_id(&redispatch).unwrap();
    let announcement = serde_json::to_vec(&PlanUpdated {
        version: 1,
        created_at: at(0),
    })
    .unwrap();
    // Core NATS drops what comes before the console subscribes, so announce until it hears.
    let nats = s.nats.clone();
    let announcing = tokio::spawn(async move {
        loop {
            let payload = announcement.clone().into();
            nats.publish_with_headers(PLANS_UPDATED, headers.clone(), payload)
                .await
                .unwrap();
            sleep(Duration::from_millis(100)).await;
        }
    });

    let plan = next(&mut updates, "plan", |_| true).await;
    assert_eq!(plan["version"], 1);
    assert_eq!(plan["trace_id"], trace.as_str());
    let diff = s.seen("GET", "/plans/1/diff").unwrap();
    assert!(
        diff.traceparent.unwrap().contains(&trace),
        "the diff is fetched in the same trace"
    );
    let again = timeout(Duration::from_secs(1), next(&mut updates, "plan", |_| true)).await;
    assert!(again.is_err(), "announced again and again, pushed once");
    announcing.abort();
    drop(redispatch);
    let spans = exporter.get_finished_spans().unwrap();
    assert!(spans.iter().any(|s| s.name == "console push"));
}

#[tokio::test]
async fn actions_reach_dispatch_and_the_broker_in_their_traces_or_are_refused() {
    let (_guard, _spans) = capture();
    let s = setup(Duration::from_secs(1)).await;
    let filters = ["sim/trucks/+/breakdown", "plants/+/commands/sync"];
    let mut commands = listen(&s.mqtt_url, &filters).await;
    until("the console's MQTT link", || s.console.mqtt_up()).await;
    until("the edge", || {
        s.console.snapshot().edge.is_some_and(|e| e.reachable)
    })
    .await;

    let done = s.console.break_down("truck-07").await.unwrap();
    assert!(done.trace_id.is_some());
    let (topic, _) = timeout(Duration::from_secs(10), commands.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(topic, "sim/trucks/truck-07/breakdown");
    let pending = s.console.break_down("truck-07").await.unwrap_err();
    assert_eq!(pending.status, StatusCode::CONFLICT, "{}", pending.message);
    let unknown = s.console.break_down("truck-99").await.unwrap_err();
    assert_eq!(unknown.status, StatusCode::BAD_REQUEST);
    s.bus
        .publish(&status(
            "truck-07",
            TruckStatus::OutOfService,
            domain::clock::now(),
        ))
        .await
        .unwrap();
    until("the breakdown", || {
        s.console
            .snapshot()
            .trucks
            .iter()
            .any(|t| t.id == "truck-07" && t.status == TruckStatus::OutOfService)
    })
    .await;
    let out = s.console.break_down("truck-07").await.unwrap_err();
    assert!(
        out.message.contains("already out of service"),
        "{}",
        out.message
    );

    let done = s.console.sync_plant("plant-walldorf").await.unwrap();
    let (topic, payload) = timeout(Duration::from_secs(10), commands.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(topic, "plants/plant-walldorf/commands/sync");
    assert!(
        payload.contains(done.trace_id.as_deref().unwrap()),
        "{payload}"
    );
    s.fake().agent_mqtt = false;
    until("the agent's link to go down", || {
        let snapshot = s.console.snapshot();
        let edge = snapshot.edge.unwrap();
        edge.status.unwrap()["mqtt"] == false
    })
    .await;
    let refused = s.console.sync_plant("plant-walldorf").await.unwrap_err();
    assert_eq!(refused.status, StatusCode::CONFLICT, "{}", refused.message);

    let done = s.console.delay_site("50001012", 30).await.unwrap();
    let delay = s.seen("POST", "/sites/50001012/delays").unwrap();
    let trace = done.trace_id.unwrap();
    assert!(delay.traceparent.unwrap().contains(&trace));
    let done = s.console.replan().await.unwrap();
    assert_eq!(done.version, Some(1));

    let log = s.console.snapshot().log;
    assert_eq!(log[0].action, "Re-plan");
    assert_eq!(log[0].outcome, "plan v1");
    assert!(
        log.iter()
            .any(|e| e.action == "Delay" && e.trace_id.as_deref() == Some(&trace))
    );
    assert!(
        log.iter()
            .any(|e| e.action == "Sync now" && e.outcome.contains("link is down"))
    );
}

#[tokio::test]
async fn make_chaos_orders_waits_for_the_plan_cuts_and_reports_what_waited() {
    let (_guard, _spans) = capture();
    let s = setup(Duration::from_secs(1)).await;
    until("the uplink reset and the edge", || {
        s.fake().resets > 0 && s.console.snapshot().edge.is_some_and(|e| e.reachable)
    })
    .await;

    let done = s.console.make_chaos().unwrap();
    let run = || s.console.snapshot().uplink.run.unwrap();
    until("the run to end", || !run().active()).await;
    let run = run();
    assert_eq!(run.step, Step::Done, "{:?}", run.report);
    assert!(done.trace_id.is_some());
    assert_eq!(run.trace_id, done.trace_id, "the run is the action's trace");
    assert_eq!(run.orders, s.fake().orders, "three orders near Walldorf");
    assert_eq!(run.orders.len(), 3);
    assert_eq!(run.plan_version, Some(1));
    assert_eq!(run.held, Some(2));
    let report = run.report.unwrap();
    assert!(
        report.starts_with("2 tickets waited out the cut"),
        "{report}"
    );
    let patches = s.fake().patches.clone();
    assert_eq!(
        patches,
        [
            ("dispatch".to_owned(), false),
            ("mqtt".to_owned(), false),
            ("dispatch".to_owned(), true),
            ("mqtt".to_owned(), true),
        ]
    );
}

#[tokio::test]
async fn reconnect_ends_the_cut_early_and_a_second_run_waits_its_turn() {
    let s = setup(Duration::from_secs(600)).await;
    until("the uplink reset and the edge", || {
        s.fake().resets > 0 && s.console.snapshot().edge.is_some_and(|e| e.reachable)
    })
    .await;

    s.console.make_chaos().unwrap();
    let step = || s.console.snapshot().uplink.run.unwrap().step;
    until("the cut", || step() == Step::Cut && s.fake().cut()).await;
    let twice = s.console.make_chaos().unwrap_err();
    assert_eq!(twice.status, StatusCode::CONFLICT);
    let reconnected = Instant::now();
    s.console.reconnect().await.unwrap();
    until("the run to end", || step() == Step::Done).await;
    assert!(reconnected.elapsed() < Duration::from_secs(30));
    assert!(!s.fake().cut(), "the uplink is back");
}
