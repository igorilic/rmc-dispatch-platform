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
    PlantStatus, TruckId, TruckStatus,
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
    /// Make chaos's orders, once posted: dispatch plans them after a few polls.
    planning: Option<(Vec<Value>, usize)>,
    /// Polls of `/status` left before it reports the newest plan: the agent syncs late.
    status_lag: usize,
    /// The agent's link to the broker, as its `/status` reports it, while the uplink is up.
    agent_mqtt: bool,
    /// Whether `/status` also reports the links down while the uplink is cut.
    links_follow_cut: bool,
    proxies: BTreeMap<String, bool>,
    resets: usize,
    patches: Vec<(String, bool)>,
    /// What happened, in order, for the steps of Make chaos.
    timeline: Vec<&'static str>,
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
            links_follow_cut: true,
            proxies: [("dispatch".to_owned(), true), ("mqtt".to_owned(), true)].into(),
            ..Fake::default()
        }
    }

    fn cut(&self) -> bool {
        self.proxies.values().any(|enabled| !enabled)
    }

    /// The agent's `/status`: tickets wait in the outbox while the uplink is cut, and it reports
    /// the newest plan once its lag is over.
    fn status(&mut self) -> Value {
        let newest = self.plans.last().map(|p| p["version"].clone());
        let version = if self.status_lag > 0 {
            self.status_lag -= 1;
            self.plans.iter().rev().nth(1).map(|p| p["version"].clone())
        } else {
            if newest.is_some() && !self.timeline.contains(&"agent holds the plan") {
                self.timeline.push("agent holds the plan");
            }
            newest
        };
        let down = self.links_follow_cut && self.cut();
        json!({
            "plant": "plant-walldorf",
            "mqtt": self.agent_mqtt && !down,
            "dispatch": !down,
            "outbox": if self.cut() { 2 } else { 0 },
            "sync_lag_seconds": 0,
            "plan_version": version,
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

    /// `/plans/latest`, where Make chaos's orders show up a few polls after they were posted.
    fn latest(&mut self) -> Option<Value> {
        if let Some((loads, polls)) = self.planning.take() {
            if polls > 0 {
                self.planning = Some((loads, polls - 1));
            } else {
                self.plan(loads);
                self.timeline.push("dispatch planned the orders");
                self.status_lag = 3;
            }
        }
        self.plans.last().cloned()
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
    let missing = || (StatusCode::NOT_FOUND, Json(Value::Null));
    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    match (method.as_str(), segments.as_slice()) {
        ("GET", ["world"]) => {
            // Like dispatch, the newest version it holds.
            let mut world = f.world.clone();
            world["plan_version"] = f.plans.last().map_or(Value::Null, |p| p["version"].clone());
            ok(world)
        }
        ("GET", ["plans", "latest"]) => f.latest().map_or_else(missing, ok),
        ("GET", ["plans", version, "diff"]) => {
            let version: usize = version.parse().unwrap();
            match version {
                // The first version has no diff.
                1 => missing(),
                v if v <= f.plans.len() => ok(json!({ "triggers": ["test"], "changes": [] })),
                _ => missing(),
            }
        }
        ("GET", ["plans", version]) => {
            let version: usize = version.parse().unwrap();
            f.plans
                .get(version.wrapping_sub(1))
                .cloned()
                .map_or_else(missing, ok)
        }
        ("POST", ["plans"]) => {
            let plan = f.plan(Vec::new());
            (StatusCode::CREATED, Json(plan))
        }
        ("PUT", ["plants", _, "status"]) | ("POST", ["sites", _, "delays"]) => {
            ok(json!({ "version": null, "diff": null }))
        }
        ("GET", ["plants", plant, "plan"]) => {
            ok(json!({ "plant": plant, "version": 1, "loads": [] }))
        }
        ("GET", ["orders"]) => ok(json!([
            { "id": "10001001/BW-1", "site": "50001012" },
            { "id": "10001001/BW-2", "site": "50001007" },
        ])),
        ("POST", ["webhooks", integration]) => {
            let customer = match *integration {
                "bauwerk-ag" => "10001001",
                _ => "10001003",
            };
            let at = body.find("EDGE-").unwrap();
            f.orders.push(format!("{customer}/{}", &body[at..at + 13]));
            if f.orders.len() % 3 == 0 {
                f.timeline.push("orders placed");
                let loads = f.orders[f.orders.len() - 3..]
                    .iter()
                    .map(|order| json!({ "order": order, "planned_plant": "plant-walldorf" }))
                    .collect();
                f.planning = Some((loads, 2));
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
            if !enabled && !f.timeline.contains(&"uplink cut") {
                f.timeline.push("uplink cut");
            }
            f.proxies.insert((*name).to_owned(), enabled);
            f.patches.push(((*name).to_owned(), enabled));
            ok(json!({ "name": name, "enabled": enabled }))
        }
        ("POST", ["reset"]) => {
            f.resets += 1;
            f.proxies.values_mut().for_each(|enabled| *enabled = true);
            (StatusCode::NO_CONTENT, Json(Value::Null))
        }
        _ => missing(),
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

    fn seen(&self, method: &str, path: &str) -> Vec<Seen> {
        self.fake()
            .seen
            .iter()
            .filter(|s| s.method == method && s.path == path)
            .cloned()
            .collect()
    }

    fn truck(&self, id: &str) -> console::state::TruckView {
        let snapshot = self.console.snapshot();
        snapshot.trucks.into_iter().find(|t| t.id == id).unwrap()
    }

    /// Announces `version` on `plans.updated` in `span`'s trace until the console has it.
    fn announce(&self, version: i64, span: &tracing::Span) -> tokio::task::JoinHandle<()> {
        let mut headers = async_nats::HeaderMap::new();
        span.in_scope(|| telemetry::inject(&mut headers));
        let announcement = serde_json::to_vec(&PlanUpdated {
            version,
            created_at: at(0),
        })
        .unwrap();
        let nats = self.nats.clone();
        // Core NATS drops what comes before the console subscribes, so announce until it hears.
        tokio::spawn(async move {
            loop {
                let payload = announcement.clone().into();
                nats.publish_with_headers(PLANS_UPDATED, headers.clone(), payload)
                    .await
                    .unwrap();
                sleep(Duration::from_millis(100)).await;
            }
        })
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

async fn ready_for_chaos(s: &Setup) {
    until("the uplink reset and the edge", || {
        s.fake().resets > 0 && s.console.snapshot().edge.is_some_and(|e| e.reachable)
    })
    .await;
}

#[tokio::test]
async fn a_page_gets_the_world_then_live_events_and_the_newest_report_wins() {
    let s = setup(Duration::from_secs(1)).await;
    s.fake().world["trucks"] = json!([{
        "id": "truck-07", "status": "en_route", "status_at": at(1),
        "position": null, "position_at": null,
    }]);
    s.console.resync();
    until("/world's report", || {
        s.truck("truck-07").status == TruckStatus::EnRoute
    })
    .await;

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

    // A /world that hasn't caught up on truck-07 changes nothing for it. Its newer report on
    // truck-08 shows the merge ran.
    s.fake().world["trucks"] = json!([
        { "id": "truck-07", "status": "en_route", "status_at": at(1), "position": null, "position_at": null },
        { "id": "truck-08", "status": "returning", "status_at": at(4), "position": null, "position_at": null },
    ]);
    s.console.resync();
    let merged = next(&mut updates, "truck", |t| t["id"] == "truck-08").await;
    assert_eq!(merged["status"], "returning");
    assert_eq!(s.truck("truck-07").status, TruckStatus::OutOfService);
}

#[tokio::test]
async fn each_plan_version_is_pushed_once_in_the_trace_that_made_it() {
    let (_guard, exporter) = capture();
    let s = setup(Duration::from_secs(1)).await;
    // After the startup read of /world, so the version comes only through its announcement.
    until("the startup read", || !s.seen("GET", "/world").is_empty()).await;
    s.fake().plan(Vec::new());
    let (_, mut updates) = s.console.subscribe();
    let redispatch = info_span!("redispatch");
    let trace = telemetry::trace_id(&redispatch).unwrap();
    let announcing = s.announce(1, &redispatch);

    let plan = next(&mut updates, "plan", |_| true).await;
    assert_eq!(plan["version"], 1);
    assert_eq!(plan["trace_id"], trace.as_str());
    assert_eq!(plan["diff"], Value::Null, "the first version has no diff");
    let diff = s.seen("GET", "/plans/1/diff");
    assert!(
        diff[0].traceparent.as_ref().unwrap().contains(&trace),
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
async fn a_version_dispatch_doesnt_confirm_is_never_pushed() {
    let (_guard, _spans) = capture();
    let s = setup(Duration::from_secs(1)).await;
    let (_, mut updates) = s.console.subscribe();
    // Dispatch has no version 7, so its diff and the plan itself are 404.
    let announcing = s.announce(7, &info_span!("someone"));
    until("the console to ask", || {
        !s.seen("GET", "/plans/7").is_empty()
    })
    .await;
    let pushed = timeout(
        Duration::from_millis(500),
        next(&mut updates, "plan", |_| true),
    )
    .await;
    assert!(pushed.is_err(), "nothing confirmed, nothing pushed");
    assert!(s.console.snapshot().plan.is_none());
    announcing.abort();
}

#[tokio::test]
async fn a_version_missed_on_nats_is_caught_up_without_a_borrowed_trace() {
    let (_guard, _spans) = capture();
    let s = setup(Duration::from_secs(1)).await;
    {
        let mut fake = s.fake();
        fake.plan(Vec::new());
        fake.plan(Vec::new());
    }
    let (_, mut updates) = s.console.subscribe();
    let first = info_span!("redispatch v1");
    let announcing = s.announce(1, &first);
    let caught_up = next(&mut updates, "plan", |p| p["version"] == 2).await;
    announcing.abort();
    assert_eq!(caught_up["trace_id"], Value::Null, "v1's trace isn't v2's");

    // v2's own announcement, when it comes, brings its trace.
    let second = info_span!("redispatch v2");
    let trace = telemetry::trace_id(&second).unwrap();
    let announcing = s.announce(2, &second);
    let traced = next(&mut updates, "plan", |p| p["trace_id"] == trace.as_str()).await;
    assert_eq!(traced["version"], 2);
    announcing.abort();
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
        s.truck("truck-07").status == TruckStatus::OutOfService
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
    let other = s.console.sync_plant("plant-mannheim").await.unwrap_err();
    assert_eq!(other.status, StatusCode::BAD_REQUEST, "no agent there");
    s.fake().agent_mqtt = false;
    until("the agent's link to go down", || {
        let edge = s.console.snapshot().edge.unwrap();
        edge.status.unwrap()["mqtt"] == false
    })
    .await;
    let refused = s.console.sync_plant("plant-walldorf").await.unwrap_err();
    assert_eq!(refused.status, StatusCode::CONFLICT, "{}", refused.message);

    let done = s.console.delay_site("50001012", 30).await.unwrap();
    let delay = &s.seen("POST", "/sites/50001012/delays")[0];
    let trace = done.trace_id.unwrap();
    assert!(delay.traceparent.as_ref().unwrap().contains(&trace));
    let unknown = s.console.delay_site("50009999", 30).await.unwrap_err();
    assert_eq!(unknown.status, StatusCode::BAD_REQUEST);

    let done = s
        .console
        .set_plant_status("plant-weinheim", PlantStatus::Offline)
        .await
        .unwrap();
    let put = &s.seen("PUT", "/plants/plant-weinheim/status")[0];
    assert!(
        put.traceparent
            .as_ref()
            .unwrap()
            .contains(done.trace_id.as_deref().unwrap())
    );
    let snapshot = s.console.snapshot();
    let weinheim = snapshot
        .plants
        .iter()
        .find(|p| p.id == "plant-weinheim")
        .unwrap();
    assert_eq!(
        weinheim.status,
        PlantStatus::Offline,
        "an empty diff still shows"
    );

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
async fn sync_is_refused_while_the_uplink_is_cut_even_if_the_agent_looks_connected() {
    let s = setup(Duration::from_secs(1)).await;
    ready_for_chaos(&s).await;
    {
        let mut fake = s.fake();
        fake.links_follow_cut = false;
        fake.proxies.insert("mqtt".into(), false);
    }
    until("the cut", || s.console.snapshot().uplink.cut).await;
    until("the edge", || {
        s.console.snapshot().edge.is_some_and(|e| e.reachable)
    })
    .await;
    let refused = s.console.sync_plant("plant-walldorf").await.unwrap_err();
    assert_eq!(refused.status, StatusCode::CONFLICT, "{}", refused.message);
    let chaos = s.console.make_chaos().unwrap_err();
    assert!(chaos.message.contains("cut already"), "{}", chaos.message);

    // Reconnect brings back an uplink that is cut without a run.
    s.console.reconnect().await.unwrap();
    until("the uplink back", || !s.console.snapshot().uplink.cut).await;
    let nothing = s.console.reconnect().await.unwrap_err();
    assert_eq!(nothing.status, StatusCode::CONFLICT, "{}", nothing.message);
}

#[tokio::test]
async fn the_popups_read_dispatch() {
    let s = setup(Duration::from_secs(1)).await;
    let plan = s.console.plant_plan("plant-walldorf").await.unwrap();
    assert_eq!(plan["plant"], "plant-walldorf");
    let orders = s.console.site_orders("50001012").await.unwrap();
    assert_eq!(
        orders,
        json!([{ "id": "10001001/BW-1", "site": "50001012" }])
    );
    let asked = &s
        .fake()
        .seen
        .iter()
        .find(|q| q.path == "/orders")
        .cloned()
        .unwrap();
    assert_eq!(asked.method, "GET");
    assert!(s.console.plant_plan("plant-nowhere").await.is_err());
}

#[tokio::test]
async fn make_chaos_orders_waits_for_the_plan_and_the_plant_then_cuts_and_reports() {
    let (_guard, _spans) = capture();
    let s = setup(Duration::from_secs(1)).await;
    ready_for_chaos(&s).await;
    let resets = s.fake().resets;

    let done = s.console.make_chaos().unwrap();
    let trace = done.trace_id.clone().unwrap();
    let run = || s.console.snapshot().uplink.run.unwrap();
    until("the run to end", || !run().active()).await;
    let run = run();
    assert_eq!(run.step, Step::Done, "{:?}", run.report);
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

    assert_eq!(
        s.fake().timeline,
        [
            "orders placed",
            "dispatch planned the orders",
            "agent holds the plan",
            "uplink cut"
        ],
        "the cut waits for the plan and for the plant to hold it"
    );
    let patches = s.fake().patches.clone();
    assert_eq!(
        patches,
        [("dispatch".to_owned(), false), ("mqtt".to_owned(), false)]
    );
    assert_eq!(
        s.fake().resets,
        resets + 1,
        "the uplink came back through a reset"
    );
    let polls = s.seen("GET", "/plans/latest");
    assert!(!polls.is_empty());
    assert!(
        polls
            .iter()
            .all(|p| p.traceparent.as_ref().is_some_and(|tp| tp.contains(&trace))),
        "the run's calls are in the action's trace"
    );
}

#[tokio::test]
async fn reconnect_ends_the_cut_early_and_leaves_nothing_for_the_next_run() {
    let s = setup(Duration::from_secs(600)).await;
    ready_for_chaos(&s).await;
    let step = || s.console.snapshot().uplink.run.unwrap().step;

    s.console.make_chaos().unwrap();
    until("the cut", || step() == Step::Cut && s.fake().cut()).await;
    let twice = s.console.make_chaos().unwrap_err();
    assert_eq!(twice.status, StatusCode::CONFLICT);
    let reconnected = Instant::now();
    s.console.reconnect().await.unwrap();
    until("the run to end", || step() == Step::Done).await;
    assert!(reconnected.elapsed() < Duration::from_secs(30));
    assert!(!s.fake().cut(), "the uplink is back");
    let late = s.console.reconnect().await.unwrap_err();
    assert_eq!(late.status, StatusCode::CONFLICT, "{}", late.message);

    // A second run keeps its cut: no click from before is left over to end it.
    s.console.make_chaos().unwrap();
    until("the second cut", || step() == Step::Cut && s.fake().cut()).await;
    sleep(Duration::from_secs(1)).await;
    assert_eq!(step(), Step::Cut);
    assert!(s.fake().cut());
    s.console.reconnect().await.unwrap();
    until("the second run to end", || step() == Step::Done).await;
}
