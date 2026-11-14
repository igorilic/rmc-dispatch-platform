//! The console's HTTP side: the page, its live stream, its snapshot, its popups and its actions.
//!
//! Everything sits under `/console`, the path the cluster's ingress gives the console; Traefik
//! strips no prefix. `/healthz` sits at the root, for the probes. A request that changes anything
//! needs the `X-Pourline-Console` header, which another site can't send without a CORS preflight
//! the console never answers, and a `Host` named by `PUBLIC_URL`, which stops DNS rebinding.
//! Reads are open: they serve simulated data.
//!
//! The live stream is server-sent events: `retry:` and a `snapshot` first, then one event per
//! change (`truck`, `plant`, `plan`, `edge`, `uplink`, `log`) and a `heartbeat` every 15 s. A
//! page that falls behind is dropped; its browser reconnects and gets a new snapshot.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, Request, State};
use axum::http::{Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post, put};
use domain::PlantStatus;
use futures::{Stream, StreamExt, stream};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::actions::{Outcome, Refused};
use crate::{Console, metrics};

/// The header every change needs.
pub const ACTION_HEADER: &str = "x-pourline-console";

/// How soon a browser reconnects after the stream ends.
const RETRY: Duration = Duration::from_secs(3);

const INDEX: &str = include_str!("../web/index.html");
const CSS: &str = include_str!("../web/app.css");
const JS: &str = include_str!("../web/app.js");

pub fn router(console: Arc<Console>) -> Router {
    let hosts = Arc::new(allowed_hosts(&console.settings.public_url));
    let console_routes = Router::new()
        .route(
            "/console",
            get(|| async { Redirect::permanent("/console/") }),
        )
        .route(
            "/console/",
            get(|| asset("text/html; charset=utf-8", INDEX)),
        )
        .route("/console/app.css", get(|| asset("text/css", CSS)))
        .route("/console/app.js", get(|| asset("text/javascript", JS)))
        .route("/console/events", get(events))
        .route("/console/api/snapshot", get(snapshot))
        .route("/console/api/plants/{id}/plan", get(plant_plan))
        .route("/console/api/sites/{id}/orders", get(site_orders))
        .route("/console/api/trucks/{id}/breakdown", post(break_down))
        .route("/console/api/sites/{id}/delay", post(delay_site))
        .route("/console/api/plants/{id}/status", put(plant_status))
        .route("/console/api/plants/{id}/sync", post(sync_plant))
        .route("/console/api/plans", post(replan))
        .route("/console/api/chaos", post(make_chaos).delete(reconnect))
        .layer(middleware::from_fn_with_state(hosts, guard))
        .with_state(console);
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .merge(console_routes)
}

/// The `Host` values a change may come with: `PUBLIC_URL`'s, and for `localhost` also
/// `127.0.0.1` with the same port, and the other way round.
pub fn allowed_hosts(public_url: &str) -> Vec<String> {
    let rest = public_url
        .split_once("://")
        .map_or(public_url, |(_, rest)| rest);
    let host = rest.split('/').next().unwrap_or_default().to_owned();
    let twin = if let Some(port) = host.strip_prefix("localhost") {
        Some(format!("127.0.0.1{port}"))
    } else {
        host.strip_prefix("127.0.0.1")
            .map(|port| format!("localhost{port}"))
    };
    std::iter::once(host).chain(twin).collect()
}

async fn guard(State(hosts): State<Arc<Vec<String>>>, request: Request, next: Next) -> Response {
    if matches!(*request.method(), Method::GET | Method::HEAD) {
        return next.run(request).await;
    }
    let headers = request.headers();
    let host = headers.get(header::HOST).and_then(|h| h.to_str().ok());
    if !headers.contains_key(ACTION_HEADER) || !host.is_some_and(|h| hosts.iter().any(|a| a == h)) {
        let error = json!({ "error": "changes need the console's own page" });
        return (StatusCode::FORBIDDEN, Json(error)).into_response();
    }
    next.run(request).await
}

async fn asset(content_type: &'static str, body: &'static str) -> Response {
    (
        [
            (header::CONTENT_TYPE, content_type),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        body,
    )
        .into_response()
}

/// Counts a connected page while its stream lives.
struct Connected;

impl Connected {
    fn new() -> Connected {
        metrics::client_connected();
        Connected
    }
}

impl Drop for Connected {
    fn drop(&mut self) {
        metrics::client_gone();
    }
}

async fn events(
    State(console): State<Arc<Console>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let (snapshot, updates) = console.subscribe();
    let first = Event::default()
        .event("snapshot")
        .retry(RETRY)
        .json_data(&snapshot)
        .unwrap_or_else(|_| Event::default().event("snapshot").retry(RETRY).data("{}"));
    let changes = stream::unfold(
        (updates, Connected::new()),
        |(mut updates, connected)| async move {
            let update = updates.recv().await.ok()?;
            let event = Event::default().event(update.event).data(&*update.data);
            Some((event, (updates, connected)))
        },
    );
    Sse::new(stream::once(async { first }).chain(changes).map(Ok))
}

async fn snapshot(State(console): State<Arc<Console>>) -> Response {
    Json(console.snapshot()).into_response()
}

fn refusal(r: Refused) -> Response {
    (r.status, Json(json!({ "error": r.message }))).into_response()
}

fn answer(status: StatusCode, outcome: Outcome) -> Response {
    match outcome {
        Ok(done) => (status, Json(done)).into_response(),
        Err(r) => refusal(r),
    }
}

fn data(read: Result<Value, Refused>) -> Response {
    match read {
        Ok(value) => Json(value).into_response(),
        Err(r) => refusal(r),
    }
}

async fn plant_plan(State(console): State<Arc<Console>>, Path(id): Path<String>) -> Response {
    data(console.plant_plan(&id).await)
}

async fn site_orders(State(console): State<Arc<Console>>, Path(id): Path<String>) -> Response {
    data(console.site_orders(&id).await)
}

async fn break_down(State(console): State<Arc<Console>>, Path(id): Path<String>) -> Response {
    answer(StatusCode::ACCEPTED, console.break_down(&id).await)
}

#[derive(Deserialize)]
struct Delay {
    minutes: u32,
}

async fn delay_site(
    State(console): State<Arc<Console>>,
    Path(id): Path<String>,
    body: Bytes,
) -> Response {
    // The page always says 30 minutes; a bare POST means the same.
    let minutes = serde_json::from_slice::<Delay>(&body).map_or(30, |d| d.minutes);
    answer(StatusCode::OK, console.delay_site(&id, minutes).await)
}

#[derive(Deserialize)]
struct StatusChange {
    status: PlantStatus,
}

async fn plant_status(
    State(console): State<Arc<Console>>,
    Path(id): Path<String>,
    Json(change): Json<StatusChange>,
) -> Response {
    answer(
        StatusCode::OK,
        console.set_plant_status(&id, change.status).await,
    )
}

async fn sync_plant(State(console): State<Arc<Console>>, Path(id): Path<String>) -> Response {
    answer(StatusCode::ACCEPTED, console.sync_plant(&id).await)
}

async fn replan(State(console): State<Arc<Console>>) -> Response {
    answer(StatusCode::OK, console.replan().await)
}

async fn make_chaos(State(console): State<Arc<Console>>) -> Response {
    answer(StatusCode::ACCEPTED, console.make_chaos())
}

async fn reconnect(State(console): State<Arc<Console>>) -> Response {
    answer(StatusCode::OK, console.reconnect().await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Settings, Webhook};
    use axum::body::Body;
    use domain::PlantId;
    use http_body_util::BodyExt;
    use rumqttc::{AsyncClient, MqttOptions};
    use tower::ServiceExt;

    /// A console that isn't connected to anything, which is all the guards need.
    fn console() -> Arc<Console> {
        let webhook = Webhook {
            url: "http://127.0.0.1:9/".into(),
            token: String::new(),
        };
        let settings = Settings {
            dispatch_url: "http://127.0.0.1:9".into(),
            plant: PlantId("plant-walldorf".into()),
            plant_agent_url: None,
            uplink_url: None,
            cut: Duration::from_secs(120),
            poll: Duration::from_secs(2),
            public_url: "http://localhost:8080".into(),
            bauwerk: webhook.clone(),
            neckar: webhook,
        };
        let (mqtt, _) = AsyncClient::new(MqttOptions::new("test", "127.0.0.1", 9), 10);
        Console::new(settings, mqtt).unwrap()
    }

    async fn status(request: Request<Body>) -> StatusCode {
        router(console()).oneshot(request).await.unwrap().status()
    }

    fn post(uri: &str, host: &str, header: bool) -> Request<Body> {
        let mut request = axum::http::Request::post(uri).header(header::HOST, host);
        if header {
            request = request.header(ACTION_HEADER, "1");
        }
        request.body(Body::empty()).unwrap()
    }

    #[test]
    fn the_hosts_come_from_the_public_url() {
        assert_eq!(
            allowed_hosts("http://localhost:8080"),
            ["localhost:8080", "127.0.0.1:8080"]
        );
        assert_eq!(
            allowed_hosts("https://console.example.org/"),
            ["console.example.org"]
        );
    }

    #[tokio::test]
    async fn a_change_needs_the_header_and_an_allowed_host() {
        let breakdown = "/console/api/trucks/truck-07/breakdown";
        assert_eq!(
            status(post(breakdown, "localhost:8080", false)).await,
            StatusCode::FORBIDDEN,
            "no header"
        );
        assert_eq!(
            status(post(breakdown, "evil.example:8080", true)).await,
            StatusCode::FORBIDDEN,
            "a rebound host"
        );
        assert_eq!(
            status(post(
                "/console/api/trucks/truck-99/breakdown",
                "127.0.0.1:8080",
                true
            ))
            .await,
            StatusCode::BAD_REQUEST,
            "past the guard, an unknown truck"
        );
    }

    #[tokio::test]
    async fn reads_and_probes_need_neither() {
        for uri in ["/healthz", "/console/api/snapshot", "/console/"] {
            let request = axum::http::Request::get(uri)
                .header(header::HOST, "10.42.0.7:8083")
                .body(Body::empty())
                .unwrap();
            assert_eq!(status(request).await, StatusCode::OK, "{uri}");
        }
    }

    #[tokio::test]
    async fn the_stream_starts_with_retry_and_the_snapshot() {
        let request = axum::http::Request::get("/console/events")
            .body(Body::empty())
            .unwrap();
        let response = router(console()).oneshot(request).await.unwrap();
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/event-stream"
        );
        let mut body = response.into_body();
        let frame = body.frame().await.unwrap().unwrap();
        let text = String::from_utf8(frame.into_data().unwrap().to_vec()).unwrap();
        assert!(text.starts_with("event: snapshot\n"), "{text}");
        assert!(text.contains("retry: 3000\n"), "{text}");
        let data = text.lines().find_map(|l| l.strip_prefix("data: ")).unwrap();
        let snapshot: Value = serde_json::from_str(data).unwrap();
        assert_eq!(snapshot["trucks"].as_array().unwrap().len(), 25);
        assert_eq!(snapshot["features"]["chaos"], false);
    }
}
