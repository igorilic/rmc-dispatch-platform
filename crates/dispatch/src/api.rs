//! The dispatch REST API. Its OpenAPI document is at `/api-docs/openapi.json`, and Swagger UI at
//! `/swagger-ui/`.

use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use chrono_tz::Europe::Berlin;
use domain::{Order, OrderStatus, PlantId, PlantPlan, PlantStatus, SiteId};
use serde::{Deserialize, Serialize};
use tracing::error;
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

use crate::redispatch::{Diff, Trigger};
pub use crate::store::VersionedPlan;
use crate::store::{Store, StoreError};
use crate::worker::Jobs;

#[derive(Clone)]
struct AppState {
    store: Store,
    jobs: Jobs,
    clock: fn() -> DateTime<Utc>,
}

/// What a re-dispatch did: the new version and how it differs from the one before. Both are
/// `null` when nothing changed, so no version was stored.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Redispatched {
    pub version: Option<i64>,
    pub diff: Option<Diff>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct PlantStatusChange {
    pub status: PlantStatus,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct SiteDelay {
    /// How many minutes later the site's pours start: 1 to 1,440.
    pub minutes: u32,
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Pourline dispatch",
        description = "Orders and truck-load plans."
    ),
    paths(
        healthz,
        list_orders,
        get_order,
        create_plan,
        latest_plan,
        get_plan,
        get_diff,
        plant_plan,
        set_plant_status,
        delay_site
    )
)]
struct ApiDoc;

/// The API's routes. Planning goes through the re-dispatch worker behind `jobs`. `clock` says
/// when "today" ends for site delays; the service passes `Utc::now`.
pub fn router(store: Store, jobs: Jobs, clock: fn() -> DateTime<Utc>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/orders", get(list_orders))
        .route("/orders/{customer}/{order_ref}", get(get_order))
        .route("/plans", post(create_plan))
        .route("/plans/latest", get(latest_plan))
        .route("/plans/{version}", get(get_plan))
        .route("/plans/{version}/diff", get(get_diff))
        .route("/plants/{id}/plan", get(plant_plan))
        .route("/plants/{id}/status", put(set_plant_status))
        .route("/sites/{id}/delays", post(delay_site))
        .with_state(AppState { store, jobs, clock })
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
}

/// Logs the cause and answers 500 without it.
fn internal(e: StoreError) -> StatusCode {
    error!(error = %e, "store failed");
    StatusCode::INTERNAL_SERVER_ERROR
}

#[utoipa::path(get, path = "/healthz", responses((status = 200, body = String)))]
async fn healthz() -> &'static str {
    "ok"
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct OrderFilter {
    status: Option<OrderStatus>,
    /// Requested start at or after this time.
    from: Option<DateTime<Utc>>,
    /// Requested start before this time.
    to: Option<DateTime<Utc>>,
}

/// Orders by requested start.
#[utoipa::path(get, path = "/orders", params(OrderFilter), responses(
    (status = 200, body = Vec<Order>),
    (status = 400, description = "Unknown status or malformed time"),
))]
async fn list_orders(
    State(s): State<AppState>,
    Query(f): Query<OrderFilter>,
) -> Result<Json<Vec<Order>>, StatusCode> {
    let orders = s.store.orders(f.status, f.from, f.to).await;
    Ok(Json(orders.map_err(internal)?))
}

/// One order, by our customer number and the customer's order number.
#[utoipa::path(get, path = "/orders/{customer}/{order_ref}",
    params(("customer" = String, Path, description = "Our customer number"),
           ("order_ref" = String, Path, description = "The customer's order number")),
    responses((status = 200, body = Order), (status = 404)))]
async fn get_order(
    State(s): State<AppState>,
    Path((customer, order_ref)): Path<(String, String)>,
) -> Result<Json<Order>, StatusCode> {
    let order = s.store.order(&format!("{customer}/{order_ref}")).await;
    order
        .map_err(internal)?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

/// Plans everything that isn't frozen again and stores it as a new version.
#[utoipa::path(post, path = "/plans", responses(
    (status = 201, body = VersionedPlan, headers(("location" = String))),
))]
#[tracing::instrument(name = "POST /plans", skip_all)]
async fn create_plan(State(s): State<AppState>) -> Result<impl IntoResponse, StatusCode> {
    let version = s.jobs.run(Trigger::FullReplan).await.map_err(|e| {
        error!(error = %e, "full re-plan failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    // A full re-plan always stores a version, so `None` can't happen.
    let found = s.store.plan(version).await.map_err(internal)?;
    let plan = found.ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
    let location = [(header::LOCATION, format!("/plans/{}", plan.version))];
    Ok((StatusCode::CREATED, location, Json(plan)))
}

#[utoipa::path(get, path = "/plans/latest", responses((status = 200, body = VersionedPlan), (status = 404)))]
async fn latest_plan(State(s): State<AppState>) -> Result<Json<VersionedPlan>, StatusCode> {
    stored_plan(&s.store, None).await
}

#[utoipa::path(get, path = "/plans/{version}", params(("version" = i64, Path)),
    responses((status = 200, body = VersionedPlan), (status = 404)))]
async fn get_plan(
    State(s): State<AppState>,
    Path(version): Path<i64>,
) -> Result<Json<VersionedPlan>, StatusCode> {
    stored_plan(&s.store, Some(version)).await
}

async fn stored_plan(
    store: &Store,
    version: Option<i64>,
) -> Result<Json<VersionedPlan>, StatusCode> {
    let found = store.plan(version).await.map_err(internal)?;
    Ok(Json(found.ok_or(StatusCode::NOT_FOUND)?))
}

/// What changed from the previous version, and why.
#[utoipa::path(get, path = "/plans/{version}/diff", params(("version" = i64, Path)),
    responses((status = 200, body = Diff), (status = 404)))]
async fn get_diff(
    State(s): State<AppState>,
    Path(version): Path<i64>,
) -> Result<Json<Diff>, StatusCode> {
    let found = s.store.diff(version).await.map_err(internal)?;
    found.flatten().map(Json).ok_or(StatusCode::NOT_FOUND)
}

/// A plant's slice of the latest plan: its loads by batch time, each with its mix. The
/// plant-agent fetches it to batch from. Before the first plan, `version` is `null` and there are
/// no loads.
#[utoipa::path(get, path = "/plants/{id}/plan", params(("id" = String, Path)),
    responses((status = 200, body = PlantPlan), (status = 404, description = "Unknown plant")))]
async fn plant_plan(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<PlantPlan>, StatusCode> {
    let found = s.store.plant_plan(&PlantId(id)).await.map_err(internal)?;
    Ok(Json(found.ok_or(StatusCode::NOT_FOUND)?))
}

/// Sets a plant online or offline, then re-dispatches.
#[utoipa::path(put, path = "/plants/{id}/status", params(("id" = String, Path)),
    request_body = PlantStatusChange,
    responses((status = 200, body = Redispatched), (status = 404, description = "Unknown plant")))]
#[tracing::instrument(name = "PUT /plants/{id}/status", skip_all, fields(plant = %id))]
async fn set_plant_status(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Json(change): Json<PlantStatusChange>,
) -> Result<Json<Redispatched>, StatusCode> {
    let plant = PlantId(id);
    let before = s.store.set_plant_status(&plant, change.status).await;
    let before = before.map_err(internal)?.ok_or(StatusCode::NOT_FOUND)?;
    if before == change.status {
        return Ok(Json(Redispatched {
            version: None,
            diff: None,
        }));
    }
    let trigger = match change.status {
        PlantStatus::Offline => Trigger::PlantOffline { plant },
        PlantStatus::Online => Trigger::PlantOnline { plant },
    };
    redispatch(&s, trigger).await
}

/// Starts a site's pours today some minutes later, as when the pump is late, then re-dispatches.
/// It applies to the site's orders that are batching or start today (Berlin time).
#[utoipa::path(post, path = "/sites/{id}/delays", params(("id" = String, Path, description = "Our ship-to number")),
    request_body = SiteDelay,
    responses(
        (status = 200, body = Redispatched),
        (status = 404, description = "Unknown site"),
        (status = 422, description = "Minutes outside 1 to 1,440"),
    ))]
#[tracing::instrument(name = "POST /sites/{id}/delays", skip_all, fields(site = %id))]
async fn delay_site(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Json(delay): Json<SiteDelay>,
) -> Result<Json<Redispatched>, StatusCode> {
    if !(1..=1440).contains(&delay.minutes) {
        return Err(StatusCode::UNPROCESSABLE_ENTITY);
    }
    let now = (s.clock)();
    let site = SiteId(id);
    let delayed = s
        .store
        .delay_site(&site, delay.minutes, now, day_end(now))
        .await;
    let delayed = delayed.map_err(internal)?.ok_or(StatusCode::NOT_FOUND)?;
    if delayed.is_empty() {
        return Ok(Json(Redispatched {
            version: None,
            diff: None,
        }));
    }
    let trigger = Trigger::SiteDelayed {
        site,
        minutes: delay.minutes,
        orders: delayed,
    };
    redispatch(&s, trigger).await
}

/// The next midnight in Berlin.
fn day_end(now: DateTime<Utc>) -> DateTime<Utc> {
    now.with_timezone(&Berlin)
        .date_naive()
        .succ_opt()
        .and_then(|day| {
            Berlin
                .from_local_datetime(&day.and_time(Default::default()))
                .earliest()
        })
        .map_or(now + TimeDelta::days(1), |t| t.with_timezone(&Utc))
}

async fn redispatch(s: &AppState, trigger: Trigger) -> Result<Json<Redispatched>, StatusCode> {
    let version = s.jobs.run(trigger).await.map_err(|e| {
        error!(error = %e, "re-dispatch failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let diff = match version {
        Some(v) => s.store.diff(v).await.map_err(internal)?.flatten(),
        None => None,
    };
    Ok(Json(Redispatched { version, diff }))
}
