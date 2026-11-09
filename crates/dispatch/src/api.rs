//! The dispatch REST API. Its OpenAPI document is at `/api-docs/openapi.json`, and Swagger UI at
//! `/swagger-ui/`.

use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, SubsecRound, TimeDelta, Utc};
use domain::{Order, OrderStatus};
use serde::{Deserialize, Serialize};
use tracing::{error, info};
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

use crate::planner::{self, Plan};
use crate::store::{Store, StoreError};
use crate::travel::HaversineTravelTime;

/// How far ahead `POST /plans` plans.
pub const HORIZON: TimeDelta = TimeDelta::hours(48);

#[derive(Clone)]
struct AppState {
    store: Store,
    clock: fn() -> DateTime<Utc>,
}

/// A stored plan and its version number.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct VersionedPlan {
    pub version: i64,
    #[serde(flatten)]
    pub plan: Plan,
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Pourline dispatch",
        description = "Orders and truck-load plans."
    ),
    paths(healthz, list_orders, get_order, create_plan, latest_plan, get_plan)
)]
struct ApiDoc;

/// The API's routes. `clock` gives the planning time; the service passes `Utc::now`.
pub fn router(store: Store, clock: fn() -> DateTime<Utc>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/orders", get(list_orders))
        .route("/orders/{customer}/{order_ref}", get(get_order))
        .route("/plans", post(create_plan))
        .route("/plans/latest", get(latest_plan))
        .route("/plans/{version}", get(get_plan))
        .with_state(AppState { store, clock })
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

/// Plans every open order starting in the next 48 hours and stores the plan as a new version.
#[utoipa::path(post, path = "/plans", responses(
    (status = 201, body = VersionedPlan, headers(("location" = String))),
))]
async fn create_plan(State(s): State<AppState>) -> Result<impl IntoResponse, StatusCode> {
    // Whole seconds, so the stored plan reads back exactly as it was made.
    let now = (s.clock)().trunc_subsecs(0);
    let world = s.store.world().await.map_err(internal)?;
    let orders = s
        .store
        .open_orders(now - TimeDelta::hours(1), now + HORIZON);
    let orders = orders.await.map_err(internal)?;
    // ponytail: plans on the async runtime; move to spawn_blocking if M5's bigger plans take long.
    let plan = planner::plan(
        &world,
        &orders,
        &[],
        now,
        HORIZON,
        &HaversineTravelTime::default(),
    );
    let version = s.store.save_plan(&plan).await.map_err(internal)?;
    info!(
        version,
        planned = plan.metrics.planned_orders,
        unplanned = plan.metrics.unplanned_orders,
        "plan saved"
    );
    let location = [(header::LOCATION, format!("/plans/{version}"))];
    Ok((
        StatusCode::CREATED,
        location,
        Json(VersionedPlan { version, plan }),
    ))
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
    let (version, plan) = found.ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(VersionedPlan { version, plan }))
}
