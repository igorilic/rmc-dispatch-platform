//! The dispatch REST API. Its OpenAPI document is at `/api-docs/openapi.json`, and Swagger UI at
//! `/swagger-ui/`.

use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use domain::{Order, OrderStatus};
use serde::Deserialize;
use tracing::error;
use utoipa::{IntoParams, OpenApi};
use utoipa_swagger_ui::SwaggerUi;

use crate::redispatch::Trigger;
pub use crate::store::VersionedPlan;
use crate::store::{Store, StoreError};
use crate::worker::Jobs;

#[derive(Clone)]
struct AppState {
    store: Store,
    jobs: Jobs,
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

/// The API's routes. Planning goes through the re-dispatch worker behind `jobs`.
pub fn router(store: Store, jobs: Jobs) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/orders", get(list_orders))
        .route("/orders/{customer}/{order_ref}", get(get_order))
        .route("/plans", post(create_plan))
        .route("/plans/latest", get(latest_plan))
        .route("/plans/{version}", get(get_plan))
        .with_state(AppState { store, jobs })
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
