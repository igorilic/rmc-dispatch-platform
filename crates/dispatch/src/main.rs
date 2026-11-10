//! The dispatch service: event intake from NATS, the re-dispatch worker and the REST API.
//! Settings come from environment variables; `.env.example` lists the defaults.

use std::str::FromStr;
use std::time::Duration;

use anyhow::Context;
use chrono::Utc;
use dispatch::redispatch::Settings;
use dispatch::search::{Budget, Weights};
use dispatch::store::Store;
use dispatch::{api, intake, worker};
use domain::Seed;
use tracing::info;
use tracing_subscriber::EnvFilter;

fn var(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| default.to_owned())
}

fn number<T: FromStr>(key: &str, default: &str) -> anyhow::Result<T> {
    let value = var(key, default);
    value
        .parse()
        .ok()
        .with_context(|| format!("{key} must be a number, not {value:?}"))
}

/// Local search budget and weights (SPEC §4: weighted, configurable).
fn settings() -> anyhow::Result<Settings> {
    Ok(Settings {
        budget: Budget::time(Duration::from_millis(number(
            "LOCAL_SEARCH_BUDGET_MS",
            "500",
        )?)),
        weights: Weights {
            cold_joint_minute: number("WEIGHT_COLD_JOINT_MINUTE", "20")?,
            gap_minute: number("WEIGHT_GAP_MINUTE", "1")?,
            late_minute: number("WEIGHT_LATE_MINUTE", "2")?,
            empty_km: number("WEIGHT_EMPTY_KM", "0.5")?,
            churn_load: number("WEIGHT_CHURN_LOAD", "10")?,
        },
        ..Settings::default()
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let http_addr = var("DISPATCH_HTTP_ADDR", "0.0.0.0:8081");
    let database_url = var(
        "DATABASE_URL",
        "postgres://pourline:pourline@localhost:5432/pourline",
    );
    let nats_url = var("NATS_URL", "nats://localhost:4222");
    let settings = settings()?;

    let store = Store::connect(&database_url).await?;
    store.import_master_data(&Seed::rhine_neckar()?).await?;
    info!("connected to Postgres, master data imported");
    // Connects in the background, so dispatch starts even while NATS is still coming up.
    let nats = async_nats::ConnectOptions::new()
        .retry_on_initial_connect()
        .connect(&nats_url)
        .await?;
    let (jobs, queue) = worker::channel();
    tokio::spawn(worker::run(store.clone(), queue, settings, Utc::now));
    let js = async_nats::jetstream::new(nats);
    tokio::spawn(intake::run(js.clone(), store.clone(), jobs.clone()));
    tokio::spawn(intake::run_fleet(js, store.clone(), jobs.clone()));

    let listener = tokio::net::TcpListener::bind(&http_addr).await?;
    info!(addr = %http_addr, "API listening, Swagger UI at /swagger-ui/");
    axum::serve(listener, api::router(store, jobs, Utc::now))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    info!("shutting down");
    Ok(())
}

/// Docker stops containers with SIGTERM, which a PID-1 process ignores unless it handles it.
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    match signal(SignalKind::terminate()) {
        Ok(mut terminate) => tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        },
        Err(_) => {
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}
