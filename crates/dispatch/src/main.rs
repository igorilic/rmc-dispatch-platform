//! The dispatch service: order intake from NATS and the REST API. Settings come from environment
//! variables; `.env.example` lists the defaults.

use chrono::Utc;
use dispatch::store::Store;
use dispatch::{api, intake};
use domain::Seed;
use tracing::info;
use tracing_subscriber::EnvFilter;

fn var(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| default.to_owned())
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

    let store = Store::connect(&database_url).await?;
    store.import_master_data(&Seed::rhine_neckar()?).await?;
    info!("connected to Postgres, master data imported");
    // Connects in the background, so dispatch starts even while NATS is still coming up.
    let nats = async_nats::ConnectOptions::new()
        .retry_on_initial_connect()
        .connect(&nats_url)
        .await?;
    tokio::spawn(intake::run(async_nats::jetstream::new(nats), store.clone()));

    let listener = tokio::net::TcpListener::bind(&http_addr).await?;
    info!(addr = %http_addr, "API listening, Swagger UI at /swagger-ui/");
    axum::serve(listener, api::router(store, Utc::now))
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
