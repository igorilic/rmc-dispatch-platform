//! The plant-agent service: plan sync, the batch computer, the outbox forwarder, and the health,
//! status and metrics endpoints. All settings come from environment variables; see `Config`.

use std::fmt::Display;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, ensure};
use domain::Seed;
use plant_agent::forward::{self, Settings};
use plant_agent::store::Store;
use plant_agent::{Agent, Clock, batching, http, metrics, sync};
use tokio::task::JoinSet;
use tracing::{info, warn};

/// How often the gauges are set from the status.
const REPORT: Duration = Duration::from_secs(5);

/// Settings from the environment. Every variable is optional; `.env.example` lists the defaults.
struct Config {
    plant: String,
    dispatch_url: String,
    mqtt_url: String,
    db: PathBuf,
    http_addr: SocketAddr,
    metrics_addr: SocketAddr,
    plan_poll: Duration,
}

impl Config {
    fn from_lookup(get: impl Fn(&str) -> Option<String>) -> anyhow::Result<Config> {
        let var = |key: &str, default: &str| {
            get(key)
                .filter(|v| !v.trim().is_empty())
                .unwrap_or_else(|| default.to_owned())
        };
        let poll: u64 = parsed("PLAN_POLL_SECS", &var("PLAN_POLL_SECS", "15"))?;
        ensure!(poll > 0, "PLAN_POLL_SECS must be at least 1");
        Ok(Config {
            plant: var("PLANT_ID", "plant-walldorf"),
            // Every service reads the same .env, so these two mean what they mean elsewhere.
            dispatch_url: var("DISPATCH_URL", "http://localhost:8081"),
            mqtt_url: var("MQTT_URL", "mqtt://localhost:1883"),
            db: PathBuf::from(var("PLANT_AGENT_DB", "plant-agent.sqlite")),
            http_addr: parsed(
                "PLANT_AGENT_HTTP_ADDR",
                &var("PLANT_AGENT_HTTP_ADDR", "0.0.0.0:8082"),
            )?,
            metrics_addr: parsed(
                "PLANT_AGENT_METRICS_ADDR",
                &var("PLANT_AGENT_METRICS_ADDR", "0.0.0.0:9466"),
            )?,
            plan_poll: Duration::from_secs(poll),
        })
    }
}

fn parsed<T: FromStr>(key: &str, raw: &str) -> anyhow::Result<T>
where
    T::Err: Display,
{
    raw.trim()
        .parse()
        .map_err(|e| anyhow::anyhow!("{key}={raw}: {e}"))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::from_lookup(|key| std::env::var(key).ok())?;
    let _telemetry = telemetry::init("plant_agent", config.metrics_addr)?;
    info!(
        offset_hours = domain::clock::init_from_env()?.num_hours(),
        "world clock (CLOCK_OFFSET_HOURS)"
    );
    metrics::describe();
    let seed = Seed::rhine_neckar()?;
    let plant = seed
        .plant(&config.plant)
        .cloned()
        .with_context(|| format!("PLANT_ID={} is not a plant in the seed", config.plant))?;
    let store =
        Store::open(&config.db).with_context(|| format!("opening {}", config.db.display()))?;
    let agent = Arc::new(Agent::new(
        plant,
        store,
        Clock::start(domain::clock::now()),
    )?);
    let status = agent.status()?;
    info!(
        plant = %config.plant,
        db = %config.db.display(),
        dispatch = %config.dispatch_url,
        mqtt = %config.mqtt_url,
        plan_version = ?status.plan_version,
        outbox = status.outbox,
        "plant-agent starting"
    );

    let mut tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();
    let settings = Settings::new(
        config.mqtt_url.clone(),
        format!("pourline-plant-agent-{}", config.plant),
    );
    let (store, wake, link) = (
        Arc::clone(&agent.store),
        Arc::clone(&agent.wake),
        Arc::clone(&agent.mqtt),
    );
    tasks.spawn(async move { Ok(forward::run(store, settings, wake, link).await?) });
    let (syncing, url, every) = (
        Arc::clone(&agent),
        config.dispatch_url.clone(),
        config.plan_poll,
    );
    tasks.spawn(async move { Ok(sync::run(syncing, &url, every).await?) });
    let batching = Arc::clone(&agent);
    tasks.spawn(async move { Ok(batching::run(batching).await?) });
    let reporting = Arc::clone(&agent);
    tasks.spawn(async move {
        let mut tick = tokio::time::interval(REPORT);
        loop {
            tick.tick().await;
            match reporting.status() {
                Ok(status) => metrics::report(&status),
                Err(error) => warn!(%error, "can't read the status for metrics"),
            }
        }
    });
    let listener = tokio::net::TcpListener::bind(config.http_addr).await?;
    info!(addr = %config.http_addr, "status listening");
    let router = http::router(Arc::clone(&agent));
    tasks.spawn(async move { Ok(axum::serve(listener, router).await?) });

    // Every task runs forever, so one ending is a failure. Exit and let the container restart.
    tokio::select! {
        finished = tasks.join_next() => match finished {
            Some(Ok(Err(error))) => Err(error.context("a plant-agent task failed")),
            Some(Err(join)) => Err(anyhow::Error::new(join).context("a plant-agent task panicked")),
            Some(Ok(Ok(()))) | None => anyhow::bail!("a plant-agent task stopped unexpectedly"),
        },
        signal = shutdown_signal() => {
            signal?;
            info!("shutting down");
            Ok(())
        }
    }
}

/// Docker and Kubernetes stop containers with SIGTERM, which a PID-1 process ignores unless it
/// handles it.
async fn shutdown_signal() -> anyhow::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result?,
        _ = terminate.recv() => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &'static [(&'static str, &'static str)]) -> impl Fn(&str) -> Option<String> {
        move |key| {
            pairs
                .iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| (*v).to_owned())
        }
    }

    #[test]
    fn config_has_working_defaults() {
        let config = Config::from_lookup(env(&[])).unwrap();
        assert_eq!(config.plant, "plant-walldorf");
        assert_eq!(config.dispatch_url, "http://localhost:8081");
        assert_eq!(config.mqtt_url, "mqtt://localhost:1883");
        assert_eq!(config.db, PathBuf::from("plant-agent.sqlite"));
        assert_eq!(config.http_addr, SocketAddr::from(([0, 0, 0, 0], 8082)));
        assert_eq!(config.metrics_addr, SocketAddr::from(([0, 0, 0, 0], 9466)));
        assert_eq!(config.plan_poll, Duration::from_secs(15));
    }

    #[test]
    fn config_reads_overrides_and_rejects_nonsense() {
        let config = Config::from_lookup(env(&[
            ("PLANT_ID", "plant-mannheim"),
            ("DISPATCH_URL", "http://cloud"),
            ("PLANT_AGENT_DB", " "),
        ]))
        .unwrap();
        assert_eq!(
            (config.plant.as_str(), config.dispatch_url.as_str()),
            ("plant-mannheim", "http://cloud")
        );
        assert_eq!(
            config.db,
            PathBuf::from("plant-agent.sqlite"),
            "blank means unset"
        );

        assert!(Config::from_lookup(env(&[("PLAN_POLL_SECS", "0")])).is_err());
        assert!(Config::from_lookup(env(&[("PLAN_POLL_SECS", "soon")])).is_err());
        assert!(Config::from_lookup(env(&[("PLANT_AGENT_HTTP_ADDR", "8082")])).is_err());
    }
}
