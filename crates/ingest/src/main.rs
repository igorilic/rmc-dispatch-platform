//! The ingest service: webhooks, the SFTP poller and the MQTT subscriber, all publishing canonical
//! events to NATS JetStream. All settings come from environment variables; see `Config`.

use std::fmt::Display;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::ensure;
use domain::{Channel, Seed};
use ingest::mapping::Mapping;
use ingest::nats::Bus;
use ingest::sources::{self, SftpSource, Webhooks};
use tokio::task::JoinSet;
use tracing::info;

/// Settings from the environment. Every variable is optional; `.env.example` lists the defaults.
struct Config {
    http_addr: String,
    metrics_addr: SocketAddr,
    nats_url: String,
    mqtt_url: String,
    sftp: SftpSource,
    mappings_dir: PathBuf,
    max_in_flight: usize,
    dedup_ttl: Duration,
}

/// `bauwerk-ag` → `WEBHOOK_TOKEN_BAUWERK_AG`.
fn token_var(integration: &str) -> String {
    format!(
        "WEBHOOK_TOKEN_{}",
        integration.to_uppercase().replace('-', "_")
    )
}

/// The integration's bearer token, or a throwaway dev default shared with the simulators.
fn webhook_token(get: &impl Fn(&str) -> Option<String>, integration: &str) -> String {
    get(&token_var(integration))
        .filter(|t| !t.trim().is_empty())
        .unwrap_or_else(|| format!("dev-token-{integration}"))
}

impl Config {
    fn from_lookup(get: impl Fn(&str) -> Option<String>) -> anyhow::Result<Config> {
        let var = |key: &str, default: &str| {
            get(key)
                .filter(|v| !v.trim().is_empty())
                .unwrap_or_else(|| default.to_owned())
        };
        let at_least_one = |key: &str, default: &str| -> anyhow::Result<u64> {
            let value: u64 = parsed(key, &var(key, default))?;
            ensure!(value > 0, "{key} must be at least 1");
            Ok(value)
        };
        Ok(Config {
            http_addr: var("HTTP_ADDR", "0.0.0.0:8080"),
            metrics_addr: parsed("METRICS_ADDR", &var("METRICS_ADDR", "0.0.0.0:9464"))?,
            nats_url: var("NATS_URL", "nats://localhost:4222"),
            mqtt_url: var("MQTT_URL", "mqtt://localhost:1883"),
            sftp: SftpSource {
                host: var("SFTP_HOST", "localhost"),
                port: parsed("SFTP_PORT", &var("SFTP_PORT", "2222"))?,
                user: var("SFTP_USER", "pourline"),
                password: var("SFTP_PASSWORD", "pourline"),
                every: Duration::from_secs(at_least_one("SFTP_POLL_SECS", "30")?),
            },
            mappings_dir: PathBuf::from(var("MAPPINGS_DIR", "customers")),
            max_in_flight: parsed("MAX_IN_FLIGHT", &var("MAX_IN_FLIGHT", "64"))?,
            dedup_ttl: Duration::from_secs(at_least_one("DEDUP_TTL_DAYS", "7")? * 24 * 3600),
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
    let lookup = |key: &str| std::env::var(key).ok();
    let config = Config::from_lookup(lookup)?;
    let _telemetry = telemetry::init("ingest", config.metrics_addr)?;
    ingest::metrics::describe();
    let seed = Arc::new(Seed::rhine_neckar()?);
    let mappings = Mapping::load_all(&config.mappings_dir)?;
    let names: Vec<&str> = mappings.iter().map(|m| m.integration.as_str()).collect();
    info!(integrations = ?names, dir = %config.mappings_dir.display(), "mappings loaded");
    let bus = Bus::connect(&config.nats_url, config.dedup_ttl).await?;
    info!(nats = %config.nats_url, "connected to NATS JetStream");

    // The integrations that can dead-letter: every mapping, plus MQTT's telematics and plants.
    let integrations: Vec<String> = mappings
        .iter()
        .map(|m| m.integration.clone())
        .chain(["telematics".to_owned(), "plants".to_owned()])
        .collect();
    let (sftp, webhooks): (Vec<Mapping>, Vec<Mapping>) = mappings
        .into_iter()
        .partition(|m| m.channel == Channel::Sftp);
    let token = |integration: &str| webhook_token(&lookup, integration);
    let router = sources::webhook_router(Webhooks::new(
        bus.clone(),
        Arc::clone(&seed),
        webhooks,
        token,
        config.max_in_flight,
    ));
    let listener = tokio::net::TcpListener::bind(&config.http_addr).await?;
    info!(addr = %config.http_addr, "webhooks listening");

    let mut tasks = JoinSet::new();
    tasks.spawn(async move { Ok(axum::serve(listener, router).await?) });
    let (mqtt_bus, mqtt_seed, mqtt_url) = (bus.clone(), Arc::clone(&seed), config.mqtt_url.clone());
    tasks.spawn(async move { Ok(sources::run_mqtt(&mqtt_url, mqtt_bus, mqtt_seed).await?) });
    let dlq_bus = bus.clone();
    tasks.spawn(async move {
        ingest::metrics::watch_dlq(dlq_bus, Duration::from_secs(15), integrations).await;
        anyhow::Ok(())
    });
    // ponytail: one SFTP account for every SFTP integration; give each its own when a second one appears.
    for mapping in sftp {
        let (source, bus, seed) = (config.sftp.clone(), bus.clone(), Arc::clone(&seed));
        tasks.spawn(async move {
            sources::run_sftp(source, bus, seed, Arc::new(mapping)).await;
            anyhow::Ok(())
        });
    }

    // Every task runs forever, so one ending is a failure. Exit and let the container restart.
    tokio::select! {
        finished = tasks.join_next() => match finished {
            Some(Ok(Err(error))) => Err(error.context("an ingest task failed")),
            Some(Err(join)) => Err(anyhow::Error::new(join).context("an ingest task panicked")),
            Some(Ok(Ok(()))) | None => anyhow::bail!("an ingest task stopped unexpectedly"),
        },
        signal = shutdown_signal() => {
            signal?;
            info!("shutting down");
            Ok(())
        }
    }
}

/// Docker stops containers with SIGTERM, which a PID-1 process ignores unless it handles it.
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
        assert_eq!(config.http_addr, "0.0.0.0:8080");
        assert_eq!(config.metrics_addr, SocketAddr::from(([0, 0, 0, 0], 9464)));
        assert_eq!(config.nats_url, "nats://localhost:4222");
        assert_eq!(config.mqtt_url, "mqtt://localhost:1883");
        assert_eq!(
            (config.sftp.host.as_str(), config.sftp.port),
            ("localhost", 2222)
        );
        assert_eq!(config.sftp.every, Duration::from_secs(30));
        assert_eq!(config.mappings_dir, PathBuf::from("customers"));
        assert_eq!(config.max_in_flight, 64);
        assert_eq!(config.dedup_ttl, Duration::from_secs(7 * 24 * 3600));
    }

    #[test]
    fn tokens_come_from_per_integration_variables() {
        assert_eq!(token_var("bauwerk-ag"), "WEBHOOK_TOKEN_BAUWERK_AG");
        let lookup = env(&[
            ("WEBHOOK_TOKEN_NECKAR_INFRA", "s3cret"),
            ("WEBHOOK_TOKEN_BAUWERK_AG", " "),
        ]);
        assert_eq!(webhook_token(&lookup, "neckar-infra"), "s3cret");
        assert_eq!(
            webhook_token(&lookup, "bauwerk-ag"),
            "dev-token-bauwerk-ag",
            "blank means unset"
        );
    }

    #[test]
    fn config_rejects_nonsense() {
        assert!(Config::from_lookup(env(&[("DEDUP_TTL_DAYS", "0")])).is_err());
        assert!(Config::from_lookup(env(&[("SFTP_POLL_SECS", "0")])).is_err());
        assert!(Config::from_lookup(env(&[("MAX_IN_FLIGHT", "lots")])).is_err());
        assert!(Config::from_lookup(env(&[("SFTP_PORT", "ssh")])).is_err());
        assert!(Config::from_lookup(env(&[("METRICS_ADDR", "9464")])).is_err());
    }
}
