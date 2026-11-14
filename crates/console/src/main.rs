//! The map console service (SPEC §9). All settings come from environment variables; see `Config`.

use std::fmt::Display;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, ensure};
use console::{Console, Settings, Webhook};
use domain::{PlantId, Seed};
use rumqttc::{AsyncClient, MqttOptions};
use tracing::info;

/// Settings from the environment. Every variable is optional; `.env.example` lists the defaults.
struct Config {
    http_addr: SocketAddr,
    metrics_addr: SocketAddr,
    nats_url: String,
    mqtt_url: String,
    settings: Settings,
}

impl Config {
    fn from_lookup(get: impl Fn(&str) -> Option<String>) -> anyhow::Result<Config> {
        let var = |key: &str, default: &str| {
            get(key)
                .filter(|v| !v.trim().is_empty())
                .unwrap_or_else(|| default.to_owned())
        };
        let optional = |key: &str| get(key).filter(|v| !v.trim().is_empty());
        let cut: u64 = parsed("CHAOS_SECONDS", &var("CHAOS_SECONDS", "120"))?;
        ensure!(cut > 0, "CHAOS_SECONDS must be at least 1");
        let plant = var("PLANT_ID", "plant-walldorf");
        ensure!(
            Seed::rhine_neckar()?.plant(&plant).is_some(),
            "PLANT_ID={plant} is not a plant in the seed"
        );
        let webhook = |integration: &str, url_key: &str| Webhook {
            url: var(
                url_key,
                &format!("http://localhost:8080/webhooks/{integration}"),
            ),
            // The name ingest's webhook_token looks up, and the same dev default.
            token: var(
                &format!(
                    "WEBHOOK_TOKEN_{}",
                    integration.to_uppercase().replace('-', "_")
                ),
                &format!("dev-token-{integration}"),
            ),
        };
        Ok(Config {
            http_addr: parsed("CONSOLE_ADDR", &var("CONSOLE_ADDR", "0.0.0.0:8083"))?,
            metrics_addr: parsed(
                "CONSOLE_METRICS_ADDR",
                &var("CONSOLE_METRICS_ADDR", "0.0.0.0:9468"),
            )?,
            // Every service reads the same .env, so these mean what they mean elsewhere.
            nats_url: var("NATS_URL", "nats://localhost:4222"),
            mqtt_url: var("MQTT_URL", "mqtt://localhost:1883"),
            settings: Settings {
                dispatch_url: var("DISPATCH_URL", "http://localhost:8081"),
                plant: PlantId(plant),
                plant_agent_url: optional("PLANT_AGENT_URL"),
                uplink_url: optional("UPLINK_URL"),
                cut: Duration::from_secs(cut),
                poll: Duration::from_secs(2),
                public_url: var("PUBLIC_URL", "http://localhost:8080"),
                bauwerk: webhook("bauwerk-ag", "BAUWERK_WEBHOOK_URL"),
                neckar: webhook("neckar-infra", "NECKAR_WEBHOOK_URL"),
            },
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
    let _telemetry = telemetry::init("console", config.metrics_addr)?;
    info!(
        offset_hours = domain::clock::init_from_env()?.num_hours(),
        plant = %config.settings.plant.0,
        edge = config.settings.plant_agent_url.is_some(),
        uplink = config.settings.uplink_url.is_some(),
        "console starting"
    );
    console::metrics::describe();

    let mut options =
        MqttOptions::parse_url(format!("{}?client_id=pourline-console", config.mqtt_url))
            .context("MQTT_URL")?;
    options.set_keep_alive(Duration::from_secs(10));
    let (mqtt, eventloop) = AsyncClient::new(options, 16);
    let console = Console::new(config.settings, mqtt)?;

    // Core NATS doesn't replay what it dropped, so the console reads /world after a reconnect.
    let resyncing = Arc::clone(&console);
    let nats = async_nats::ConnectOptions::new()
        .retry_on_initial_connect()
        .event_callback(move |event| {
            let console = Arc::clone(&resyncing);
            async move {
                if matches!(event, async_nats::Event::Connected) {
                    console.resync();
                }
            }
        })
        .connect(&config.nats_url)
        .await?;
    console.start(nats, eventloop);

    let listener = tokio::net::TcpListener::bind(config.http_addr).await?;
    info!(addr = %config.http_addr, "console listening at /console/");
    axum::serve(listener, console::http::router(console))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    info!("shutting down");
    Ok(())
}

/// Kubernetes stops pods with SIGTERM, which a PID-1 process ignores unless it handles it.
async fn shutdown_signal() {
    let interrupt = tokio::signal::ctrl_c();
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = interrupt => {}
        () = terminate => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let pairs: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |key| pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
    }

    #[test]
    fn config_has_working_defaults() {
        let config = Config::from_lookup(env(&[])).unwrap();
        assert_eq!(config.http_addr.port(), 8083);
        assert_eq!(config.metrics_addr.port(), 9468);
        assert_eq!(config.settings.plant.0, "plant-walldorf");
        assert_eq!(config.settings.plant_agent_url, None, "no edge card");
        assert_eq!(config.settings.uplink_url, None, "no Make chaos");
        assert_eq!(config.settings.cut, Duration::from_secs(120));
        assert_eq!(config.settings.bauwerk.token, "dev-token-bauwerk-ag");
        assert_eq!(config.settings.neckar.token, "dev-token-neckar-infra");
    }

    #[test]
    fn config_reads_overrides_and_rejects_nonsense() {
        let config = Config::from_lookup(env(&[
            ("PLANT_AGENT_URL", "http://172.28.0.4:8082"),
            ("UPLINK_URL", "http://uplink:8474"),
            ("CHAOS_SECONDS", "30"),
        ]))
        .unwrap();
        assert_eq!(
            config.settings.plant_agent_url.as_deref(),
            Some("http://172.28.0.4:8082")
        );
        assert_eq!(config.settings.cut, Duration::from_secs(30));
        assert!(Config::from_lookup(env(&[("CHAOS_SECONDS", "0")])).is_err());
        assert!(Config::from_lookup(env(&[("CHAOS_SECONDS", "soon")])).is_err());
        assert!(Config::from_lookup(env(&[("PLANT_ID", "plant-nowhere")])).is_err());
    }
}
