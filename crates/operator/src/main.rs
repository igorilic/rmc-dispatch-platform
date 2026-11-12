//! `operator`: runs the controller for the `CustomerIntegration`s in its own namespace, which it
//! learns from its service account. `operator crd` prints the CRD instead. All settings come from
//! environment variables; see `Config`.

use std::net::SocketAddr;

use anyhow::Context as _;
use ingest::nats::StatusReader;
use kube::Client;
use operator::workload::Settings;
use tracing::info;

/// The variables the operator passes on to every workload, when they are set.
const PASSED_ON: &[&str] = &[
    "NATS_URL",
    "SFTP_HOST",
    "SFTP_PORT",
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "RUST_LOG",
];

/// Settings from the environment. Every variable is optional; `.env.example` lists the defaults.
struct Config {
    nats_url: String,
    metrics_addr: SocketAddr,
    settings: Settings,
}

impl Config {
    fn from_lookup(get: impl Fn(&str) -> Option<String>) -> anyhow::Result<Config> {
        let var = |key: &str| get(key).filter(|v| !v.trim().is_empty());
        let nats_url = var("NATS_URL").unwrap_or_else(|| "nats://localhost:4222".into());
        let metrics_addr = var("OPERATOR_METRICS_ADDR").unwrap_or_else(|| "0.0.0.0:9467".into());
        let mut env: Vec<(String, String)> = PASSED_ON
            .iter()
            .filter_map(|key| var(key).map(|value| ((*key).to_owned(), value)))
            .collect();
        // The workloads reach NATS where the operator does, default included.
        if !env.iter().any(|(key, _)| key == "NATS_URL") {
            env.insert(0, ("NATS_URL".into(), nats_url.clone()));
        }
        Ok(Config {
            metrics_addr: metrics_addr
                .trim()
                .parse()
                .with_context(|| format!("OPERATOR_METRICS_ADDR={metrics_addr}"))?,
            nats_url,
            settings: Settings {
                ingest_image: var("INGEST_IMAGE")
                    .unwrap_or_else(|| "k3d-pourline-registry:5050/ingest:dev".into()),
                env,
            },
        })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if std::env::args().nth(1).as_deref() == Some("crd") {
        print!("{}", operator::crd::crd_yaml()?);
        return Ok(());
    }
    let config = Config::from_lookup(|key| std::env::var(key).ok())?;
    let _telemetry = telemetry::init("operator", config.metrics_addr)?;
    let client = Client::try_default().await?;
    let namespace = client.default_namespace().to_owned();
    let nats = StatusReader::connect(&config.nats_url).await?;
    info!(%namespace, image = %config.settings.ingest_image, nats = %config.nats_url, "watching CustomerIntegrations");
    operator::run(client, &namespace, config.settings, nats, shutdown_signal()).await;
    info!("shut down");
    Ok(())
}

/// Kubernetes stops pods with SIGTERM, which a PID-1 process ignores unless it handles it.
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    match signal(SignalKind::terminate()) {
        Ok(mut terminate) => {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }
        }
        Err(_) => {
            let _ = tokio::signal::ctrl_c().await;
        }
    }
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
        assert_eq!(config.nats_url, "nats://localhost:4222");
        assert_eq!(config.metrics_addr, SocketAddr::from(([0, 0, 0, 0], 9467)));
        assert_eq!(
            config.settings.ingest_image,
            "k3d-pourline-registry:5050/ingest:dev"
        );
        assert_eq!(
            config.settings.env,
            [("NATS_URL".to_owned(), "nats://localhost:4222".to_owned())]
        );
    }

    #[test]
    fn the_workloads_get_the_operators_connection_settings() {
        let config = Config::from_lookup(env(&[
            ("NATS_URL", "nats://nats:4222"),
            ("SFTP_HOST", "sftp"),
            ("SFTP_PORT", "2222"),
            ("OTEL_EXPORTER_OTLP_ENDPOINT", " "),
            ("INGEST_IMAGE", "registry/ingest@sha256:abc"),
            ("DATABASE_URL", "not for ingest"),
        ]))
        .unwrap();
        let passed: Vec<&str> = config
            .settings
            .env
            .iter()
            .map(|(k, _)| k.as_str())
            .collect();
        assert_eq!(
            passed,
            ["NATS_URL", "SFTP_HOST", "SFTP_PORT"],
            "blank means unset"
        );
        assert_eq!(config.settings.ingest_image, "registry/ingest@sha256:abc");
        assert!(Config::from_lookup(env(&[("OPERATOR_METRICS_ADDR", "9467")])).is_err());
    }
}
