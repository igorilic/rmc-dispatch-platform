//! Fake trucks, plants and customers, so Pourline runs locally with no real data (SPEC §2, M2).
//!
//! One process runs everything as tokio tasks: 25 trucks, 3 plants and 3 customers. Truck and
//! plant messages go to Mosquitto. Customer orders go to ingest's webhooks and to the SFTP server.
//! Trucks break down on command (`sim/trucks/{id}/breakdown`). All settings come from environment
//! variables; see `Config`.

use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, ensure};
use chrono::Utc;
use domain::{PlantId, Seed};
use rand::{SeedableRng, rngs::StdRng};
use rumqttc::{AsyncClient, MqttOptions};
use simulators::Clock;
use simulators::customers::{self, SftpDrop, Webhook};
use simulators::fleet::{FleetConfig, TruckSim};
use simulators::link::{self, Chaos};
use simulators::orders::{OrderBook, OrderNumbers};
use simulators::plant::PlantSim;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::info;
use tracing_subscriber::EnvFilter;

/// Settings from the environment. Every variable is optional; `.env.example` lists the defaults.
struct Config {
    mqtt_url: String,
    sftp: SftpDrop,
    bauwerk_webhook_url: Option<String>,
    neckar_webhook_url: Option<String>,
    bauwerk_webhook_token: String,
    neckar_webhook_token: String,
    order_interval: Duration,
    fleet: FleetConfig,
    seed: u64,
}

impl Config {
    fn from_lookup(get: impl Fn(&str) -> Option<String>) -> anyhow::Result<Config> {
        let var = |key: &str| get(key).filter(|v| !v.trim().is_empty());
        let parse = |key: &str, default: &str| var(key).unwrap_or_else(|| default.to_owned());
        let rate = |key: &str, default: &str| -> anyhow::Result<f64> {
            let rate: f64 = parsed(key, &parse(key, default))?;
            ensure!(
                (0.0..=1.0).contains(&rate),
                "{key} must be between 0 and 1, got {rate}"
            );
            Ok(rate)
        };
        let secs = |key: &str, default: &str| -> anyhow::Result<Duration> {
            let secs: u64 = parsed(key, &parse(key, default))?;
            ensure!(secs > 0, "{key} must be at least 1 second");
            Ok(Duration::from_secs(secs))
        };
        Ok(Config {
            mqtt_url: parse("MQTT_URL", "mqtt://localhost:1883"),
            sftp: SftpDrop {
                host: parse("SFTP_HOST", "localhost"),
                port: parsed("SFTP_PORT", &parse("SFTP_PORT", "2222"))?,
                user: parse("SFTP_USER", "pourline"),
                password: parse("SFTP_PASSWORD", "pourline"),
                every: secs("RHEINBAU_DROP_INTERVAL_SECS", "300")?,
            },
            bauwerk_webhook_url: var("BAUWERK_WEBHOOK_URL"),
            neckar_webhook_url: var("NECKAR_WEBHOOK_URL"),
            bauwerk_webhook_token: parse("WEBHOOK_TOKEN_BAUWERK_AG", "dev-token-bauwerk-ag"),
            neckar_webhook_token: parse("WEBHOOK_TOKEN_NECKAR_INFRA", "dev-token-neckar-infra"),
            order_interval: secs("ORDER_INTERVAL_SECS", "180")?,
            fleet: FleetConfig {
                chaos: Chaos {
                    duplicate_rate: rate("TELEMATICS_DUPLICATE_RATE", "0.03")?,
                    reorder_rate: rate("TELEMATICS_REORDER_RATE", "0.03")?,
                },
                dead_zone_trip_share: rate("DEAD_ZONE_TRIP_SHARE", "0.2")?,
                repair: secs("TRUCK_REPAIR_MINUTES", "60").map(|minutes| minutes * 60)?,
            },
            seed: parsed("SIM_SEED", &parse("SIM_SEED", "42"))?,
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
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let config = Config::from_lookup(|key| std::env::var(key).ok())?;
    let world = Arc::new(Seed::rhine_neckar()?);
    let clock = Clock::start(Utc::now());
    let seed = |n: u64| StdRng::seed_from_u64(config.seed.wrapping_add(n));

    let (tx, rx) = mpsc::channel(4096);
    let mqtt = MqttOptions::parse_url(format!(
        "{}?client_id=pourline-simulators-{}",
        config.mqtt_url,
        std::process::id()
    ))?;
    let (client, eventloop) = AsyncClient::new(mqtt, 1024);
    let mut tasks = JoinSet::new();

    let mut plants: HashMap<PlantId, Arc<PlantSim>> = HashMap::new();
    for (i, plant) in world.plants.iter().enumerate() {
        let sim = PlantSim::new(
            plant.clone(),
            tx.clone(),
            clock,
            config.fleet.chaos,
            config.seed.wrapping_add(100 + i as u64),
        );
        plants.insert(plant.id.clone(), Arc::new(sim));
    }
    let mut breakdowns = HashMap::new();
    for (i, truck) in world.trucks.iter().enumerate() {
        let home = plants
            .get(&truck.home_plant)
            .cloned()
            .with_context(|| format!("unknown home plant for {}", truck.id.0))?;
        let sim = TruckSim::new(
            truck.clone(),
            home,
            Arc::clone(&world),
            tx.clone(),
            clock,
            config.fleet,
            config.seed.wrapping_add(i as u64),
        );
        breakdowns.insert(truck.id.0.clone(), sim.breakdown());
        tasks.spawn(sim.run());
    }
    drop(tx);
    tasks.spawn(link::drive(eventloop, client.clone(), breakdowns));
    tasks.spawn(link::publish(rx, client));

    let mixes: Vec<String> = world.mixes.iter().map(|m| m.code.0.clone()).collect();
    // Each integration orders for one customer from our master data, to that customer's ship-tos.
    let book = |integration: &str, numbers: OrderNumbers| -> anyhow::Result<OrderBook> {
        let (_, customer) = customers::INTEGRATIONS
            .iter()
            .find(|(name, _)| *name == integration)
            .with_context(|| format!("no customer for integration {integration}"))?;
        let master = world.customers.iter().find(|c| c.id.0 == *customer);
        let master = master.with_context(|| {
            format!("customer {customer} ({integration}) is not in the master data")
        })?;
        info!(integration, customer, name = %master.name, "ordering for customer");
        let ship_tos = world
            .sites_of(customer)
            .iter()
            .map(|s| s.id.0.clone())
            .collect();
        Ok(OrderBook::new(
            numbers,
            (*customer).to_owned(),
            ship_tos,
            mixes.clone(),
        ))
    };
    let bauwerk = Webhook {
        customer: "bauwerk-ag",
        url: config.bauwerk_webhook_url.clone(),
        token: config.bauwerk_webhook_token.clone(),
        content_type: "application/json",
        duplicate_rate: 0.05,
        render: |order, _| Ok(customers::bauwerk_json(order)?),
    };
    let neckar = Webhook {
        customer: "neckar-infra",
        url: config.neckar_webhook_url.clone(),
        token: config.neckar_webhook_token.clone(),
        content_type: "application/xml",
        duplicate_rate: 0.0,
        render: |order, sites| {
            customers::neckar_xml(order, customers::neckar_version(&order.ship_to, sites))
        },
    };
    let numbers = |prefix, first, width| OrderNumbers {
        prefix,
        first,
        width,
    };
    let interval = config.order_interval;
    tasks.spawn(customers::run_webhook(
        bauwerk,
        book("bauwerk-ag", numbers("BW-2026-", 1, 4))?,
        interval,
        clock,
        seed(200),
    ));
    tasks.spawn(customers::run_webhook(
        neckar,
        book("neckar-infra", numbers("NI-", 88001, 5))?,
        interval,
        clock,
        seed(201),
    ));
    let rheinbau = book("rheinbau-gmbh", numbers("RB-", 7001, 4))?;
    tasks.spawn(customers::run_sftp(
        config.sftp,
        rheinbau,
        interval,
        clock,
        seed(202),
    ));

    info!(
        plants = world.plants.len(),
        trucks = world.trucks.len(),
        sites = world.sites.len(),
        mqtt = %config.mqtt_url,
        bauwerk_webhook = config.bauwerk_webhook_url.as_deref().unwrap_or("unset, logging"),
        neckar_webhook = config.neckar_webhook_url.as_deref().unwrap_or("unset, logging"),
        "simulators running"
    );

    // Any task ending is a failure: they all run forever. Exit and let the container restart.
    tokio::select! {
        finished = tasks.join_next() => match finished {
            Some(Ok(Err(error))) => Err(error.context("a simulator failed")),
            Some(Err(join)) => Err(anyhow::Error::new(join).context("a simulator panicked")),
            Some(Ok(Ok(()))) | None => anyhow::bail!("a simulator stopped unexpectedly"),
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
        assert_eq!(config.mqtt_url, "mqtt://localhost:1883");
        assert_eq!(
            (config.sftp.host.as_str(), config.sftp.port),
            ("localhost", 2222)
        );
        assert_eq!(config.sftp.every, Duration::from_secs(300));
        assert_eq!(config.order_interval, Duration::from_secs(180));
        assert_eq!(config.fleet.chaos.duplicate_rate, 0.03);
        assert_eq!(config.fleet.dead_zone_trip_share, 0.2);
        assert_eq!(config.fleet.repair, Duration::from_secs(3600));
        assert_eq!(config.seed, 42);
        // Throwaway dev tokens, the same defaults ingest uses.
        assert_eq!(config.bauwerk_webhook_token, "dev-token-bauwerk-ag");
        assert_eq!(config.neckar_webhook_token, "dev-token-neckar-infra");
        assert_eq!(
            (config.bauwerk_webhook_url, config.neckar_webhook_url),
            (None, None)
        );
    }

    #[test]
    fn config_reads_overrides_and_rejects_nonsense() {
        let config = Config::from_lookup(env(&[
            (
                "BAUWERK_WEBHOOK_URL",
                "http://ingest:8080/webhooks/bauwerk-ag",
            ),
            ("NECKAR_WEBHOOK_URL", " "),
            ("SIM_SEED", "7"),
        ]))
        .unwrap();
        assert_eq!(
            config.bauwerk_webhook_url.as_deref(),
            Some("http://ingest:8080/webhooks/bauwerk-ag")
        );
        assert_eq!(config.neckar_webhook_url, None, "blank means unset");
        assert_eq!(config.seed, 7);

        assert!(Config::from_lookup(env(&[("TELEMATICS_DUPLICATE_RATE", "1.5")])).is_err());
        assert!(Config::from_lookup(env(&[("DEAD_ZONE_TRIP_SHARE", "-0.1")])).is_err());
        assert!(Config::from_lookup(env(&[("ORDER_INTERVAL_SECS", "0")])).is_err());
        assert!(Config::from_lookup(env(&[("SFTP_PORT", "ssh")])).is_err());
    }
}
