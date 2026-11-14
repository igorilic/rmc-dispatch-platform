//! Shared by the integration tests: throwaway Postgres and NATS containers (same images as compose).

#![allow(dead_code)]

use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::Europe::Berlin;
use domain::{CustomerId, MixCode, Order, OrderId, OrderStatus, SiteId};
use telemetry::testing::{InMemorySpanExporter, SpanData};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

pub async fn postgres() -> (ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("postgres", "18.6-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "test")
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    (
        container,
        format!("postgres://postgres:test@127.0.0.1:{port}/postgres"),
    )
}

pub async fn nats() -> (ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("nats", "2.15.3-alpine")
        .with_exposed_port(4222.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
        .with_cmd(["--jetstream"])
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(4222).await.unwrap();
    (container, format!("nats://127.0.0.1:{port}"))
}

/// Tomorrow morning in Berlin, at whole seconds (Postgres keeps microseconds).
pub fn at(h: u32, m: u32) -> DateTime<Utc> {
    Berlin
        .with_ymd_and_hms(2026, 11, 10, h, m, 0)
        .unwrap()
        .with_timezone(&Utc)
}

/// A bauwerk-ag order for one of its real ship-tos in the seed.
pub fn order(number: &str, volume: f64, start: DateTime<Utc>) -> Order {
    Order {
        id: OrderId(format!("10001001/{number}")),
        customer: CustomerId("10001001".into()),
        site: SiteId("50001007".into()),
        mix: MixCode("C30/37-XC4".into()),
        total_volume_m3: volume,
        requested_start: start,
        pour_rate_m3_per_h: 30.0,
        max_gap_minutes: 20,
        status: OrderStatus::Pending,
    }
}

/// Polls until a span named `name` has ended, or fails after 10 s.
pub async fn finished(exporter: &InMemorySpanExporter, name: &str) -> Vec<SpanData> {
    for _ in 0..100 {
        let spans = exporter.get_finished_spans().unwrap();
        if spans.iter().any(|s| s.name == name) {
            return spans;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no {name} span ended");
}

pub fn named<'a>(spans: &'a [SpanData], name: &str) -> &'a SpanData {
    spans
        .iter()
        .find(|s| s.name == name)
        .unwrap_or_else(|| panic!("no {name} span"))
}
