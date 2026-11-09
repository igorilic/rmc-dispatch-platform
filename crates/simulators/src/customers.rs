//! The three fake customers' systems (SPEC §5). They differ only in how they deliver orders.
//!
//! - `bauwerk-ag` posts JSON with its own field names. Every change is re-sent as the full
//!   document, and 5 % of posts are sent twice.
//! - `rheinbau-gmbh` drops a CSV on SFTP every few minutes: semicolons, decimal commas and Berlin
//!   local time. Each drop is a full snapshot of the open orders, plus any cancelled since the last drop.
//! - `neckar-infra` posts XML. Half of its sites use schema v1 and half use v2.
//!
//! Until ingest exists (M3), the webhook URLs are unset and the payloads are logged instead.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use chrono_tz::Europe::Berlin;
use quick_xml::se::Serializer;
use rand::{RngExt, rngs::StdRng};
use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tokio::time::{Instant, sleep};
use tracing::{error, info, warn};

use crate::Clock;
use crate::orders::{GeneratedOrder, OrderBook};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BauwerkOrder<'a> {
    job_id: &'a str,
    updated_at: String,
    deliver_to: DeliverTo<'a>,
    product: &'a str,
    qty: Qty,
    pour_start: String,
    pump_rate: u32,
    max_interval_minutes: u32,
    state: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeliverTo<'a> {
    site_ref: &'a str,
}

#[derive(Serialize)]
struct Qty {
    value: f64,
    unit: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct NeckarV1<'a> {
    #[serde(rename = "@version")]
    version: u8,
    order_no: &'a str,
    site_code: &'a str,
    product: &'a str,
    quantity: f64,
    start: String,
    pour_rate: u32,
    max_gap: u32,
    status: &'static str,
}

/// v2 moves to attributes, UTC timestamps and litres, and adds a revision counter.
#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct NeckarV2<'a> {
    #[serde(rename = "@version")]
    version: u8,
    header: V2Header<'a>,
    delivery: V2Delivery<'a>,
    material: V2Material<'a>,
}

#[derive(Serialize)]
struct V2Header<'a> {
    #[serde(rename = "@orderNo")]
    order_no: &'a str,
    #[serde(rename = "@revision")]
    revision: u32,
    #[serde(rename = "@status")]
    status: &'static str,
}

#[derive(Serialize)]
struct V2Delivery<'a> {
    #[serde(rename = "@site")]
    site: &'a str,
    #[serde(rename = "@start")]
    start: String,
    #[serde(rename = "@maxGapMin")]
    max_gap_min: u32,
}

#[derive(Serialize)]
struct V2Material<'a> {
    #[serde(rename = "@code")]
    code: &'a str,
    #[serde(rename = "@quantityLitres")]
    quantity_litres: u32,
    #[serde(rename = "@rateLitresPerHour")]
    rate_litres_per_hour: u32,
}

fn berlin(t: DateTime<Utc>) -> String {
    t.with_timezone(&Berlin)
        .to_rfc3339_opts(SecondsFormat::Secs, false)
}

fn decimal_comma(x: f64) -> String {
    format!("{x:.1}").replace('.', ",")
}

pub fn bauwerk_json(o: &GeneratedOrder) -> serde_json::Result<String> {
    serde_json::to_string(&BauwerkOrder {
        job_id: &o.order_ref,
        updated_at: berlin(o.updated_at),
        deliver_to: DeliverTo {
            site_ref: &o.site_ref,
        },
        product: &o.mix,
        qty: Qty {
            value: o.volume_m3,
            unit: "m3",
        },
        pour_start: berlin(o.start),
        pump_rate: o.pour_rate_m3_per_h,
        max_interval_minutes: o.max_gap_minutes,
        state: if o.cancelled {
            "CANCELLED"
        } else {
            "CONFIRMED"
        },
    })
}

pub fn rheinbau_csv(orders: &[GeneratedOrder]) -> anyhow::Result<String> {
    let mut csv = csv::WriterBuilder::new()
        .delimiter(b';')
        .terminator(csv::Terminator::Any(b'\n'))
        .from_writer(Vec::new());
    csv.write_record([
        "Auftragsnr",
        "Baustelle",
        "Sorte",
        "Menge",
        "Liefertermin",
        "Leistung",
        "MaxPause",
        "Status",
    ])?;
    for o in orders {
        csv.write_record([
            o.order_ref.as_str(),
            o.site_ref.as_str(),
            o.mix.as_str(),
            decimal_comma(o.volume_m3).as_str(),
            o.start
                .with_timezone(&Berlin)
                .format("%d.%m.%Y %H:%M")
                .to_string()
                .as_str(),
            decimal_comma(f64::from(o.pour_rate_m3_per_h)).as_str(),
            o.max_gap_minutes.to_string().as_str(),
            if o.cancelled {
                "storniert"
            } else {
                "bestätigt"
            },
        ])?;
    }
    Ok(String::from_utf8(csv.into_inner()?)?)
}

/// Every other neckar-infra site is still on v1.
pub fn neckar_version(site_ref: &str, site_refs: &[String]) -> u8 {
    match site_refs.iter().position(|s| s == site_ref) {
        Some(i) if i % 2 == 1 => 2,
        _ => 1,
    }
}

pub fn neckar_xml(o: &GeneratedOrder, version: u8) -> anyhow::Result<String> {
    let status = if o.cancelled {
        "cancelled"
    } else {
        "confirmed"
    };
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    let mut ser = Serializer::with_root(&mut xml, Some("Order"))?;
    ser.indent(' ', 2);
    if version == 1 {
        NeckarV1 {
            version,
            order_no: &o.order_ref,
            site_code: &o.site_ref,
            product: &o.mix,
            quantity: o.volume_m3,
            start: berlin(o.start),
            pour_rate: o.pour_rate_m3_per_h,
            max_gap: o.max_gap_minutes,
            status,
        }
        .serialize(ser)?;
    } else {
        NeckarV2 {
            version,
            header: V2Header {
                order_no: &o.order_ref,
                revision: o.revision,
                status,
            },
            delivery: V2Delivery {
                site: &o.site_ref,
                start: o.start.to_rfc3339_opts(SecondsFormat::Secs, true),
                max_gap_min: o.max_gap_minutes,
            },
            material: V2Material {
                code: &o.mix,
                quantity_litres: (o.volume_m3 * 1000.0).round() as u32,
                rate_litres_per_hour: o.pour_rate_m3_per_h * 1000,
            },
        }
        .serialize(ser)?;
    }
    xml.push('\n');
    Ok(xml)
}

fn jitter(rng: &mut StdRng, interval: Duration) -> Duration {
    interval.mul_f64(rng.random_range(0.5..1.5))
}

/// A customer that posts each order change to a webhook.
pub struct Webhook {
    pub customer: &'static str,
    /// `None` until ingest exists. The payload is logged instead.
    pub url: Option<String>,
    pub content_type: &'static str,
    pub duplicate_rate: f64,
    pub render: fn(&GeneratedOrder, &[String]) -> anyhow::Result<String>,
}

pub async fn run_webhook(
    hook: Webhook,
    mut book: OrderBook,
    order_interval: Duration,
    clock: Clock,
    mut rng: StdRng,
) -> anyhow::Result<()> {
    let http = reqwest::Client::new();
    loop {
        sleep(jitter(&mut rng, order_interval)).await;
        let order = book.next_change(&mut rng, clock.now());
        let body = (hook.render)(&order, book.site_refs())?;
        let copies = if rng.random_bool(hook.duplicate_rate) {
            2
        } else {
            1
        };
        for _ in 0..copies {
            deliver(&http, &hook, &order, &body).await;
        }
    }
}

async fn deliver(http: &reqwest::Client, hook: &Webhook, order: &GeneratedOrder, body: &str) {
    let customer = hook.customer;
    let Some(url) = &hook.url else {
        info!(customer, order = %order.order_ref, revision = order.revision, payload = body, "order (no receiver until M3, logged)");
        return;
    };
    for attempt in 1..=3u32 {
        let sent = http
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, hook.content_type)
            .body(body.to_owned())
            .send()
            .await
            .and_then(reqwest::Response::error_for_status);
        match sent {
            Ok(_) => {
                info!(customer, order = %order.order_ref, revision = order.revision, "order posted");
                return;
            }
            Err(error) => {
                warn!(customer, order = %order.order_ref, attempt, %error, "posting the order failed");
                sleep(Duration::from_secs(2u64.pow(attempt))).await;
            }
        }
    }
    error!(customer, order = %order.order_ref, "gave up after 3 attempts");
}

/// rheinbau-gmbh's SFTP account and how often it drops a file.
pub struct SftpDrop {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub every: Duration,
}

const DROP_DIR: &str = "rheinbau-gmbh";

pub async fn run_sftp(
    drop: SftpDrop,
    mut book: OrderBook,
    order_interval: Duration,
    clock: Clock,
    mut rng: StdRng,
) -> anyhow::Result<()> {
    let customer = "rheinbau-gmbh";
    let mut cancelled: Vec<GeneratedOrder> = Vec::new();
    let next_order = sleep(jitter(&mut rng, order_interval));
    tokio::pin!(next_order);
    let mut drops = tokio::time::interval_at(Instant::now() + drop.every, drop.every);
    loop {
        tokio::select! {
            () = &mut next_order => {
                let order = book.next_change(&mut rng, clock.now());
                info!(customer, order = %order.order_ref, revision = order.revision, cancelled = order.cancelled, "order changed, goes out with the next drop");
                if order.cancelled {
                    cancelled.push(order);
                }
                next_order.as_mut().reset(Instant::now() + jitter(&mut rng, order_interval));
            }
            _ = drops.tick() => {
                let rows: Vec<GeneratedOrder> = book.open().iter().chain(&cancelled).cloned().collect();
                if rows.is_empty() {
                    continue;
                }
                let name = format!("orders_{}.csv", clock.now().with_timezone(&Berlin).format("%Y%m%d_%H%M%S"));
                match upload(&drop, &name, rheinbau_csv(&rows)?.as_bytes()).await {
                    Ok(()) => {
                        info!(customer, file = %format!("{DROP_DIR}/{name}"), orders = rows.len(), "CSV dropped on SFTP");
                        cancelled.clear();
                    }
                    Err(error) => warn!(customer, %error, "SFTP drop failed, retrying with the next drop"),
                }
            }
        }
    }
}

struct TrustAnyHostKey;

impl russh::client::Handler for TrustAnyHostKey {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _key: &russh::keys::PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        // ponytail: trusts any host key, fine for the local compose SFTP server; pin the key before pointing it at a real one.
        Ok(true)
    }
}

/// Writes `name.part` and then renames it, so a reader never picks up half a file.
async fn upload(drop: &SftpDrop, name: &str, data: &[u8]) -> anyhow::Result<()> {
    let config = Arc::new(russh::client::Config::default());
    let mut session =
        russh::client::connect(config, (drop.host.as_str(), drop.port), TrustAnyHostKey).await?;
    let auth = session
        .authenticate_password(&drop.user, &drop.password)
        .await?;
    anyhow::ensure!(auth.success(), "SFTP login rejected for user {}", drop.user);
    let channel = session.channel_open_session().await?;
    channel.request_subsystem(true, "sftp").await?;
    let sftp = russh_sftp::client::SftpSession::new(channel.into_stream()).await?;
    if !sftp.try_exists(DROP_DIR).await? {
        sftp.create_dir(DROP_DIR).await?;
    }
    let part = format!("{DROP_DIR}/{name}.part");
    let mut file = sftp.create(part.clone()).await?;
    file.write_all(data).await?;
    file.shutdown().await?;
    sftp.rename(part, format!("{DROP_DIR}/{name}")).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::world::World;
    use serde_json::Value;

    #[allow(clippy::too_many_arguments)]
    fn order(
        order_ref: &str,
        site_ref: &str,
        mix: &str,
        volume_m3: f64,
        start: &str,
        pour_rate_m3_per_h: u32,
        max_gap_minutes: u32,
        revision: u32,
    ) -> GeneratedOrder {
        GeneratedOrder {
            order_ref: order_ref.into(),
            site_ref: site_ref.into(),
            mix: mix.into(),
            volume_m3,
            start: start.parse().unwrap(),
            pour_rate_m3_per_h,
            max_gap_minutes,
            revision,
            updated_at: "2026-11-09T09:15:02Z".parse().unwrap(),
            cancelled: false,
        }
    }

    #[test]
    fn bauwerk_json_matches_the_sample() {
        let o = order(
            "BW-2026-0042",
            "BW-HD-03",
            "C30/37-XC4",
            42.5,
            "2026-11-10T06:00:00Z",
            30,
            20,
            1,
        );
        let sample: Value = serde_json::from_str(include_str!(
            "../../../customers/bauwerk-ag/samples/order.json"
        ))
        .unwrap();
        let rendered: Value = serde_json::from_str(&bauwerk_json(&o).unwrap()).unwrap();
        assert_eq!(rendered, sample);

        let cancelled = GeneratedOrder {
            cancelled: true,
            ..o
        };
        let rendered: Value = serde_json::from_str(&bauwerk_json(&cancelled).unwrap()).unwrap();
        assert_eq!(rendered["state"], "CANCELLED");
    }

    #[test]
    fn rheinbau_csv_matches_the_sample() {
        let orders = [
            order(
                "RB-7781",
                "RB-SCHW-02",
                "C25/30-XC2",
                42.5,
                "2026-11-10T06:00:00Z",
                30,
                20,
                1,
            ),
            order(
                "RB-7782",
                "RB-MA-01",
                "C30/37-XC4",
                120.0,
                "2026-11-10T05:30:00Z",
                45,
                15,
                1,
            ),
            GeneratedOrder {
                cancelled: true,
                ..order(
                    "RB-7779",
                    "RB-SP-01",
                    "C20/25-X0",
                    18.0,
                    "2026-11-10T12:00:00Z",
                    20,
                    30,
                    2,
                )
            },
        ];
        let sample = include_str!("../../../customers/rheinbau-gmbh/samples/orders.csv");
        assert_eq!(rheinbau_csv(&orders).unwrap(), sample);
    }

    #[test]
    fn neckar_xml_v1_and_v2_match_the_samples() {
        let v1 = order(
            "NI-88213",
            "NI-HD-07",
            "C30/37-XC4",
            42.5,
            "2026-11-10T06:00:00Z",
            30,
            20,
            1,
        );
        let v1_sample = include_str!("../../../customers/neckar-infra/samples/order-v1.xml");
        assert_eq!(neckar_xml(&v1, 1).unwrap(), v1_sample);

        let v2 = order(
            "NI-88214",
            "NI-NG-02",
            "C30/37-XC4",
            42.5,
            "2026-11-10T06:00:00Z",
            30,
            20,
            2,
        );
        let v2_sample = include_str!("../../../customers/neckar-infra/samples/order-v2.xml");
        assert_eq!(neckar_xml(&v2, 2).unwrap(), v2_sample);
    }

    #[test]
    fn neckar_sites_are_split_between_v1_and_v2() {
        let world = World::seed().unwrap();
        let sites: Vec<String> = world
            .sites_of("neckar-infra")
            .iter()
            .map(|s| s.customer_ref.clone())
            .collect();
        let v1 = sites
            .iter()
            .filter(|s| neckar_version(s, &sites) == 1)
            .count();
        let v2 = sites
            .iter()
            .filter(|s| neckar_version(s, &sites) == 2)
            .count();
        assert_eq!(v1 + v2, sites.len());
        assert!(v1.abs_diff(v2) <= 1, "{v1} on v1, {v2} on v2");
    }
}
