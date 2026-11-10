//! `make demo`: a scripted re-dispatch scenario against the running stack (SPEC §9).
//!
//! 1. Two orders arrive through the customers' own channels: a bauwerk-ag JSON webhook and a
//!    neckar-infra XML webhook. Dispatch plans them.
//! 2. The demo plays the plant: batch tickets for load #1 of the first order, so it freezes.
//! 3. The truck of a later load breaks down. The simulated truck reports `out_of_service` over
//!    MQTT like any other status, and dispatch re-dispatches.
//! 4. The second order's site is delayed 30 minutes through the dispatch API.
//!
//! It prints the plan and every diff. Settings come from environment variables; the defaults
//! reach a local stack, and compose's `demo` service points them at the containers.

use std::time::Duration;

use anyhow::{Context, bail};
use chrono::{DateTime, TimeDelta, Timelike, Utc};
use chrono_tz::Europe::Berlin;
use domain::{DeliveryStatus, Seed};
use reqwest::header::CONTENT_TYPE;
use rumqttc::{AsyncClient, MqttOptions, QoS};
use serde_json::{Value, json};
use simulators::customers::{bauwerk_json, neckar_version, neckar_xml};
use simulators::orders::GeneratedOrder;
use simulators::plant::Ticket;
use tokio::time::{Instant, sleep};

const MIX: &str = "C30/37-XC4";
const POLL: Duration = Duration::from_millis(500);

fn env(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| default.to_owned())
}

struct Demo {
    http: reqwest::Client,
    dispatch: String,
    /// Where a person reaches dispatch, for the closing hint.
    dispatch_public: String,
    ingest: String,
    mqtt: AsyncClient,
    timeout: Duration,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mqtt_url = env("MQTT_URL", "mqtt://localhost:1883");
    let options = MqttOptions::parse_url(format!(
        "{mqtt_url}?client_id=pourline-demo-{}",
        std::process::id()
    ))?;
    let (mqtt, mut eventloop) = AsyncClient::new(options, 64);
    tokio::spawn(async move {
        loop {
            if eventloop.poll().await.is_err() {
                sleep(Duration::from_secs(1)).await;
            }
        }
    });
    let demo = Demo {
        http: reqwest::Client::new(),
        dispatch: env("DISPATCH_URL", "http://localhost:8081"),
        dispatch_public: env(
            "DISPATCH_PUBLIC_URL",
            &env("DISPATCH_URL", "http://localhost:8081"),
        ),
        ingest: env("INGEST_URL", "http://localhost:8080"),
        mqtt,
        timeout: Duration::from_secs(env("DEMO_TIMEOUT_SECS", "120").parse()?),
    };
    demo.run().await?;
    // Let the last MQTT messages leave before the process exits.
    sleep(Duration::from_secs(1)).await;
    Ok(())
}

impl Demo {
    async fn run(&self) -> anyhow::Result<()> {
        println!("Pourline demo: re-dispatch\n");
        self.wait_until_up().await?;
        let seed = Seed::rhine_neckar()?;
        let now = Utc::now();
        let start = round_up_to_5_minutes(now + TimeDelta::minutes(45));
        let stamp = now.with_timezone(&Berlin).format("%H%M%S");
        let bauwerk_site = seed.sites_of("10001001")[0].id.0.clone();
        let neckar_sites: Vec<String> = seed
            .sites_of("10001003")
            .iter()
            .map(|s| s.id.0.clone())
            .collect();
        let order =
            |order_ref: String, customer: &str, ship_to: &str, volume_m3, start| GeneratedOrder {
                order_ref,
                customer: customer.into(),
                ship_to: ship_to.into(),
                mix: MIX.into(),
                volume_m3,
                start,
                pour_rate_m3_per_h: 30,
                max_gap_minutes: 20,
                revision: 1,
                updated_at: now,
                cancelled: false,
            };
        let a = order(
            format!("DEMO-{stamp}-A"),
            "10001001",
            &bauwerk_site,
            40.0,
            start,
        );
        let b_start = start + TimeDelta::minutes(15);
        let b = order(
            format!("DEMO-{stamp}-B"),
            "10001003",
            &neckar_sites[0],
            27.0,
            b_start,
        );
        let id_a = format!("{}/{}", a.customer, a.order_ref);
        let id_b = format!("{}/{}", b.customer, b.order_ref);
        let ours = [id_a.as_str(), id_b.as_str()];
        let latest = self.get("/plans/latest").await?;
        let before = latest
            .as_ref()
            .and_then(|p| p["version"].as_i64())
            .unwrap_or(0);

        println!(
            "1. Two orders arrive: {id_a} (40 m³ for {}) by JSON webhook, {id_b} (27 m³ for {}) by XML webhook.",
            berlin(start),
            berlin(b_start)
        );
        self.post_order("bauwerk-ag", "application/json", bauwerk_json(&a)?)
            .await?;
        let version = neckar_version(&b.ship_to, &neckar_sites);
        self.post_order("neckar-infra", "application/xml", neckar_xml(&b, version)?)
            .await?;
        let planned = self
            .next_version(before, |p| {
                ours.iter().all(|id| {
                    loads(p, id).next().is_some()
                        || p["unplanned"]
                            .as_array()
                            .is_some_and(|u| u.iter().any(|x| x["order"] == *id))
                })
            })
            .await?;
        if let Some(u) = planned["unplanned"]
            .as_array()
            .and_then(|u| u.iter().find(|x| ours.iter().any(|id| x["order"] == *id)))
        {
            bail!("{} couldn't be planned: {}", u["order"], u["reason"]);
        }
        print_plan(&planned, &ours);

        let first = loads(&planned, &id_a)
            .find(|l| l["sequence"] == 1)
            .context("no load #1")?
            .clone();
        let (plant, truck) = (
            str_of(&first, "planned_plant"),
            str_of(&first, "planned_truck"),
        );
        println!(
            "\n2. {plant} batches load #1 of {id_a} on {truck} at {}, as planned: from now on it's frozen.",
            time_of(&first, "planned_batch_at")
        );
        self.play_the_plant(&first, &id_a).await?;
        self.wait_for("the order to be in progress", || async {
            let order = self.get(&format!("/orders/{id_a}")).await?;
            Ok(order.is_some_and(|o| o["status"] == "in_progress"))
        })
        .await?;
        println!("   {id_a} is in progress.");

        let victim = loads(&planned, &id_a)
            .find(|l| l["sequence"] != 1 && l["planned_truck"] != first["planned_truck"])
            .context("no later load on another truck")?;
        let broken = str_of(victim, "planned_truck").to_owned();
        println!(
            "\n3. {broken}, which carries load #{} of {id_a}, breaks down.",
            victim["sequence"]
        );
        self.mqtt
            .publish(
                format!("sim/trucks/{broken}/breakdown"),
                QoS::AtLeastOnce,
                false,
                Vec::new(),
            )
            .await?;
        let latest_version = planned["version"].as_i64().unwrap_or(before);
        let repaired = self
            .next_version(latest_version, |p| {
                p["triggers"].as_array().is_some_and(|ts| {
                    ts.iter()
                        .any(|t| t["type"] == "truck_out_of_service" && t["truck"] == *broken)
                })
            })
            .await?;
        let v = repaired["version"].as_i64().unwrap_or_default();
        let diff = self
            .get(&format!("/plans/{v}/diff"))
            .await?
            .context("no diff")?;
        print_diff(v, &diff, &ours);
        let frozen = loads(&repaired, &id_a)
            .find(|l| l["sequence"] == 1)
            .cloned();

        println!(
            "\n4. The pump at {}'s site ({}) is 30 minutes late.",
            id_b, b.ship_to
        );
        let delayed = self
            .send_json(
                &format!("/sites/{}/delays", b.ship_to),
                &json!({ "minutes": 30 }),
            )
            .await?;
        match (delayed["version"].as_i64(), delayed.get("diff")) {
            (Some(v), Some(diff)) if !diff.is_null() => print_diff(v, diff, &ours),
            _ => println!("   Nothing had to change."),
        }

        let latest = self.get("/plans/latest").await?.context("no plan")?;
        let still = loads(&latest, &id_a).find(|l| l["sequence"] == 1).cloned();
        println!();
        if frozen.is_some() && still == frozen {
            println!("✓ Load #1 of {id_a} stayed frozen through every re-dispatch.");
        } else {
            bail!("load #1 of {id_a} moved: {frozen:?} → {still:?}");
        }
        println!(
            "✓ Every version passed dispatch's hard-constraint checker before it was stored.\n\n\
             Explore: {0}/plans/latest, {0}/plans/{{version}}/diff, Swagger UI at {0}/swagger-ui/",
            self.dispatch_public
        );
        Ok(())
    }

    async fn wait_until_up(&self) -> anyhow::Result<()> {
        self.wait_for("dispatch to answer", || async {
            let up = self
                .http
                .get(format!("{}/healthz", self.dispatch))
                .send()
                .await;
            Ok(up.is_ok_and(|r| r.status().is_success()))
        })
        .await
    }

    /// Batching, loading and loaded tickets for `load`, naming the order and load number. The
    /// plant batches on schedule, so the frozen load is the planned one.
    async fn play_the_plant(&self, load: &Value, order: &str) -> anyhow::Result<()> {
        let plant = str_of(load, "planned_plant");
        let batch_at = load["planned_batch_at"]
            .as_str()
            .and_then(|s| s.parse::<DateTime<Utc>>().ok())
            .unwrap_or_else(Utc::now)
            .timestamp_millis();
        let ticket_no = format!("DEMO-{batch_at}");
        let events = [
            DeliveryStatus::Batching,
            DeliveryStatus::Loading,
            DeliveryStatus::Loaded,
        ];
        for (i, event) in (0..).zip(events) {
            let ticket = Ticket {
                plant,
                ticket_no: &ticket_no,
                vehicle: str_of(load, "planned_truck"),
                ts_ms: batch_at + i * 1000,
                event,
                mix: MIX,
                volume_m3: load["volume_m3"].as_f64().unwrap_or_default(),
                order: Some(order),
                load_no: load["sequence"]
                    .as_u64()
                    .and_then(|n| u32::try_from(n).ok()),
            };
            self.mqtt
                .publish(
                    format!("plants/{plant}/tickets"),
                    QoS::AtLeastOnce,
                    false,
                    serde_json::to_vec(&ticket)?,
                )
                .await?;
        }
        Ok(())
    }

    async fn post_order(
        &self,
        integration: &str,
        content_type: &str,
        body: String,
    ) -> anyhow::Result<()> {
        let token_key = format!(
            "WEBHOOK_TOKEN_{}",
            integration.to_uppercase().replace('-', "_")
        );
        let token = env(&token_key, &format!("dev-token-{integration}"));
        let response = self
            .http
            .post(format!("{}/webhooks/{integration}", self.ingest))
            .bearer_auth(token)
            .header(CONTENT_TYPE, content_type)
            .body(body)
            .send()
            .await
            .with_context(|| format!("posting to ingest at {}", self.ingest))?;
        if !response.status().is_success() {
            bail!(
                "ingest refused the {integration} order: {} {}",
                response.status(),
                response.text().await.unwrap_or_default()
            );
        }
        Ok(())
    }

    /// The JSON at `path` on the dispatch API, or `None` for 404.
    async fn get(&self, path: &str) -> anyhow::Result<Option<Value>> {
        let response = self
            .http
            .get(format!("{}{path}", self.dispatch))
            .send()
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            bail!("GET {path}: {}", response.status());
        }
        Ok(Some(serde_json::from_str(&response.text().await?)?))
    }

    async fn send_json(&self, path: &str, body: &Value) -> anyhow::Result<Value> {
        let response = self
            .http
            .post(format!("{}{path}", self.dispatch))
            .header(CONTENT_TYPE, "application/json")
            .body(body.to_string())
            .send()
            .await?;
        if !response.status().is_success() {
            bail!("POST {path}: {}", response.status());
        }
        Ok(serde_json::from_str(&response.text().await?)?)
    }

    /// The first plan version after `after` that satisfies `check`.
    async fn next_version(
        &self,
        after: i64,
        check: impl Fn(&Value) -> bool,
    ) -> anyhow::Result<Value> {
        let deadline = Instant::now() + self.timeout;
        let mut next = after + 1;
        loop {
            match self.get(&format!("/plans/{next}")).await? {
                Some(plan) if check(&plan) => return Ok(plan),
                Some(_) => next += 1,
                None if Instant::now() > deadline => {
                    bail!(
                        "no matching plan version after {after} within {:?}",
                        self.timeout
                    )
                }
                None => sleep(POLL).await,
            }
        }
    }

    async fn wait_for<F, Fut>(&self, what: &str, check: F) -> anyhow::Result<()>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = anyhow::Result<bool>>,
    {
        let deadline = Instant::now() + self.timeout;
        while !check().await? {
            if Instant::now() > deadline {
                bail!("gave up waiting for {what} after {:?}", self.timeout);
            }
            sleep(POLL).await;
        }
        Ok(())
    }
}

fn loads<'a>(plan: &'a Value, order: &'a str) -> impl Iterator<Item = &'a Value> {
    plan["loads"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(move |l| l["order"] == order)
}

fn str_of<'a>(v: &'a Value, key: &str) -> &'a str {
    v[key].as_str().unwrap_or("?")
}

fn round_up_to_5_minutes(t: DateTime<Utc>) -> DateTime<Utc> {
    let extra = (5 - t.minute() % 5) % 5;
    (t + TimeDelta::minutes(i64::from(extra)))
        .with_second(0)
        .and_then(|t| t.with_nanosecond(0))
        .unwrap_or(t)
}

fn berlin(t: DateTime<Utc>) -> String {
    t.with_timezone(&Berlin).format("%H:%M").to_string()
}

fn time_of(load: &Value, key: &str) -> String {
    load[key]
        .as_str()
        .and_then(|s| s.parse::<DateTime<Utc>>().ok())
        .map_or_else(|| "?".into(), berlin)
}

fn short(plant: &str) -> &str {
    plant.trim_start_matches("plant-")
}

fn print_plan(plan: &Value, ours: &[&str]) {
    println!(
        "   Plan v{}:\n   {:<24} {:<5} {:<10} {:<10} {:<6} {:<6}",
        plan["version"], "order", "load", "truck", "plant", "batch", "ETA"
    );
    for id in ours {
        for l in loads(plan, id) {
            println!(
                "   {:<24} #{:<4} {:<10} {:<10} {:<6} {:<6}",
                id,
                l["sequence"].as_u64().unwrap_or_default(),
                str_of(l, "planned_truck"),
                short(str_of(l, "planned_plant")),
                time_of(l, "planned_batch_at"),
                time_of(l, "eta_on_site")
            );
        }
    }
}

/// `before → after`, or one side when the other is the same or missing.
fn change(
    before: Option<&Value>,
    after: Option<&Value>,
    show: impl Fn(&Value) -> String,
) -> String {
    match (before.map(&show), after.map(&show)) {
        (Some(b), Some(a)) if b != a => format!("{b} → {a}"),
        (_, Some(one)) | (Some(one), None) => one,
        (None, None) => String::new(),
    }
}

/// The ETA column also says when a load was added or removed.
fn eta_change(before: Option<&Value>, after: Option<&Value>) -> String {
    let eta = |l: &Value| time_of(l, "eta_on_site");
    match (before, after) {
        (Some(b), None) => format!("{} → removed", eta(b)),
        (None, Some(a)) => format!("new → {}", eta(a)),
        _ => change(before, after, eta),
    }
}

fn print_diff(version: i64, diff: &Value, ours: &[&str]) {
    let triggers: Vec<&str> = diff["triggers"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    println!("   v{version} · {}", triggers.join("; "));
    println!(
        "   {:<24} {:<5} {:<22} {:<20} {:<15} reason",
        "order", "load", "truck", "plant", "ETA"
    );
    let changes: Vec<&Value> = diff["changes"].as_array().into_iter().flatten().collect();
    let (mine, others): (Vec<&Value>, Vec<&Value>) = changes
        .into_iter()
        .partition(|c| ours.iter().any(|id| c["order"] == *id));
    for c in &mine {
        let (b, a) = (
            c.get("before").filter(|v| !v.is_null()),
            c.get("after").filter(|v| !v.is_null()),
        );
        println!(
            "   {:<24} #{:<4} {:<22} {:<20} {:<15} {}",
            str_of(c, "order"),
            c["sequence"].as_u64().unwrap_or_default(),
            change(b, a, |l| str_of(l, "planned_truck").to_owned()),
            change(b, a, |l| short(str_of(l, "planned_plant")).to_owned()),
            eta_change(b, a),
            str_of(c, "reason")
        );
    }
    if !others.is_empty() {
        println!("   + {} knock-on changes to other orders", others.len());
    }
    let (mb, ma) = (&diff["metrics_before"], &diff["metrics_after"]);
    let num = |m: &Value, k: &str| m[k].as_f64().unwrap_or_default();
    println!(
        "   max gap {:.0} → {:.0} min · gaps over limit {} → {} · lateness {:.0} → {:.0} min",
        num(mb, "max_gap_minutes"),
        num(ma, "max_gap_minutes"),
        mb["gaps_over_limit"],
        ma["gaps_over_limit"],
        num(mb, "lateness_minutes"),
        num(ma, "lateness_minutes")
    );
}
