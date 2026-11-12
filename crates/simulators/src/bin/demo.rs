//! `make demo`: a scripted re-dispatch scenario against the running stack (SPEC §9).
//!
//! 1. Two orders arrive through the customers' own channels: a bauwerk-ag JSON webhook and a
//!    neckar-infra XML webhook. Dispatch plans them.
//! 2. The demo plays the plant: batch tickets for load #1 of the first order, so it freezes.
//! 3. The truck of a later load breaks down. The simulated truck reports `out_of_service` over
//!    MQTT like any other status, and dispatch re-dispatches.
//! 4. The second order's site is delayed 30 minutes through the dispatch API.
//!
//! It prints the plan and every diff, then links to the first order's trace in Jaeger and to the
//! Grafana dashboard. Settings come from environment variables; the defaults reach a local stack,
//! and compose's `demo` service points them at the containers.
//!
//! `demo edge` is `make chaos`'s first step instead: three orders at the ship-tos nearest
//! Walldorf, starting as soon as possible, so Walldorf's bays are busy for the next minutes. It
//! ends with `PLAN_VERSION=<n>`, the version that planned them.

use std::time::Duration;

use anyhow::{Context, bail};
use chrono::{DateTime, TimeDelta, Timelike, Utc};
use chrono_tz::Europe::Berlin;
use domain::{DeliveryStatus, MixCode, OrderId, PlantId, PlantTicket, Seed, Site, TruckId};
use reqwest::header::CONTENT_TYPE;
use rumqttc::{AsyncClient, MqttOptions, QoS};
use serde_json::{Value, json};
use simulators::customers::{bauwerk_json, neckar_version, neckar_xml};
use simulators::orders::GeneratedOrder;
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
    /// The customers' webhooks, under the simulators' variable names. In Kubernetes each webhook
    /// integration has its own Service, so there is no single ingest URL.
    bauwerk_webhook: String,
    neckar_webhook: String,
    /// Where a person reaches Jaeger and Grafana, for the closing links.
    jaeger_public: String,
    grafana_public: String,
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
        bauwerk_webhook: env(
            "BAUWERK_WEBHOOK_URL",
            "http://localhost:8080/webhooks/bauwerk-ag",
        ),
        neckar_webhook: env(
            "NECKAR_WEBHOOK_URL",
            "http://localhost:8080/webhooks/neckar-infra",
        ),
        jaeger_public: env("JAEGER_PUBLIC_URL", "http://localhost:16686"),
        grafana_public: env("GRAFANA_PUBLIC_URL", "http://localhost:3000"),
        mqtt,
        timeout: Duration::from_secs(env("DEMO_TIMEOUT_SECS", "120").parse()?),
    };
    match std::env::args().nth(1).as_deref() {
        None => demo.run().await?,
        Some("edge") => demo.edge().await?,
        Some(other) => bail!("unknown scenario {other:?}: run demo, or demo edge"),
    }
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
        let trace_a = self
            .post_order("bauwerk-ag", "application/json", bauwerk_json(&a)?)
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
        if let Some(trace) = trace_a {
            println!(
                "Trace of {id_a}, from the webhook to the re-dispatch that planned it: {}/trace/{trace}",
                self.jaeger_public
            );
        }
        println!(
            "Metrics: the Dispatch overview dashboard at {}",
            self.grafana_public
        );
        Ok(())
    }

    /// `demo edge`: orders that keep Walldorf's bays busy during `make chaos`'s cut. Simulated
    /// orders start 2 to 26 hours ahead, so without these the plant would have nothing to batch.
    async fn edge(&self) -> anyhow::Result<()> {
        const PLANT: &str = "plant-walldorf";
        self.wait_until_up().await?;
        let seed = Seed::rhine_neckar()?;
        let walldorf = seed.plant(PLANT).context("no plant-walldorf")?.location;
        // The webhook customers: bauwerk-ag (JSON) and neckar-infra (XML).
        let neckar_sites: Vec<String> = seed
            .sites_of("10001003")
            .iter()
            .map(|s| s.id.0.clone())
            .collect();
        let mut nearest: Vec<(f64, &Site)> = seed
            .sites
            .iter()
            .filter(|s| ["10001001", "10001003"].contains(&s.customer.0.as_str()))
            .filter_map(|s| Some((seed.geocoder.locate(s)?.distance_km(walldorf), s)))
            .collect();
        nearest.sort_by(|a, b| a.0.total_cmp(&b.0));

        let now = Utc::now();
        // Already too late to arrive on time, so dispatch batches the first loads right away.
        let start = now + TimeDelta::minutes(5);
        let stamp = now.with_timezone(&Berlin).format("%H%M%S");
        let before = self
            .get("/plans/latest")
            .await?
            .and_then(|p| p["version"].as_i64())
            .unwrap_or(0);
        let mut ids = Vec::new();
        for (i, (km, site)) in nearest.iter().take(3).enumerate() {
            let order = GeneratedOrder {
                order_ref: format!("EDGE-{stamp}-{}", i + 1),
                customer: site.customer.0.clone(),
                ship_to: site.id.0.clone(),
                mix: MIX.into(),
                volume_m3: 27.0,
                start,
                pour_rate_m3_per_h: 60,
                max_gap_minutes: 20,
                revision: 1,
                updated_at: now,
                cancelled: false,
            };
            if order.customer == "10001001" {
                self.post_order("bauwerk-ag", "application/json", bauwerk_json(&order)?)
                    .await?;
            } else {
                let version = neckar_version(&order.ship_to, &neckar_sites);
                self.post_order(
                    "neckar-infra",
                    "application/xml",
                    neckar_xml(&order, version)?,
                )
                .await?;
            }
            let id = format!("{}/{}", order.customer, order.order_ref);
            println!(
                "  {id}: 27 m³ for {} ({km:.1} km from Walldorf), as soon as possible",
                site.name
            );
            ids.push(id);
        }
        let planned = self
            .next_version(before, |p| {
                ids.iter().all(|id| {
                    loads(p, id).next().is_some()
                        || p["unplanned"]
                            .as_array()
                            .is_some_and(|u| u.iter().any(|x| x["order"] == *id))
                })
            })
            .await?;
        // Any order's loads keep the bays busy, and an earlier run's orders may already fill them.
        let soon = Utc::now() + TimeDelta::minutes(15);
        let at_walldorf = planned["loads"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|l| l["planned_plant"] == PLANT && l["frozen"] == false)
            .filter(|l| {
                l["planned_batch_at"]
                    .as_str()
                    .and_then(|s| s.parse::<DateTime<Utc>>().ok())
                    .is_some_and(|at| at < soon)
            })
            .count();
        if at_walldorf == 0 {
            bail!("dispatch planned no loads at {PLANT} for the next 15 minutes");
        }
        let version = planned["version"].as_i64().unwrap_or_default();
        println!("  plan v{version}: {at_walldorf} loads at {PLANT} batch within 15 minutes");
        println!("PLAN_VERSION={version}");
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
        .await?;
        // Any answer will do, even 405: in Kubernetes a webhook's Service can take a moment to
        // route to a pod that just became ready.
        for url in [&self.bauwerk_webhook, &self.neckar_webhook] {
            self.wait_for(&format!("{url} to answer"), || async {
                Ok(self.http.get(url).send().await.is_ok())
            })
            .await?;
        }
        Ok(())
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
            let ticket = PlantTicket {
                plant: PlantId(plant.to_owned()),
                ticket_no: ticket_no.clone(),
                vehicle: TruckId(str_of(load, "planned_truck").to_owned()),
                ts_ms: batch_at + i * 1000,
                event,
                mix: MixCode(MIX.to_owned()),
                volume_m3: load["volume_m3"].as_f64().unwrap_or_default(),
                order: Some(OrderId(order.to_owned())),
                load_no: load["sequence"]
                    .as_u64()
                    .and_then(|n| u32::try_from(n).ok()),
            };
            self.mqtt
                .publish(
                    ticket.topic(),
                    QoS::AtLeastOnce,
                    false,
                    serde_json::to_vec(&ticket)?,
                )
                .await?;
        }
        Ok(())
    }

    /// Posts an order like the customer's system does, and returns the delivery's trace id.
    async fn post_order(
        &self,
        integration: &str,
        content_type: &str,
        body: String,
    ) -> anyhow::Result<Option<String>> {
        let token_key = format!(
            "WEBHOOK_TOKEN_{}",
            integration.to_uppercase().replace('-', "_")
        );
        let token = env(&token_key, &format!("dev-token-{integration}"));
        let url = match integration {
            "bauwerk-ag" => &self.bauwerk_webhook,
            "neckar-infra" => &self.neckar_webhook,
            other => bail!("the demo has no webhook URL for {other}"),
        };
        let response = self
            .http
            .post(url)
            .bearer_auth(token)
            .header(CONTENT_TYPE, content_type)
            .body(body)
            .send()
            .await
            .with_context(|| format!("posting to ingest at {url}"))?;
        if !response.status().is_success() {
            bail!(
                "ingest refused the {integration} order: {} {}",
                response.status(),
                response.text().await.unwrap_or_default()
            );
        }
        let accepted: Value = serde_json::from_str(&response.text().await?)?;
        Ok(accepted["trace_id"].as_str().map(str::to_owned))
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
