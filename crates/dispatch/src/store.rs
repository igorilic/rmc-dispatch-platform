//! Postgres: master data, orders and plan versions.
//!
//! Every query is checked against the schema at compile time. `.sqlx/` holds the offline data, so CI
//! and Docker builds need no database; `make sqlx-prepare` refreshes it after a query changes.
//! Migrations in `migrations/` run on connect.

use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use domain::{
    Address, CustomerId, Delivery, DeliveryStatus, Load, LoadRef, Location, MixCode,
    OperatingHours, Order, OrderId, OrderStatus, Plant, PlantId, PostalCodeGeocoder, Seed, Site,
    SiteId, TimeWindow, Truck, TruckId, TruckStatus,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use sqlx::types::Json;
use tracing::warn;
use utoipa::ToSchema;

use crate::planner::{Metrics, Plan, Unplanned, World};
use crate::redispatch::{Diff, Trigger};

/// A stored plan: its version, what it replaced, and why.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct VersionedPlan {
    pub version: i64,
    /// The version this one replaced; `None` for the first.
    pub previous_version: Option<i64>,
    /// What made dispatch plan again.
    pub triggers: Vec<Trigger>,
    #[serde(flatten)]
    pub plan: Plan,
}

/// What [`Store::upsert_order`] did.
#[derive(Debug, Clone, PartialEq)]
pub enum Upsert {
    New,
    /// Replaced; holds the order as it was.
    Changed(Order),
    /// What is stored is newer, so nothing changed.
    Stale,
}

/// A batch ticket for a planned load, as the fleet intake records it.
#[derive(Debug, Clone, PartialEq)]
pub struct Ticket<'a> {
    pub load: &'a LoadRef,
    pub plant: &'a PlantId,
    pub truck: &'a TruckId,
    pub volume_m3: f64,
    pub ticket_no: &'a str,
    pub event: DeliveryStatus,
    pub at: DateTime<Utc>,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("database: {0}")]
    Db(#[from] sqlx::Error),
    #[error("migration: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("stored data is invalid: {0}")]
    Invalid(String),
}

/// Statuses are stored as their snake_case wire names.
fn text<T: Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_default()
}

fn parse<T: DeserializeOwned>(text: &str) -> Result<T, StoreError> {
    serde_json::from_value(serde_json::Value::String(text.to_owned()))
        .map_err(|_| StoreError::Invalid(format!("unexpected value {text:?}")))
}

fn unsigned(n: i32) -> Result<u32, StoreError> {
    u32::try_from(n).map_err(|_| StoreError::Invalid(format!("negative count {n}")))
}

fn signed(n: u32) -> Result<i32, StoreError> {
    i32::try_from(n).map_err(|_| StoreError::Invalid(format!("{n} is too large")))
}

struct OrderRow {
    id: String,
    customer_id: String,
    site_id: String,
    mix: String,
    total_volume_m3: f64,
    requested_start: DateTime<Utc>,
    pour_rate_m3_per_h: f64,
    max_gap_minutes: i32,
    status: String,
}

impl TryFrom<OrderRow> for Order {
    type Error = StoreError;

    fn try_from(r: OrderRow) -> Result<Order, StoreError> {
        Ok(Order {
            id: OrderId(r.id),
            customer: CustomerId(r.customer_id),
            site: SiteId(r.site_id),
            mix: MixCode(r.mix),
            total_volume_m3: r.total_volume_m3,
            requested_start: r.requested_start,
            pour_rate_m3_per_h: r.pour_rate_m3_per_h,
            max_gap_minutes: unsigned(r.max_gap_minutes)?,
            status: parse(&r.status)?,
        })
    }
}

struct LoadRow {
    order_id: String,
    sequence: i32,
    volume_m3: f64,
    plant_id: String,
    truck_id: String,
    batch_at: DateTime<Utc>,
    eta_on_site: DateTime<Utc>,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    frozen: bool,
}

impl TryFrom<LoadRow> for Load {
    type Error = StoreError;

    fn try_from(r: LoadRow) -> Result<Load, StoreError> {
        Ok(Load {
            order: OrderId(r.order_id),
            sequence: unsigned(r.sequence)?,
            volume_m3: r.volume_m3,
            planned_plant: PlantId(r.plant_id),
            planned_truck: TruckId(r.truck_id),
            planned_batch_at: r.batch_at,
            eta_on_site: r.eta_on_site,
            discharge_window: TimeWindow {
                start: r.window_start,
                end: r.window_end,
            },
            frozen: r.frozen,
        })
    }
}

#[derive(Clone)]
pub struct Store {
    pool: PgPool,
}

impl Store {
    /// Connects, retrying while the database starts, and runs pending migrations.
    pub async fn connect(url: &str) -> Result<Store, StoreError> {
        let mut delay = Duration::from_millis(200);
        let pool = loop {
            match PgPoolOptions::new().max_connections(10).connect(url).await {
                Ok(pool) => break pool,
                Err(error) if delay < Duration::from_secs(10) => {
                    warn!(%error, "Postgres not reachable yet, retrying");
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                }
                Err(error) => return Err(error.into()),
            }
        };
        sqlx::migrate!().run(&pool).await?;
        Ok(Store { pool })
    }

    /// Upserts all master data in one transaction. Truck and plant status are only set the first
    /// time, so operational changes survive a restart.
    pub async fn import_master_data(&self, seed: &Seed) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        for c in &seed.customers {
            sqlx::query!(
                "INSERT INTO customers (id, name, address) VALUES ($1, $2, $3)
                 ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, address = EXCLUDED.address",
                c.id.0,
                c.name,
                Json(&c.address) as _,
            )
            .execute(&mut *tx)
            .await?;
        }
        for (code, at) in seed.geocoder.centroids() {
            sqlx::query!(
                "INSERT INTO postal_codes (code, lat, lon) VALUES ($1, $2, $3)
                 ON CONFLICT (code) DO UPDATE SET lat = EXCLUDED.lat, lon = EXCLUDED.lon",
                code,
                at.lat,
                at.lon,
            )
            .execute(&mut *tx)
            .await?;
        }
        for s in &seed.sites {
            sqlx::query!(
                "INSERT INTO sites (id, customer_id, name, address, lat, lon, access_notes)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (id) DO UPDATE SET customer_id = EXCLUDED.customer_id, name = EXCLUDED.name,
                     address = EXCLUDED.address, lat = EXCLUDED.lat, lon = EXCLUDED.lon,
                     access_notes = EXCLUDED.access_notes",
                s.id.0,
                s.customer.0,
                s.name,
                s.address.as_ref().map(Json) as _,
                s.location.map(|l| l.lat),
                s.location.map(|l| l.lon),
                s.access_notes,
            )
            .execute(&mut *tx)
            .await?;
        }
        for m in &seed.mixes {
            sqlx::query!(
                "INSERT INTO mix_designs (code, max_minutes_from_batching) VALUES ($1, $2)
                 ON CONFLICT (code) DO UPDATE SET max_minutes_from_batching = EXCLUDED.max_minutes_from_batching",
                m.code.0,
                signed(m.max_minutes_from_batching)?,
            )
            .execute(&mut *tx)
            .await?;
        }
        for p in &seed.plants {
            sqlx::query!(
                "INSERT INTO plants (id, lat, lon, loading_bays, batching_rate_m3_per_h, opens, closes, status)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                 ON CONFLICT (id) DO UPDATE SET lat = EXCLUDED.lat, lon = EXCLUDED.lon,
                     loading_bays = EXCLUDED.loading_bays, batching_rate_m3_per_h = EXCLUDED.batching_rate_m3_per_h,
                     opens = EXCLUDED.opens, closes = EXCLUDED.closes",
                p.id.0,
                p.location.lat,
                p.location.lon,
                signed(p.loading_bays)?,
                p.batching_rate_m3_per_h,
                p.operating_hours.open,
                p.operating_hours.close,
                text(&p.status),
            )
            .execute(&mut *tx)
            .await?;
        }
        for t in &seed.trucks {
            sqlx::query!(
                "INSERT INTO trucks (id, home_plant, drum_capacity_m3, status) VALUES ($1, $2, $3, $4)
                 ON CONFLICT (id) DO UPDATE SET home_plant = EXCLUDED.home_plant,
                     drum_capacity_m3 = EXCLUDED.drum_capacity_m3",
                t.id.0,
                t.home_plant.0,
                t.drum_capacity_m3,
                text(&t.status),
            )
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Master data as the planner needs it, with every ship-to located.
    pub async fn world(&self) -> Result<World, StoreError> {
        let plants = sqlx::query!(
            "SELECT id, lat, lon, loading_bays, batching_rate_m3_per_h, opens, closes, status FROM plants ORDER BY id"
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|r| {
            Ok(Plant {
                id: PlantId(r.id),
                location: Location { lat: r.lat, lon: r.lon },
                loading_bays: unsigned(r.loading_bays)?,
                batching_rate_m3_per_h: r.batching_rate_m3_per_h,
                operating_hours: OperatingHours { open: r.opens, close: r.closes },
                status: parse(&r.status)?,
            })
        })
        .collect::<Result<Vec<_>, StoreError>>()?;
        let trucks = sqlx::query!(
            "SELECT id, home_plant, drum_capacity_m3, status, lat, lon FROM trucks ORDER BY id"
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|r| {
            Ok(Truck {
                id: TruckId(r.id),
                home_plant: PlantId(r.home_plant),
                drum_capacity_m3: r.drum_capacity_m3,
                status: parse(&r.status)?,
                last_position: r.lat.zip(r.lon).map(|(lat, lon)| Location { lat, lon }),
            })
        })
        .collect::<Result<Vec<_>, StoreError>>()?;
        let geocoder = PostalCodeGeocoder::new(
            sqlx::query!("SELECT code, lat, lon FROM postal_codes")
                .fetch_all(&self.pool)
                .await?
                .into_iter()
                .map(|r| {
                    (
                        r.code,
                        Location {
                            lat: r.lat,
                            lon: r.lon,
                        },
                    )
                })
                .collect(),
        );
        let sites = sqlx::query!(
            r#"SELECT id, customer_id, name, address AS "address: Json<Address>", lat, lon, access_notes FROM sites"#
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .filter_map(|r| {
            let site = Site {
                id: SiteId(r.id),
                customer: CustomerId(r.customer_id),
                name: r.name,
                address: r.address.map(|a| a.0),
                location: r.lat.zip(r.lon).map(|(lat, lon)| Location { lat, lon }),
                access_notes: r.access_notes,
            };
            geocoder.locate(&site).map(|at| (site.id, at))
        })
        .collect();
        let mixes = sqlx::query!("SELECT code, max_minutes_from_batching FROM mix_designs")
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|r| Ok((MixCode(r.code), unsigned(r.max_minutes_from_batching)?)))
            .collect::<Result<_, StoreError>>()?;
        Ok(World {
            plants,
            trucks,
            sites,
            mixes,
        })
    }

    /// Inserts or replaces an order, unless what is stored is newer.
    pub async fn upsert_order(
        &self,
        order: &Order,
        occurred_at: DateTime<Utc>,
        source_revision: Option<&str>,
    ) -> Result<Upsert, StoreError> {
        let mut tx = self.pool.begin().await?;
        let before = sqlx::query_as!(
            OrderRow,
            "SELECT id, customer_id, site_id, mix, total_volume_m3, requested_start, pour_rate_m3_per_h,
                 max_gap_minutes, status FROM orders WHERE id = $1 FOR UPDATE",
            order.id.0,
        )
        .fetch_optional(&mut *tx)
        .await?
        .map(Order::try_from)
        .transpose()?;
        let result = sqlx::query!(
            "INSERT INTO orders (id, customer_id, site_id, mix, total_volume_m3, requested_start,
                 pour_rate_m3_per_h, max_gap_minutes, status, occurred_at, source_revision)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
             ON CONFLICT (id) DO UPDATE SET customer_id = EXCLUDED.customer_id, site_id = EXCLUDED.site_id,
                 mix = EXCLUDED.mix, total_volume_m3 = EXCLUDED.total_volume_m3,
                 requested_start = EXCLUDED.requested_start, pour_rate_m3_per_h = EXCLUDED.pour_rate_m3_per_h,
                 max_gap_minutes = EXCLUDED.max_gap_minutes, status = EXCLUDED.status,
                 occurred_at = EXCLUDED.occurred_at, source_revision = EXCLUDED.source_revision
             WHERE orders.occurred_at <= EXCLUDED.occurred_at",
            order.id.0,
            order.customer.0,
            order.site.0,
            order.mix.0,
            order.total_volume_m3,
            order.requested_start,
            order.pour_rate_m3_per_h,
            signed(order.max_gap_minutes)?,
            text(&order.status),
            occurred_at,
            source_revision,
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(match (result.rows_affected(), before) {
            (0, _) => Upsert::Stale,
            (_, None) => Upsert::New,
            (_, Some(before)) => Upsert::Changed(before),
        })
    }

    /// Records a truck's reported status, unless a newer report is stored. Returns the status it
    /// replaced, if it applied.
    pub async fn report_truck_status(
        &self,
        truck: &TruckId,
        status: TruckStatus,
        at: DateTime<Utc>,
    ) -> Result<Option<TruckStatus>, StoreError> {
        let previous = sqlx::query_scalar!(
            r#"WITH before AS (SELECT id, status FROM trucks WHERE id = $1 FOR UPDATE)
               UPDATE trucks SET status = $2, status_at = $3 FROM before
               WHERE trucks.id = before.id AND (trucks.status_at IS NULL OR trucks.status_at < $3)
               RETURNING before.status AS "previous!""#,
            truck.0,
            text(&status),
            at,
        )
        .fetch_optional(&self.pool)
        .await?;
        previous.as_deref().map(parse).transpose()
    }

    /// Records a truck's reported position, unless a newer one is stored.
    pub async fn report_truck_position(
        &self,
        truck: &TruckId,
        at_location: Location,
        at: DateTime<Utc>,
    ) -> Result<bool, StoreError> {
        let result = sqlx::query!(
            "UPDATE trucks SET lat = $2, lon = $3, position_at = $4
             WHERE id = $1 AND (position_at IS NULL OR position_at < $4)",
            truck.0,
            at_location.lat,
            at_location.lon,
            at,
        )
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Creates or advances the delivery for a ticket's load. The first report of each status
    /// wins, so duplicates and late arrivals change nothing. Returns whether the delivery is new.
    pub async fn record_ticket(&self, t: &Ticket<'_>) -> Result<bool, StoreError> {
        let inserted = sqlx::query_scalar!(
            r#"INSERT INTO deliveries (order_id, sequence, plant_id, truck_id, volume_m3, ticket_no, started_at, events)
               VALUES ($1, $2, $3, $4, $5, $6, $7, jsonb_build_object($8::text, $7::timestamptz))
               ON CONFLICT (order_id, sequence) DO UPDATE SET
                   started_at = LEAST(deliveries.started_at, EXCLUDED.started_at),
                   events = EXCLUDED.events || deliveries.events
               RETURNING (xmax = 0) AS "inserted!""#,
            t.load.order.0,
            signed(t.load.sequence)?,
            t.plant.0,
            t.truck.0,
            t.volume_m3,
            t.ticket_no,
            t.at,
            text(&t.event),
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(inserted)
    }

    /// Deliveries whose batching started at or after `since`.
    pub async fn deliveries(&self, since: DateTime<Utc>) -> Result<Vec<Delivery>, StoreError> {
        sqlx::query!(
            r#"SELECT order_id, sequence, plant_id, truck_id, volume_m3,
                   events AS "events: Json<BTreeMap<DeliveryStatus, DateTime<Utc>>>"
               FROM deliveries WHERE started_at >= $1 ORDER BY order_id, sequence"#,
            since,
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|r| {
            Ok(Delivery {
                order: OrderId(r.order_id),
                sequence: unsigned(r.sequence)?,
                plant: PlantId(r.plant_id),
                truck: TruckId(r.truck_id),
                volume_m3: r.volume_m3,
                events: r.events.0,
            })
        })
        .collect()
    }

    /// Cancels an order, unless what is stored is newer. An unknown order has nothing to cancel.
    pub async fn cancel_order(
        &self,
        id: &str,
        occurred_at: DateTime<Utc>,
    ) -> Result<bool, StoreError> {
        let result = sqlx::query!(
            "UPDATE orders SET status = 'cancelled', occurred_at = $2 WHERE id = $1 AND occurred_at <= $2",
            id,
            occurred_at,
        )
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn order(&self, id: &str) -> Result<Option<Order>, StoreError> {
        sqlx::query_as!(
            OrderRow,
            "SELECT id, customer_id, site_id, mix, total_volume_m3, requested_start, pour_rate_m3_per_h,
                 max_gap_minutes, status FROM orders WHERE id = $1",
            id,
        )
        .fetch_optional(&self.pool)
        .await?
        .map(Order::try_from)
        .transpose()
    }

    /// Orders by start, optionally only one status and only starting in `[from, to)`.
    pub async fn orders(
        &self,
        status: Option<OrderStatus>,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
    ) -> Result<Vec<Order>, StoreError> {
        sqlx::query_as!(
            OrderRow,
            "SELECT id, customer_id, site_id, mix, total_volume_m3, requested_start, pour_rate_m3_per_h,
                 max_gap_minutes, status FROM orders
             WHERE ($1::text IS NULL OR status = $1)
               AND ($2::timestamptz IS NULL OR requested_start >= $2)
               AND ($3::timestamptz IS NULL OR requested_start < $3)
             ORDER BY requested_start, id",
            status.as_ref().map(text),
            from,
            to,
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(Order::try_from)
        .collect()
    }

    /// The orders a planning run needs: open ones starting in `[from, to)`, plus those in `also`
    /// (orders with deliveries or in the previous plan). A site delay moves an order's start: the
    /// planner sees the delayed start.
    pub async fn planning_orders(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        also: &[String],
    ) -> Result<Vec<Order>, StoreError> {
        sqlx::query_as!(
            OrderRow,
            r#"SELECT id, customer_id, site_id, mix, total_volume_m3, starts AS "requested_start!",
                   pour_rate_m3_per_h, max_gap_minutes, status
               FROM (SELECT *, GREATEST(requested_start, delayed_until) AS starts FROM orders) o
               WHERE (status IN ('unconfirmed', 'pending', 'confirmed') AND starts >= $1 AND starts < $2)
                  OR id = ANY($3)
               ORDER BY starts, id"#,
            from,
            to,
            also,
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(Order::try_from)
        .collect()
    }

    /// Stores a plan as the next version, with what it replaced, why, and the diff. Returns the
    /// version.
    pub async fn save_plan(
        &self,
        plan: &Plan,
        previous: Option<i64>,
        triggers: &[Trigger],
        diff: &Diff,
    ) -> Result<i64, StoreError> {
        let mut tx = self.pool.begin().await?;
        let version = sqlx::query_scalar!(
            "INSERT INTO plans (created_at, horizon_end, metrics, unplanned, previous_version, triggers, diff)
             VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING version",
            plan.created_at,
            plan.horizon_end,
            Json(&plan.metrics) as _,
            Json(&plan.unplanned) as _,
            previous,
            Json(triggers) as _,
            Json(diff) as _,
        )
        .fetch_one(&mut *tx)
        .await?;
        // ponytail: one insert per load; switch to UNNEST if M5's 1,000-load plans make saving slow.
        for l in &plan.loads {
            sqlx::query!(
                "INSERT INTO loads (plan_version, order_id, sequence, volume_m3, plant_id, truck_id, batch_at,
                     eta_on_site, window_start, window_end, frozen) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
                version,
                l.order.0,
                signed(l.sequence)?,
                l.volume_m3,
                l.planned_plant.0,
                l.planned_truck.0,
                l.planned_batch_at,
                l.eta_on_site,
                l.discharge_window.start,
                l.discharge_window.end,
                l.frozen,
            )
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(version)
    }

    /// A plan version, or the latest when `version` is `None`. Loads come by order, then sequence.
    pub async fn plan(&self, version: Option<i64>) -> Result<Option<VersionedPlan>, StoreError> {
        let Some(row) = sqlx::query!(
            r#"SELECT version, created_at, horizon_end, metrics AS "metrics: Json<Metrics>",
                   unplanned AS "unplanned: Json<Vec<Unplanned>>", previous_version,
                   triggers AS "triggers: Json<Vec<Trigger>>"
               FROM plans WHERE version = COALESCE($1, (SELECT max(version) FROM plans))"#,
            version,
        )
        .fetch_optional(&self.pool)
        .await?
        else {
            return Ok(None);
        };
        let loads = sqlx::query_as!(
            LoadRow,
            "SELECT order_id, sequence, volume_m3, plant_id, truck_id, batch_at, eta_on_site, window_start, window_end,
                 frozen FROM loads WHERE plan_version = $1 ORDER BY order_id, sequence",
            row.version,
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(Load::try_from)
        .collect::<Result<_, _>>()?;
        Ok(Some(VersionedPlan {
            version: row.version,
            previous_version: row.previous_version,
            triggers: row.triggers.0,
            plan: Plan {
                created_at: row.created_at,
                horizon_end: row.horizon_end,
                loads,
                unplanned: row.unplanned.0,
                metrics: row.metrics.0,
            },
        }))
    }
}
