//! Postgres: master data, orders and plan versions.
//!
//! Every query is checked against the schema at compile time. `.sqlx/` holds the offline data, so CI
//! and Docker builds need no database; `make sqlx-prepare` refreshes it after a query changes.
//! Migrations in `migrations/` run on connect.

use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use domain::{
    Address, CustomerId, Delivery, DeliveryStatus, Load, LoadRef, Location, MixCode,
    OperatingHours, Order, OrderId, OrderStatus, Plant, PlantId, PlantLoad, PlantPlan, PlantStatus,
    PostalCodeGeocoder, Seed, Site, SiteId, TimeWindow, Truck, TruckId, TruckStatus,
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

/// Deliveries older than this are long discharged: they freeze nothing and delay nothing.
pub const DELIVERY_WINDOW: TimeDelta = TimeDelta::hours(12);

/// What a map starts from: each plant's status, each truck's latest reported status and position
/// with their report times, and the latest plan version. Names, sites and capacities are master
/// data, in the seed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct WorldState {
    /// The world clock's time, which `CLOCK_OFFSET_HOURS` shifts like every report time here.
    pub now: DateTime<Utc>,
    /// The latest stored plan version; `None` before the first plan.
    pub plan_version: Option<i64>,
    pub plants: Vec<PlantState>,
    pub trucks: Vec<TruckState>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PlantState {
    pub id: PlantId,
    pub status: PlantStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct TruckState {
    pub id: TruckId,
    /// The seed's status until the first report.
    pub status: TruckStatus,
    /// When the status was reported; `None` before the first report.
    pub status_at: Option<DateTime<Utc>>,
    /// The latest GPS fix; `None` before the first.
    pub position: Option<Location>,
    pub position_at: Option<DateTime<Utc>>,
}

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

    /// The live state of plants and trucks and the latest plan version, as of `now`.
    pub async fn world_state(&self, now: DateTime<Utc>) -> Result<WorldState, StoreError> {
        let plants = sqlx::query!("SELECT id, status FROM plants ORDER BY id")
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|r| {
                Ok(PlantState {
                    id: PlantId(r.id),
                    status: parse(&r.status)?,
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        let trucks = sqlx::query!(
            "SELECT id, status, status_at, lat, lon, position_at FROM trucks ORDER BY id"
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|r| {
            Ok(TruckState {
                id: TruckId(r.id),
                status: parse(&r.status)?,
                status_at: r.status_at,
                position: r.lat.zip(r.lon).map(|(lat, lon)| Location { lat, lon }),
                position_at: r.position_at,
            })
        })
        .collect::<Result<Vec<_>, StoreError>>()?;
        let plan_version = sqlx::query_scalar!("SELECT max(version) FROM plans")
            .fetch_one(&self.pool)
            .await?;
        Ok(WorldState {
            now,
            plan_version,
            plants,
            trucks,
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

    /// Sets a plant online or offline. Returns the status it had, or `None` for an unknown plant.
    pub async fn set_plant_status(
        &self,
        plant: &PlantId,
        status: PlantStatus,
    ) -> Result<Option<PlantStatus>, StoreError> {
        let previous = sqlx::query_scalar!(
            r#"WITH before AS (SELECT id, status FROM plants WHERE id = $1 FOR UPDATE)
               UPDATE plants SET status = $2 FROM before WHERE plants.id = before.id
               RETURNING before.status AS "previous!""#,
            plant.0,
            text(&status),
        )
        .fetch_optional(&self.pool)
        .await?;
        previous.as_deref().map(parse).transpose()
    }

    /// Delays the site's pours by `minutes`: orders that are batching, and open orders starting
    /// between an hour ago and `day_end`, now start at the later of their start and `now`, plus
    /// `minutes`. Returns the orders it delayed, or `None` for an unknown site.
    pub async fn delay_site(
        &self,
        site: &SiteId,
        minutes: u32,
        now: DateTime<Utc>,
        day_end: DateTime<Utc>,
    ) -> Result<Option<Vec<OrderId>>, StoreError> {
        let mut tx = self.pool.begin().await?;
        let known = sqlx::query_scalar!(
            r#"SELECT EXISTS (SELECT 1 FROM sites WHERE id = $1) AS "known!""#,
            site.0,
        )
        .fetch_one(&mut *tx)
        .await?;
        if !known {
            return Ok(None);
        }
        let delayed = sqlx::query_scalar!(
            "UPDATE orders SET delayed_until = GREATEST(requested_start, delayed_until, $2) + make_interval(mins => $3)
             WHERE site_id = $1 AND status <> 'cancelled' AND (
                 (status IN ('unconfirmed', 'pending', 'confirmed')
                     AND GREATEST(requested_start, delayed_until) >= $2::timestamptz - interval '1 hour'
                     AND GREATEST(requested_start, delayed_until) < $4)
                 OR id IN (SELECT order_id FROM deliveries WHERE started_at >= $5))
             RETURNING id",
            site.0,
            now,
            signed(minutes)?,
            day_end,
            now - DELIVERY_WINDOW,
        )
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Some(delayed.into_iter().map(OrderId).collect()))
    }

    /// A version's diff from the one before. `None` without such a version, `Some(None)` for a
    /// version stored before diffs existed.
    pub async fn diff(&self, version: i64) -> Result<Option<Option<Diff>>, StoreError> {
        let found = sqlx::query_scalar!(
            r#"SELECT diff AS "diff: Json<Diff>" FROM plans WHERE version = $1"#,
            version,
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(found.map(|d| d.map(|d| d.0)))
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
            r#"SELECT id AS "id!", customer_id AS "customer_id!", site_id AS "site_id!", mix AS "mix!",
                   total_volume_m3 AS "total_volume_m3!", requested_start AS "requested_start!",
                   pour_rate_m3_per_h AS "pour_rate_m3_per_h!", max_gap_minutes AS "max_gap_minutes!",
                   shown AS "status!"
               FROM shown_orders WHERE id = $1"#,
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
            r#"SELECT id AS "id!", customer_id AS "customer_id!", site_id AS "site_id!", mix AS "mix!",
                   total_volume_m3 AS "total_volume_m3!", requested_start AS "requested_start!",
                   pour_rate_m3_per_h AS "pour_rate_m3_per_h!", max_gap_minutes AS "max_gap_minutes!",
                   shown AS "status!"
               FROM shown_orders
               WHERE ($1::text IS NULL OR shown = $1)
                 AND ($2::timestamptz IS NULL OR requested_start >= $2)
                 AND ($3::timestamptz IS NULL OR requested_start < $3)
               ORDER BY requested_start, id"#,
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

    /// A plant's loads in the latest version, by batch time, each with its order's mix. `None` for
    /// an unknown plant.
    pub async fn plant_plan(&self, plant: &PlantId) -> Result<Option<PlantPlan>, StoreError> {
        let Some(head) = sqlx::query!(
            r#"SELECT v.version AS "version?", v.created_at AS "created_at?"
               FROM plants p LEFT JOIN plans v ON v.version = (SELECT max(version) FROM plans)
               WHERE p.id = $1"#,
            plant.0,
        )
        .fetch_optional(&self.pool)
        .await?
        else {
            return Ok(None);
        };
        let mut loads = Vec::new();
        if let Some(version) = head.version {
            let rows = sqlx::query!(
                "SELECT l.order_id, l.sequence, l.volume_m3, l.plant_id, l.truck_id, l.batch_at, l.eta_on_site,
                     l.window_start, l.window_end, l.frozen, o.mix
                 FROM loads l JOIN orders o ON o.id = l.order_id
                 WHERE l.plan_version = $1 AND l.plant_id = $2
                 ORDER BY l.batch_at, l.order_id, l.sequence",
                version,
                plant.0,
            )
            .fetch_all(&self.pool)
            .await?;
            for r in rows {
                let load = Load::try_from(LoadRow {
                    order_id: r.order_id,
                    sequence: r.sequence,
                    volume_m3: r.volume_m3,
                    plant_id: r.plant_id,
                    truck_id: r.truck_id,
                    batch_at: r.batch_at,
                    eta_on_site: r.eta_on_site,
                    window_start: r.window_start,
                    window_end: r.window_end,
                    frozen: r.frozen,
                })?;
                loads.push(PlantLoad {
                    load,
                    mix: MixCode(r.mix),
                });
            }
        }
        Ok(Some(PlantPlan {
            plant: plant.clone(),
            version: head.version,
            created_at: head.created_at,
            loads,
        }))
    }
}
