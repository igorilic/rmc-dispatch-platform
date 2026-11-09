//! Postgres: master data, orders and plan versions.
//!
//! Every query is checked against the schema at compile time. `.sqlx/` holds the offline data, so CI
//! and Docker builds need no database; `make sqlx-prepare` refreshes it after a query changes.
//! Migrations in `migrations/` run on connect.

use std::time::Duration;

use chrono::{DateTime, Utc};
use domain::{
    Address, CustomerId, Load, Location, MixCode, OperatingHours, Order, OrderId, OrderStatus,
    Plant, PlantId, PostalCodeGeocoder, Seed, Site, SiteId, TimeWindow, Truck, TruckId,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use sqlx::types::Json;
use tracing::warn;

use crate::planner::{Metrics, Plan, Unplanned, World};

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
            // ponytail: not stored until M5 step 5 adds deliveries and the column.
            frozen: false,
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
        let trucks =
            sqlx::query!("SELECT id, home_plant, drum_capacity_m3, status FROM trucks ORDER BY id")
                .fetch_all(&self.pool)
                .await?
                .into_iter()
                .map(|r| {
                    Ok(Truck {
                        id: TruckId(r.id),
                        home_plant: PlantId(r.home_plant),
                        drum_capacity_m3: r.drum_capacity_m3,
                        status: parse(&r.status)?,
                        last_position: None,
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

    /// Inserts or replaces an order, unless what is stored is newer. Returns whether it was applied.
    pub async fn upsert_order(
        &self,
        order: &Order,
        occurred_at: DateTime<Utc>,
        source_revision: Option<&str>,
    ) -> Result<bool, StoreError> {
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
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
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

    /// Orders that may still be planned (not cancelled, not started) starting in `[from, to)`.
    pub async fn open_orders(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<Order>, StoreError> {
        sqlx::query_as!(
            OrderRow,
            "SELECT id, customer_id, site_id, mix, total_volume_m3, requested_start, pour_rate_m3_per_h,
                 max_gap_minutes, status FROM orders
             WHERE status IN ('unconfirmed', 'pending', 'confirmed') AND requested_start >= $1 AND requested_start < $2
             ORDER BY requested_start, id",
            from,
            to,
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(Order::try_from)
        .collect()
    }

    /// Stores a plan as the next version and returns the version.
    pub async fn save_plan(&self, plan: &Plan) -> Result<i64, StoreError> {
        let mut tx = self.pool.begin().await?;
        let version = sqlx::query_scalar!(
            "INSERT INTO plans (created_at, horizon_end, metrics, unplanned) VALUES ($1, $2, $3, $4) RETURNING version",
            plan.created_at,
            plan.horizon_end,
            Json(&plan.metrics) as _,
            Json(&plan.unplanned) as _,
        )
        .fetch_one(&mut *tx)
        .await?;
        // ponytail: one insert per load; switch to UNNEST if M5's 1,000-load plans make saving slow.
        for l in &plan.loads {
            sqlx::query!(
                "INSERT INTO loads (plan_version, order_id, sequence, volume_m3, plant_id, truck_id, batch_at,
                     eta_on_site, window_start, window_end) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
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
            )
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(version)
    }

    /// A plan version, or the latest when `version` is `None`. Loads come by order, then sequence.
    pub async fn plan(&self, version: Option<i64>) -> Result<Option<(i64, Plan)>, StoreError> {
        let Some(row) = sqlx::query!(
            r#"SELECT version, created_at, horizon_end, metrics AS "metrics: Json<Metrics>",
                   unplanned AS "unplanned: Json<Vec<Unplanned>>"
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
            "SELECT order_id, sequence, volume_m3, plant_id, truck_id, batch_at, eta_on_site, window_start, window_end
             FROM loads WHERE plan_version = $1 ORDER BY order_id, sequence",
            row.version,
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(Load::try_from)
        .collect::<Result<_, _>>()?;
        let plan = Plan {
            created_at: row.created_at,
            horizon_end: row.horizon_end,
            loads,
            unplanned: row.unplanned.0,
            metrics: row.metrics.0,
        };
        Ok(Some((row.version, plan)))
    }
}
