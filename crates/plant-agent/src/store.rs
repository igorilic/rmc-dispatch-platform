//! The agent's SQLite file, with three tables:
//!
//! - `outbox`: tickets waiting to be sent, oldest first. A ticket is serialised once, when it is
//!   queued, and the forwarder sends exactly those bytes. A resend after a lost acknowledgement is
//!   therefore the same message, and ingest derives the same event id from it.
//! - `batched`: every load this plant has started, with the last ticket it issued. A load in here
//!   is never batched again, and one that was cut short by a restart resumes where it stopped.
//! - `plan`: the last slice of the plan from dispatch, and when dispatch last confirmed it, so a
//!   restarted agent that can't reach the cloud still has a plan.
//!
//! A ticket and its load's new stage are written in one transaction, so a crash leaves both or
//! neither. SQLite's default rollback journal runs with `synchronous = FULL`, so a commit is on disk
//! before it returns.

use std::path::Path;
use std::sync::{Mutex, MutexGuard, PoisonError};

use chrono::{DateTime, Utc};
use domain::{DeliveryStatus, LoadRef, MixCode, OrderId, PlantPlan, TruckId};
use rusqlite::{Connection, OptionalExtension};

use crate::Synced;

// ponytail: no migrations; the schema is created if missing. Add `PRAGMA user_version` steps once a
// released agent's file has to change shape. `batched` grows by one row per load, a few hundred a
// day; prune old rows if a plant ever runs for years without a reset.
const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS outbox (
    id        INTEGER PRIMARY KEY AUTOINCREMENT, -- never reused, so ids follow queue order
    topic     TEXT NOT NULL,
    payload   BLOB NOT NULL,
    queued_at INTEGER NOT NULL                   -- Unix milliseconds
);
CREATE TABLE IF NOT EXISTS batched (
    order_id   TEXT NOT NULL,
    sequence   INTEGER NOT NULL,
    ticket_seq INTEGER NOT NULL UNIQUE,          -- the number in the ticket number
    truck      TEXT NOT NULL,
    mix        TEXT NOT NULL,
    volume_m3  REAL NOT NULL,
    started_at INTEGER NOT NULL,                 -- when batching started, Unix milliseconds
    stage      TEXT NOT NULL,                    -- the last ticket issued: batching, loading, loaded
    PRIMARY KEY (order_id, sequence)
);
CREATE TABLE IF NOT EXISTS plan (
    id        INTEGER PRIMARY KEY CHECK (id = 1),
    slice     TEXT NOT NULL,                     -- the PlantPlan as dispatch sent it
    synced_at INTEGER NOT NULL                   -- Unix milliseconds
);
";

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("SQLite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("stored JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("stored time {0} is out of range")]
    Time(i64),
}

/// A ticket waiting in the outbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Queued {
    pub id: i64,
    pub topic: String,
    pub payload: Vec<u8>,
    pub queued_at: DateTime<Utc>,
}

/// A load this plant has started, and how far it got.
#[derive(Debug, Clone, PartialEq)]
pub struct Started {
    pub load: LoadRef,
    pub ticket_seq: i64,
    pub truck: TruckId,
    pub mix: MixCode,
    pub volume_m3: f64,
    pub started_at: DateTime<Utc>,
    /// The last ticket issued for it.
    pub stage: DeliveryStatus,
}

/// The SQLite file. Each call is one short statement or transaction behind a lock.
pub struct Store {
    conn: Mutex<Connection>,
}

fn time(millis: i64) -> Result<DateTime<Utc>, StoreError> {
    DateTime::from_timestamp_millis(millis).ok_or(StoreError::Time(millis))
}

/// Stages are stored as their snake_case wire names.
fn stage_text(stage: DeliveryStatus) -> Result<String, StoreError> {
    Ok(serde_json::from_value(serde_json::to_value(stage)?)?)
}

impl Store {
    /// Opens the file, creating it and its tables if they don't exist.
    pub fn open(path: &Path) -> Result<Store, StoreError> {
        let conn = Connection::open(path)?;
        conn.execute_batch(SCHEMA)?;
        Ok(Store {
            conn: Mutex::new(conn),
        })
    }

    fn conn(&self) -> MutexGuard<'_, Connection> {
        // A panic can't leave the file half-written: every write is one statement or transaction.
        self.conn.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Queues a message behind every message already waiting, and returns its id.
    pub fn enqueue(
        &self,
        topic: &str,
        payload: &[u8],
        queued_at: DateTime<Utc>,
    ) -> Result<i64, StoreError> {
        let conn = self.conn();
        conn.execute(
            "INSERT INTO outbox (topic, payload, queued_at) VALUES (?1, ?2, ?3)",
            (topic, payload, queued_at.timestamp_millis()),
        )?;
        Ok(conn.last_insert_rowid())
    }

    /// Records that `started` reached its stage, and queues that stage's ticket, in one transaction.
    pub fn record(
        &self,
        started: &Started,
        topic: &str,
        payload: &[u8],
        queued_at: DateTime<Utc>,
    ) -> Result<(), StoreError> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO batched (order_id, sequence, ticket_seq, truck, mix, volume_m3, started_at, stage)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT (order_id, sequence) DO UPDATE SET stage = excluded.stage",
            (
                &started.load.order.0,
                started.load.sequence,
                started.ticket_seq,
                &started.truck.0,
                &started.mix.0,
                started.volume_m3,
                started.started_at.timestamp_millis(),
                stage_text(started.stage)?,
            ),
        )?;
        tx.execute(
            "INSERT INTO outbox (topic, payload, queued_at) VALUES (?1, ?2, ?3)",
            (topic, payload, queued_at.timestamp_millis()),
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Every load this plant has started, oldest first.
    pub fn started(&self) -> Result<Vec<Started>, StoreError> {
        let conn = self.conn();
        let mut statement = conn.prepare_cached(
            "SELECT order_id, sequence, ticket_seq, truck, mix, volume_m3, started_at, stage
             FROM batched ORDER BY ticket_seq",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, u32>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, f64>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, String>(7)?,
            ))
        })?;
        let mut started = Vec::new();
        for row in rows {
            let (order, sequence, ticket_seq, truck, mix, volume_m3, at, stage) = row?;
            started.push(Started {
                load: LoadRef {
                    order: OrderId(order),
                    sequence,
                },
                ticket_seq,
                truck: TruckId(truck),
                mix: MixCode(mix),
                volume_m3,
                started_at: time(at)?,
                stage: serde_json::from_value(serde_json::Value::String(stage))?,
            });
        }
        Ok(started)
    }

    /// The highest ticket number issued so far, or 0.
    pub fn last_ticket_seq(&self) -> Result<i64, StoreError> {
        let last: Option<i64> =
            self.conn()
                .query_row("SELECT max(ticket_seq) FROM batched", [], |row| row.get(0))?;
        Ok(last.unwrap_or(0))
    }

    /// Keeps the slice dispatch sent, replacing the one before.
    pub fn save_plan(&self, synced: &Synced) -> Result<(), StoreError> {
        let slice = serde_json::to_string(&synced.plan)?;
        self.conn().execute(
            "INSERT INTO plan (id, slice, synced_at) VALUES (1, ?1, ?2)
             ON CONFLICT (id) DO UPDATE SET slice = excluded.slice, synced_at = excluded.synced_at",
            (slice, synced.at.timestamp_millis()),
        )?;
        Ok(())
    }

    /// The last slice dispatch sent, if it ever sent one.
    pub fn plan(&self) -> Result<Option<Synced>, StoreError> {
        let row: Option<(String, i64)> = self
            .conn()
            .query_row(
                "SELECT slice, synced_at FROM plan WHERE id = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((slice, at)) = row else {
            return Ok(None);
        };
        let plan: PlantPlan = serde_json::from_str(&slice)?;
        Ok(Some(Synced {
            plan,
            at: time(at)?,
        }))
    }

    /// The message that has waited longest, if any.
    pub fn oldest(&self) -> Result<Option<Queued>, StoreError> {
        let conn = self.conn();
        let mut statement = conn.prepare_cached(
            "SELECT id, topic, payload, queued_at FROM outbox ORDER BY id LIMIT 1",
        )?;
        let mut rows = statement.query([])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        Ok(Some(Queued {
            id: row.get(0)?,
            topic: row.get(1)?,
            payload: row.get(2)?,
            queued_at: time(row.get(3)?)?,
        }))
    }

    /// How many messages are waiting.
    pub fn waiting(&self) -> Result<u64, StoreError> {
        let count: i64 = self
            .conn()
            .query_row("SELECT count(*) FROM outbox", [], |row| row.get(0))?;
        Ok(count.unsigned_abs())
    }

    /// Removes a message once the broker has acknowledged it.
    pub fn remove(&self, id: i64) -> Result<(), StoreError> {
        self.conn()
            .execute("DELETE FROM outbox WHERE id = ?1", [id])?;
        Ok(())
    }
}

/// A SQLite file in the temp directory that is deleted when dropped. For tests.
#[cfg(test)]
pub(crate) struct TempFile(pub std::path::PathBuf);

#[cfg(test)]
impl TempFile {
    pub fn new(name: &str) -> TempFile {
        let path =
            std::env::temp_dir().join(format!("plant-agent-{}-{name}.sqlite", std::process::id()));
        let _ = std::fs::remove_file(&path);
        TempFile(path)
    }
}

#[cfg(test)]
impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use domain::PlantId;

    fn at(second: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 11, 9, 0, second).unwrap()
    }

    #[test]
    fn messages_come_out_in_the_order_they_went_in() {
        let file = TempFile::new("order");
        let store = Store::open(&file.0).unwrap();
        assert_eq!(store.oldest().unwrap(), None);

        let first = store.enqueue("plants/p/tickets", b"one", at(1)).unwrap();
        let second = store.enqueue("plants/p/tickets", b"two", at(2)).unwrap();
        let oldest = store.oldest().unwrap().unwrap();
        assert_eq!(
            oldest,
            Queued {
                id: first,
                topic: "plants/p/tickets".into(),
                payload: b"one".to_vec(),
                queued_at: at(1),
            }
        );

        store.remove(first).unwrap();
        assert_eq!(store.oldest().unwrap().unwrap().id, second);
        store.remove(second).unwrap();
        assert_eq!(store.oldest().unwrap(), None);
    }

    #[test]
    fn the_queue_survives_closing_and_reopening_the_file() {
        let file = TempFile::new("reopen");
        {
            let store = Store::open(&file.0).unwrap();
            store.enqueue("t", b"one", at(1)).unwrap();
            store.enqueue("t", b"two", at(2)).unwrap();
        }
        let store = Store::open(&file.0).unwrap();
        assert_eq!(store.oldest().unwrap().unwrap().payload, b"one");
        assert_eq!(store.waiting().unwrap(), 2);
    }

    #[test]
    fn removing_one_message_leaves_the_others() {
        let file = TempFile::new("remove");
        let store = Store::open(&file.0).unwrap();
        let ids: Vec<i64> = (1..=3)
            .map(|i| store.enqueue("t", &[i], at(u32::from(i))).unwrap())
            .collect();

        store.remove(ids[1]).unwrap();
        store.remove(ids[1]).unwrap(); // a repeated acknowledgement changes nothing
        assert_eq!(store.oldest().unwrap().unwrap().id, ids[0]);
        store.remove(ids[0]).unwrap();
        assert_eq!(store.oldest().unwrap().unwrap().id, ids[2]);
    }

    #[test]
    fn ids_are_never_reused_once_the_queue_empties() {
        let file = TempFile::new("ids");
        let store = Store::open(&file.0).unwrap();
        let first = store.enqueue("t", b"one", at(1)).unwrap();
        store.remove(first).unwrap();
        let second = store.enqueue("t", b"two", at(2)).unwrap();
        assert!(second > first, "{second} after {first}");
    }

    fn started(stage: DeliveryStatus) -> Started {
        Started {
            load: LoadRef {
                order: OrderId("10001001/BW-1".into()),
                sequence: 2,
            },
            ticket_seq: 7,
            truck: TruckId("truck-07".into()),
            mix: MixCode("C30/37-XC4".into()),
            volume_m3: 9.0,
            started_at: at(5),
            stage,
        }
    }

    #[test]
    fn a_load_and_its_tickets_are_recorded_together_and_survive_a_restart() {
        let file = TempFile::new("batched");
        {
            let store = Store::open(&file.0).unwrap();
            assert_eq!(store.last_ticket_seq().unwrap(), 0);
            store
                .record(&started(DeliveryStatus::Batching), "t", b"batching", at(5))
                .unwrap();
            store
                .record(&started(DeliveryStatus::Loading), "t", b"loading", at(9))
                .unwrap();
        }
        let store = Store::open(&file.0).unwrap();
        assert_eq!(store.started().unwrap(), [started(DeliveryStatus::Loading)]);
        assert_eq!(store.last_ticket_seq().unwrap(), 7);
        assert_eq!(store.waiting().unwrap(), 2, "one ticket per stage");
        assert_eq!(store.oldest().unwrap().unwrap().payload, b"batching");
    }

    #[test]
    fn the_last_plan_survives_a_restart() {
        let file = TempFile::new("plan");
        let synced = Synced {
            plan: PlantPlan {
                plant: PlantId("plant-walldorf".into()),
                version: Some(12),
                created_at: Some(at(1)),
                loads: Vec::new(),
            },
            at: at(3),
        };
        {
            let store = Store::open(&file.0).unwrap();
            assert_eq!(store.plan().unwrap(), None);
            store
                .save_plan(&Synced {
                    at: at(2),
                    ..synced.clone()
                })
                .unwrap();
            store.save_plan(&synced).unwrap();
        }
        assert_eq!(Store::open(&file.0).unwrap().plan().unwrap(), Some(synced));
    }
}
