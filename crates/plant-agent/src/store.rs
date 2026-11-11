//! The agent's SQLite file. It holds the outbox: tickets waiting to be sent, oldest first.
//!
//! A ticket is serialised once, when it is queued, and the forwarder sends exactly those bytes. A
//! resend after a lost acknowledgement is therefore the same message, and ingest derives the same
//! event id from it. SQLite's default rollback journal runs with `synchronous = FULL`, so a commit
//! is on disk before it returns.

use std::path::Path;
use std::sync::{Mutex, MutexGuard, PoisonError};

use chrono::{DateTime, Utc};
use rusqlite::Connection;

// ponytail: no migrations; the schema is created if missing. Add `PRAGMA user_version` steps once a
// released agent's file has to change shape.
const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS outbox (
    id        INTEGER PRIMARY KEY AUTOINCREMENT, -- never reused, so ids follow queue order
    topic     TEXT NOT NULL,
    payload   BLOB NOT NULL,
    queued_at INTEGER NOT NULL                   -- Unix milliseconds
);
";

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("SQLite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("queued_at {0} is out of range")]
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

/// The SQLite file. Each call is one short statement or transaction behind a lock.
pub struct Store {
    conn: Mutex<Connection>,
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
        let millis: i64 = row.get(3)?;
        Ok(Some(Queued {
            id: row.get(0)?,
            topic: row.get(1)?,
            payload: row.get(2)?,
            queued_at: DateTime::from_timestamp_millis(millis).ok_or(StoreError::Time(millis))?,
        }))
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
}
