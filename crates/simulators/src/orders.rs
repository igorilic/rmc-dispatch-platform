//! Order generator shared by the three fake customers.
//!
//! Each tick does one of three things:
//! - creates a new order (80 %),
//! - changes the volume of an open order (15 %),
//! - cancels an open order (5 %).
//!
//! A new order carries our customer number, one of that customer's ship-tos from our master data,
//! and a mix. Its volume is 8–150 m³, mostly 10–60, in
//! 0.5 m³ steps. Its start is 2–26 hours ahead, between 06:00 and 15:45 Berlin time. The pour rate
//! is 20–60 m³/h and the maximum gap 15–30 minutes. The customers only differ in how they deliver
//! orders (see `customers`).

use chrono::{DateTime, NaiveTime, TimeDelta, TimeZone, Utc};
use chrono_tz::Europe::Berlin;
use rand::seq::IndexedRandom;
use rand::{RngExt, rngs::StdRng};

#[derive(Debug, Clone, PartialEq)]
pub struct GeneratedOrder {
    pub order_ref: String,
    /// Our customer number, from master data.
    pub customer: String,
    /// Our ship-to number, from master data.
    pub ship_to: String,
    pub mix: String,
    pub volume_m3: f64,
    pub start: DateTime<Utc>,
    pub pour_rate_m3_per_h: u32,
    pub max_gap_minutes: u32,
    /// Starts at 1 and goes up with every change.
    pub revision: u32,
    pub updated_at: DateTime<Utc>,
    pub cancelled: bool,
}

/// How a customer numbers its orders: `prefix` + a counter from `first`, zero-padded to `width`.
#[derive(Debug, Clone, Copy)]
pub struct OrderNumbers {
    pub prefix: &'static str,
    pub first: u32,
    pub width: usize,
}

pub struct OrderBook {
    numbers: OrderNumbers,
    next: u32,
    customer: String,
    ship_tos: Vec<String>,
    mixes: Vec<String>,
    open: Vec<GeneratedOrder>,
}

impl OrderBook {
    pub fn new(
        numbers: OrderNumbers,
        customer: String,
        ship_tos: Vec<String>,
        mixes: Vec<String>,
    ) -> Self {
        OrderBook {
            next: numbers.first,
            numbers,
            customer,
            ship_tos,
            mixes,
            open: Vec::new(),
        }
    }

    pub fn open(&self) -> &[GeneratedOrder] {
        &self.open
    }

    pub fn ship_tos(&self) -> &[String] {
        &self.ship_tos
    }

    /// Drops orders whose start has passed. The customer's system no longer lists them, which keeps
    /// rheinbau's snapshots bounded however long the simulators run.
    pub fn prune(&mut self, now: DateTime<Utc>) {
        self.open.retain(|o| o.start >= now);
    }

    pub fn next_change(&mut self, rng: &mut StdRng, now: DateTime<Utc>) -> GeneratedOrder {
        self.prune(now);
        let change = match rng.random::<f64>() {
            r if r < 0.80 => None,
            r if r < 0.95 => self.update(rng, now),
            _ => self.cancel(rng, now),
        };
        change.unwrap_or_else(|| self.create(rng, now))
    }

    pub fn create(&mut self, rng: &mut StdRng, now: DateTime<Utc>) -> GeneratedOrder {
        let OrderNumbers { prefix, width, .. } = self.numbers;
        let order = GeneratedOrder {
            order_ref: format!("{prefix}{:0width$}", self.next),
            customer: self.customer.clone(),
            ship_to: self.ship_tos.choose(rng).cloned().unwrap_or_default(),
            mix: self.mixes.choose(rng).cloned().unwrap_or_default(),
            volume_m3: half_m3(8.0 + 142.0 * rng.random::<f64>().powi(2)),
            start: start_time(rng, now),
            pour_rate_m3_per_h: 5 * rng.random_range(4..=12),
            max_gap_minutes: 5 * rng.random_range(3..=6),
            revision: 1,
            updated_at: now,
            cancelled: false,
        };
        self.next += 1;
        self.open.push(order.clone());
        order
    }

    /// Changes the volume of a random open order by -30 % to +30 %.
    pub fn update(&mut self, rng: &mut StdRng, now: DateTime<Utc>) -> Option<GeneratedOrder> {
        if self.open.is_empty() {
            return None;
        }
        let i = rng.random_range(0..self.open.len());
        let order = &mut self.open[i];
        order.volume_m3 = half_m3(order.volume_m3 * rng.random_range(0.7..1.3));
        order.revision += 1;
        order.updated_at = now;
        Some(order.clone())
    }

    pub fn cancel(&mut self, rng: &mut StdRng, now: DateTime<Utc>) -> Option<GeneratedOrder> {
        if self.open.is_empty() {
            return None;
        }
        let mut order = self.open.swap_remove(rng.random_range(0..self.open.len()));
        order.cancelled = true;
        order.revision += 1;
        order.updated_at = now;
        Some(order)
    }
}

fn half_m3(volume: f64) -> f64 {
    ((volume * 2.0).round() / 2.0).clamp(8.0, 150.0)
}

/// Today or tomorrow, on a quarter hour between 06:00 and 15:45 Berlin time, and at least 2 h ahead.
fn start_time(rng: &mut StdRng, now: DateTime<Utc>) -> DateTime<Utc> {
    let time = NaiveTime::from_hms_opt(rng.random_range(6..=15), 15 * rng.random_range(0..4), 0)
        .unwrap_or(NaiveTime::MIN);
    let mut day =
        now.with_timezone(&Berlin).date_naive() + TimeDelta::days(rng.random_range(0..=1));
    loop {
        // `earliest()` is only `None` inside a DST gap, which never falls between 06:00 and 16:00.
        if let Some(start) = Berlin.from_local_datetime(&day.and_time(time)).earliest() {
            let start = start.with_timezone(&Utc);
            if start >= now + TimeDelta::hours(2) {
                return start;
            }
        }
        day += TimeDelta::days(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Timelike;
    use rand::SeedableRng;

    fn book() -> OrderBook {
        let numbers = OrderNumbers {
            prefix: "RB-",
            first: 7001,
            width: 4,
        };
        OrderBook::new(
            numbers,
            "10001002".into(),
            vec!["50001015".into(), "50001020".into()],
            vec!["C25/30-XC2".into()],
        )
    }

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 9, 9, 15, 0).unwrap()
    }

    #[test]
    fn new_orders_are_plausible() {
        let mut rng = StdRng::seed_from_u64(3);
        let mut book = book();
        // An empty book can only create.
        assert_eq!(book.next_change(&mut rng, now()).order_ref, "RB-7001");
        for n in 7002..7500 {
            let o = book.create(&mut rng, now());
            assert_eq!(o.order_ref, format!("RB-{n}"));
            assert_eq!(o.customer, "10001002", "orders carry our customer number");
            assert!(
                ["50001015", "50001020"].contains(&o.ship_to.as_str()),
                "{o:?}"
            );
            assert!(
                (8.0..=150.0).contains(&o.volume_m3) && (o.volume_m3 * 2.0).fract() == 0.0,
                "{o:?}"
            );
            assert!(
                (20..=60).contains(&o.pour_rate_m3_per_h) && o.pour_rate_m3_per_h.is_multiple_of(5),
                "{o:?}"
            );
            assert!(
                (15..=30).contains(&o.max_gap_minutes) && o.max_gap_minutes.is_multiple_of(5),
                "{o:?}"
            );
            assert!(
                o.start >= now() + TimeDelta::hours(2) && o.start <= now() + TimeDelta::hours(50),
                "{o:?}"
            );
            let local = o.start.with_timezone(&Berlin);
            assert!(
                (6..=15).contains(&local.hour()) && local.minute().is_multiple_of(15),
                "{local}"
            );
            assert_eq!((o.revision, o.updated_at, o.cancelled), (1, now(), false));
        }
        assert_eq!(book.open().len(), 499);
    }

    #[test]
    fn an_update_replaces_the_whole_order_under_the_same_reference() {
        let mut rng = StdRng::seed_from_u64(4);
        let mut book = book();
        let created = book.create(&mut rng, now());
        let later = now() + TimeDelta::minutes(10);
        let updated = book.update(&mut rng, later).unwrap();
        assert_eq!(updated.order_ref, created.order_ref);
        assert_eq!((updated.revision, updated.updated_at), (2, later));
        assert!((8.0..=150.0).contains(&updated.volume_m3));
        assert_eq!(book.open(), [updated]);
    }

    #[test]
    fn orders_leave_the_book_once_their_start_has_passed() {
        let mut rng = StdRng::seed_from_u64(6);
        let mut book = book();
        for _ in 0..20 {
            book.create(&mut rng, now());
        }
        // Every start is at most ~26 h ahead, so two days later the customer no longer lists them.
        let later = now() + TimeDelta::days(2);
        book.prune(later);
        assert!(book.open().is_empty(), "{:?}", book.open());

        // next_change prunes too, so snapshots stay bounded however long the simulators run.
        for _ in 0..20 {
            book.create(&mut rng, now());
        }
        book.next_change(&mut rng, later);
        assert!(
            book.open().iter().all(|o| o.start >= later),
            "{:?}",
            book.open()
        );
    }

    #[test]
    fn a_cancellation_closes_the_order() {
        let mut rng = StdRng::seed_from_u64(5);
        let mut book = book();
        let created = book.create(&mut rng, now());
        let cancelled = book.cancel(&mut rng, now()).unwrap();
        assert_eq!(cancelled.order_ref, created.order_ref);
        assert!(cancelled.cancelled);
        assert_eq!(cancelled.revision, 2);
        assert!(book.open().is_empty());
        assert!(book.cancel(&mut rng, now()).is_none());
    }
}
