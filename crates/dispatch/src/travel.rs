//! Travel time, behind [`TravelTimeProvider`] so a routing engine such as OSRM can replace the
//! first model without touching the planner (SPEC §3).
//!
//! [`HaversineTravelTime`] computes great-circle distance × road factor ÷ average speed, with the
//! speed depending on the local time of departure: slower in the morning and afternoon rush hours.

use chrono::{DateTime, TimeDelta, Timelike, Utc};
use chrono_tz::Europe::Berlin;
use domain::Location;

pub trait TravelTimeProvider: Send + Sync {
    /// Road distance in km.
    fn distance_km(&self, from: Location, to: Location) -> f64;

    /// Driving time when leaving `from` at `depart`.
    fn travel(&self, from: Location, to: Location, depart: DateTime<Utc>) -> TimeDelta;
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HaversineTravelTime {
    pub road_factor: f64,
    pub speed_kmh: f64,
    /// Used for departures from 07:00 to 09:00 and from 16:00 to 18:00, Berlin time.
    pub rush_hour_speed_kmh: f64,
}

impl Default for HaversineTravelTime {
    fn default() -> Self {
        HaversineTravelTime {
            road_factor: 1.3,
            speed_kmh: 45.0,
            rush_hour_speed_kmh: 30.0,
        }
    }
}

impl TravelTimeProvider for HaversineTravelTime {
    fn distance_km(&self, from: Location, to: Location) -> f64 {
        from.distance_km(to) * self.road_factor
    }

    fn travel(&self, from: Location, to: Location, depart: DateTime<Utc>) -> TimeDelta {
        let hour = depart.with_timezone(&Berlin).hour();
        let rush = (7..9).contains(&hour) || (16..18).contains(&hour);
        let speed = if rush {
            self.rush_hour_speed_kmh
        } else {
            self.speed_kmh
        };
        TimeDelta::seconds((self.distance_km(from, to) / speed * 3600.0).round() as i64)
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    const MANNHEIM: Location = Location {
        lat: 49.4875,
        lon: 8.466,
    };
    const HEIDELBERG: Location = Location {
        lat: 49.3988,
        lon: 8.6724,
    };

    fn berlin(h: u32, m: u32) -> DateTime<Utc> {
        Berlin
            .with_ymd_and_hms(2026, 11, 10, h, m, 0)
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn roads_are_longer_than_the_crow_flies() {
        let model = HaversineTravelTime::default();
        // Reference value computed independently: 17.8874 km × road factor 1.3.
        assert!((model.distance_km(MANNHEIM, HEIDELBERG) - 23.2537).abs() < 0.001);
        assert_eq!(model.distance_km(MANNHEIM, MANNHEIM), 0.0);
    }

    #[test]
    fn rush_hours_are_slower() {
        let model = HaversineTravelTime::default();
        let close = |actual: TimeDelta, secs: i64| (actual.num_seconds() - secs).abs() <= 1;
        assert!(
            close(model.travel(MANNHEIM, HEIDELBERG, berlin(11, 0)), 1860),
            "45 km/h midday"
        );
        assert!(
            close(model.travel(MANNHEIM, HEIDELBERG, berlin(7, 30)), 2790),
            "30 km/h 07–09"
        );
        assert!(
            close(model.travel(MANNHEIM, HEIDELBERG, berlin(16, 59)), 2790),
            "30 km/h 16–18"
        );
        assert!(
            close(model.travel(MANNHEIM, HEIDELBERG, berlin(18, 0)), 1860),
            "rush hour is over at 18:00"
        );
        assert!(
            close(model.travel(MANNHEIM, HEIDELBERG, berlin(6, 59)), 1860),
            "and starts at 07:00"
        );
        assert_eq!(
            model.travel(MANNHEIM, MANNHEIM, berlin(8, 0)),
            TimeDelta::zero()
        );
    }
}
