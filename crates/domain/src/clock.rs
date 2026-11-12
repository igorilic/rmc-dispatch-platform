//! The simulated world's clock. Every service reads the time from [`now`], so
//! `CLOCK_OFFSET_HOURS` shifts the whole world by the same amount. That lets `make demo` run in
//! the evening as if it were daytime, while the plants' opening hours stay as they are.
//!
//! Only the world moves: orders, plans, batch tickets and telematics. Times that describe the
//! system itself, such as traces, metrics and an integration's last ingest, keep the real clock
//! (`Utc::now`).

use std::sync::OnceLock;

use chrono::{DateTime, TimeDelta, Utc};

static OFFSET: OnceLock<TimeDelta> = OnceLock::new();

#[derive(Debug, thiserror::Error)]
#[error("CLOCK_OFFSET_HOURS must be a whole number of hours, such as -10, not {0:?}")]
pub struct OffsetError(String);

/// Parses a `CLOCK_OFFSET_HOURS` value. Unset or empty means no shift.
pub fn parse_offset(value: Option<&str>) -> Result<TimeDelta, OffsetError> {
    let value = value.unwrap_or_default().trim();
    if value.is_empty() {
        return Ok(TimeDelta::zero());
    }
    value
        .parse::<i64>()
        .ok()
        .and_then(TimeDelta::try_hours)
        .ok_or_else(|| OffsetError(value.to_owned()))
}

/// Reads `CLOCK_OFFSET_HOURS` once, at startup, and returns the offset [`now`] applies from then on.
/// Call it before anything reads the time; a later call doesn't change the offset.
pub fn init_from_env() -> Result<TimeDelta, OffsetError> {
    let offset = parse_offset(std::env::var("CLOCK_OFFSET_HOURS").ok().as_deref())?;
    Ok(*OFFSET.get_or_init(|| offset))
}

/// The world's time: the wall clock shifted by the offset from [`init_from_env`], or the wall clock
/// itself if that was never called.
pub fn now() -> DateTime<Utc> {
    shifted(
        Utc::now(),
        OFFSET.get().copied().unwrap_or_else(TimeDelta::zero),
    )
}

fn shifted(wall: DateTime<Utc>, offset: TimeDelta) -> DateTime<Utc> {
    wall + offset
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_offset_unless_one_is_set() {
        assert_eq!(parse_offset(None).unwrap(), TimeDelta::zero());
        assert_eq!(parse_offset(Some("")).unwrap(), TimeDelta::zero());
        assert_eq!(parse_offset(Some("0")).unwrap(), TimeDelta::zero());
    }

    #[test]
    fn an_offset_is_whole_hours_either_way() {
        assert_eq!(parse_offset(Some("-10")).unwrap(), TimeDelta::hours(-10));
        assert_eq!(parse_offset(Some(" 3 ")).unwrap(), TimeDelta::hours(3));
    }

    #[test]
    fn anything_else_is_an_error_that_names_the_variable() {
        for bad in ["1.5", "ten", "-10h", "99999999999999"] {
            let err = parse_offset(Some(bad)).unwrap_err().to_string();
            assert!(
                err.contains("CLOCK_OFFSET_HOURS") && err.contains(bad),
                "{bad}: {err}"
            );
        }
    }

    #[test]
    fn the_world_is_the_wall_clock_shifted_by_the_offset() {
        let wall = Utc::now();
        assert_eq!(
            shifted(wall, TimeDelta::hours(-10)),
            wall - TimeDelta::hours(10)
        );
        assert_eq!(shifted(wall, TimeDelta::zero()), wall);
    }
}
