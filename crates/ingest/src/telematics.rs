//! The telematics vendor's MQTT payloads → canonical events.
//!
//! There is one vendor with one fixed format, so this is plain Rust. YAML mappings are for
//! customer integrations, whose formats vary.
//!
//! Message ids: trucks use `{truck}:{seq}:{ts_ms}` and tickets `{ticket}:{event}:{ts_ms}`. A
//! duplicate is the same message sent twice, so it has the same id. Adding the timestamp keeps a
//! restarted sender, whose `seq` begins at 1 again, from colliding with its earlier messages.

use chrono::{DateTime, Utc};
use domain::{
    Channel, DeliveryStatus, DrumRotation, Event, EventEnvelope, EventSource, Location, MixCode,
    PlantId, Seed, TruckId, TruckStatus,
};
use serde::Deserialize;
use serde::de::DeserializeOwned;

#[derive(Deserialize)]
struct Header {
    vehicle: String,
    seq: u64,
    ts_ms: i64,
}

#[derive(Deserialize)]
struct Gps {
    lat: f64,
    lon: f64,
    speed_kmh: f64,
}

#[derive(Deserialize)]
struct Drum {
    rotation: DrumRotation,
    rpm: f64,
}

#[derive(Deserialize)]
struct Status {
    status: TruckStatus,
}

#[derive(Deserialize)]
struct Ticket {
    plant: String,
    ticket_no: String,
    vehicle: String,
    ts_ms: i64,
    event: DeliveryStatus,
    mix: String,
    volume_m3: f64,
}

fn parse<T: DeserializeOwned>(payload: &[u8]) -> Result<T, Vec<String>> {
    serde_json::from_slice(payload).map_err(|e| vec![format!("invalid payload: {e}")])
}

fn time(ts_ms: i64) -> Result<DateTime<Utc>, Vec<String>> {
    DateTime::from_timestamp_millis(ts_ms)
        .ok_or_else(|| vec![format!("ts_ms {ts_ms} is out of range")])
}

/// Maps one MQTT message, or returns every reason it can't be mapped.
pub fn map(
    topic: &str,
    payload: &[u8],
    received_at: DateTime<Utc>,
    seed: &Seed,
) -> Result<EventEnvelope, Vec<String>> {
    let parts: Vec<&str> = topic.split('/').collect();
    let source = |integration: &str| EventSource {
        integration: integration.into(),
        channel: Channel::Mqtt,
        schema_version: None,
        reference: topic.into(),
    };
    match parts.as_slice() {
        ["trucks", truck, kind @ ("gps" | "drum" | "status")] => {
            let header: Header = parse(payload)?;
            let mut errors = Vec::new();
            if header.vehicle != *truck {
                errors.push(format!(
                    "vehicle {} does not match topic {topic}",
                    header.vehicle
                ));
            } else if seed.truck(truck).is_none() {
                errors.push(format!("unknown truck {truck}"));
            }
            if !errors.is_empty() {
                return Err(errors);
            }
            let truck = TruckId((*truck).to_owned());
            let event = match *kind {
                "gps" => {
                    let gps: Gps = parse(payload)?;
                    Event::TruckPositionReported {
                        truck,
                        location: Location {
                            lat: gps.lat,
                            lon: gps.lon,
                        },
                        speed_kmh: gps.speed_kmh,
                    }
                }
                "drum" => {
                    let drum: Drum = parse(payload)?;
                    Event::DrumRotationReported {
                        truck,
                        rotation: drum.rotation,
                        rpm: drum.rpm,
                    }
                }
                _ => Event::TruckStatusReported {
                    truck,
                    status: parse::<Status>(payload)?.status,
                },
            };
            Ok(EventEnvelope {
                id: format!("{}:{}:{}", header.vehicle, header.seq, header.ts_ms),
                occurred_at: time(header.ts_ms)?,
                received_at,
                source: source("telematics"),
                event,
            })
        }
        ["plants", plant, "tickets"] => {
            let ticket: Ticket = parse(payload)?;
            let mut errors = Vec::new();
            if ticket.plant != *plant {
                errors.push(format!(
                    "plant {} does not match topic {topic}",
                    ticket.plant
                ));
            } else if seed.plant(plant).is_none() {
                errors.push(format!("unknown plant {plant}"));
            }
            if seed.truck(&ticket.vehicle).is_none() {
                errors.push(format!("unknown truck {}", ticket.vehicle));
            }
            if !matches!(
                ticket.event,
                DeliveryStatus::Batching | DeliveryStatus::Loading | DeliveryStatus::Loaded
            ) {
                let event = serde_json::to_value(ticket.event).unwrap_or_default();
                let event = event.as_str().unwrap_or_default();
                errors.push(format!(
                    "a ticket can only be batching, loading or loaded, not {event}"
                ));
            }
            if seed.mix(&ticket.mix).is_none() {
                errors.push(format!("unknown mix {}", ticket.mix));
            }
            if !errors.is_empty() {
                return Err(errors);
            }
            let event_name = serde_json::to_value(ticket.event).unwrap_or_default();
            Ok(EventEnvelope {
                id: format!(
                    "{}:{}:{}",
                    ticket.ticket_no,
                    event_name.as_str().unwrap_or_default(),
                    ticket.ts_ms
                ),
                occurred_at: time(ticket.ts_ms)?,
                received_at,
                source: source("plants"),
                event: Event::BatchTicketIssued {
                    plant: PlantId(ticket.plant),
                    ticket_no: ticket.ticket_no,
                    truck: TruckId(ticket.vehicle),
                    event: ticket.event,
                    mix: MixCode(ticket.mix),
                    volume_m3: ticket.volume_m3,
                },
            })
        }
        _ => Err(vec![format!("unknown topic {topic}")]),
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use serde_json::{Value, json};

    use super::*;

    fn received() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 9, 9, 20, 0).unwrap()
    }

    fn event(topic: &str, payload: Value) -> Result<EventEnvelope, Vec<String>> {
        map(
            topic,
            payload.to_string().as_bytes(),
            received(),
            &Seed::rhine_neckar().unwrap(),
        )
    }

    #[test]
    fn truck_messages_become_truck_events() {
        let gps = event(
            "trucks/truck-07/gps",
            json!({"vehicle": "truck-07", "seq": 620, "ts_ms": 1794236511059_i64, "lat": 49.43, "lon": 8.53, "speed_kmh": 37.5}),
        )
        .unwrap();
        assert_eq!(gps.id, "truck-07:620:1794236511059");
        assert_eq!(gps.occurred_at.timestamp_millis(), 1794236511059);
        assert_eq!(gps.received_at, received());
        assert_eq!(gps.subject(), "events.trucks.truck-07");
        assert_eq!(
            gps.source,
            EventSource {
                integration: "telematics".into(),
                channel: Channel::Mqtt,
                schema_version: None,
                reference: "trucks/truck-07/gps".into()
            }
        );
        assert_eq!(
            gps.event,
            Event::TruckPositionReported {
                truck: TruckId("truck-07".into()),
                location: Location {
                    lat: 49.43,
                    lon: 8.53
                },
                speed_kmh: 37.5
            }
        );

        let drum = event(
            "trucks/truck-07/drum",
            json!({"vehicle": "truck-07", "seq": 621, "ts_ms": 1, "rotation": "discharging", "rpm": 8.0}),
        );
        assert!(matches!(
            drum.unwrap().event,
            Event::DrumRotationReported {
                rotation: DrumRotation::Discharging,
                ..
            }
        ));
        let status = event(
            "trucks/truck-07/status",
            json!({"vehicle": "truck-07", "seq": 622, "ts_ms": 1, "status": "on_site"}),
        );
        assert!(matches!(
            status.unwrap().event,
            Event::TruckStatusReported {
                status: TruckStatus::OnSite,
                ..
            }
        ));
    }

    #[test]
    fn tickets_become_batch_ticket_events() {
        let ticket = event(
            "plants/plant-walldorf/tickets",
            json!({"plant": "plant-walldorf", "ticket_no": "WAL-000002", "vehicle": "truck-25", "ts_ms": 1794240261843_i64,
                   "event": "loaded", "mix": "C30/37-XC4", "volume_m3": 8.0}),
        )
        .unwrap();
        assert_eq!(ticket.id, "WAL-000002:loaded:1794240261843");
        assert_eq!(ticket.subject(), "events.plants.plant-walldorf");
        assert_eq!(ticket.source.integration, "plants");
        assert!(matches!(
            ticket.event,
            Event::BatchTicketIssued {
                event: DeliveryStatus::Loaded,
                ..
            }
        ));
    }

    #[test]
    fn messages_that_contradict_master_data_or_their_topic_are_rejected() {
        let wrong_vehicle = event(
            "trucks/truck-07/gps",
            json!({"vehicle": "truck-08", "seq": 1, "ts_ms": 1, "lat": 0.0, "lon": 0.0, "speed_kmh": 0.0}),
        );
        assert_eq!(
            wrong_vehicle.unwrap_err(),
            ["vehicle truck-08 does not match topic trucks/truck-07/gps"]
        );

        let unknown_truck = event(
            "trucks/truck-99/status",
            json!({"vehicle": "truck-99", "seq": 1, "ts_ms": 1, "status": "available"}),
        );
        assert_eq!(unknown_truck.unwrap_err(), ["unknown truck truck-99"]);

        let not_a_ticket_event = event(
            "plants/plant-walldorf/tickets",
            json!({"plant": "plant-walldorf", "ticket_no": "WAL-1", "vehicle": "truck-25", "ts_ms": 1,
                   "event": "unloaded", "mix": "C99/99", "volume_m3": 8.0}),
        );
        assert_eq!(
            not_a_ticket_event.unwrap_err(),
            [
                "a ticket can only be batching, loading or loaded, not unloaded",
                "unknown mix C99/99"
            ]
        );

        let unknown_topic = event("trucks/truck-07/horn", json!({}));
        assert_eq!(
            unknown_topic.unwrap_err(),
            ["unknown topic trucks/truck-07/horn"]
        );

        let garbage = map(
            "trucks/truck-07/gps",
            b"{not json",
            received(),
            &Seed::rhine_neckar().unwrap(),
        );
        assert!(garbage.unwrap_err()[0].starts_with("invalid payload"));
    }
}
