//! Canonical events (SPEC §5).
//!
//! Ingest turns every channel's input into these events and publishes them on NATS JetStream.
//! The envelope `id` is the idempotency key and doubles as JetStream's `Nats-Msg-Id`. `occurred_at`
//! is the source's own time, so consumers can tolerate events that arrive out of order.
//! Telematics stay facts about trucks: tying them to a load needs the dispatch plan (M5).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    CustomerId, DeliveryStatus, LoadRef, Location, MixCode, Order, OrderId, PlantId, TruckId,
    TruckStatus,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DrumRotation {
    Charging,
    Mixing,
    Discharging,
    Idle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Channel {
    Webhook,
    Sftp,
    Mqtt,
}

/// Where an event came from, precisely enough to find the original input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventSource {
    /// The customer integration (e.g. `rheinbau-gmbh`), `telematics` or `plants`.
    pub integration: String,
    pub channel: Channel,
    /// The customer's schema version, when it has several live at once.
    pub schema_version: Option<String>,
    /// E.g. `orders_20261109_102000.csv line 2` or an MQTT topic.
    pub reference: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope {
    /// Idempotency key: the same input always produces the same id.
    pub id: String,
    pub occurred_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub source: EventSource,
    #[serde(flatten)]
    pub event: Event,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    /// The full current state of an order, as the customer sent it. Created or changed.
    OrderUpserted {
        order: Order,
        /// The source's revision or timestamp, if it has one, so consumers can drop stale updates.
        source_revision: Option<String>,
    },
    OrderCancelled {
        order: OrderId,
        customer: CustomerId,
        source_revision: Option<String>,
    },
    TruckPositionReported {
        truck: TruckId,
        location: Location,
        speed_kmh: f64,
    },
    TruckStatusReported {
        truck: TruckId,
        status: TruckStatus,
    },
    DrumRotationReported {
        truck: TruckId,
        rotation: DrumRotation,
        rpm: f64,
    },
    /// From the plant's batch computer: `batching`, `loading` or `loaded`.
    BatchTicketIssued {
        plant: PlantId,
        ticket_no: String,
        truck: TruckId,
        event: DeliveryStatus,
        mix: MixCode,
        volume_m3: f64,
        /// The load the plant batches for, when its batch computer knows it (from the plan).
        #[serde(default)]
        load: Option<LoadRef>,
    },
}

impl EventEnvelope {
    /// One subject per customer, truck or plant, so each one's events stay in order.
    pub fn subject(&self) -> String {
        match &self.event {
            Event::OrderUpserted { order, .. } => format!("events.orders.{}", order.customer.0),
            Event::OrderCancelled { customer, .. } => format!("events.orders.{}", customer.0),
            Event::TruckPositionReported { truck, .. }
            | Event::TruckStatusReported { truck, .. }
            | Event::DrumRotationReported { truck, .. } => format!("events.trucks.{}", truck.0),
            Event::BatchTicketIssued { plant, .. } => format!("events.plants.{}", plant.0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Serialize, de::DeserializeOwned};
    use serde_json::{Value, json};

    fn assert_wire_format<T: Serialize + DeserializeOwned>(wire: Value) {
        let parsed: T = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(parsed).unwrap(), wire);
    }

    fn envelope(event: Value) -> Value {
        let mut wire = json!({
            "id": "rheinbau-gmbh:RB-7781:9f2c41aa00112233",
            "occurred_at": "2026-11-09T09:20:00Z",
            "received_at": "2026-11-09T09:20:01Z",
            "source": {
                "integration": "rheinbau-gmbh",
                "channel": "sftp",
                "schema_version": null,
                "reference": "orders_20261109_102000.csv line 2"
            }
        });
        wire.as_object_mut()
            .unwrap()
            .extend(event.as_object().unwrap().clone());
        wire
    }

    fn order_upserted() -> Value {
        envelope(json!({
            "type": "order_upserted",
            "order": {
                "id": "10001002/RB-7781",
                "customer": "10001002",
                "site": "50001023",
                "mix": "C25/30-XC2",
                "total_volume_m3": 42.5,
                "requested_start": "2026-11-10T06:00:00Z",
                "pour_rate_m3_per_h": 30.0,
                "max_gap_minutes": 20,
                "status": "pending"
            },
            "source_revision": null
        }))
    }

    #[test]
    fn order_event_wire_format() {
        assert_wire_format::<EventEnvelope>(order_upserted());
        assert_wire_format::<EventEnvelope>(envelope(json!({
            "type": "order_cancelled",
            "order": "10001002/RB-7779",
            "customer": "10001002",
            "source_revision": "2"
        })));
    }

    #[test]
    fn telematics_and_ticket_wire_format() {
        assert_wire_format::<EventEnvelope>(envelope(json!({
            "type": "truck_position_reported",
            "truck": "truck-07",
            "location": { "lat": 49.43, "lon": 8.53 },
            "speed_kmh": 37.5
        })));
        assert_wire_format::<EventEnvelope>(envelope(json!({
            "type": "truck_status_reported",
            "truck": "truck-07",
            "status": "en_route"
        })));
        assert_wire_format::<EventEnvelope>(envelope(json!({
            "type": "drum_rotation_reported",
            "truck": "truck-07",
            "rotation": "discharging",
            "rpm": 8.0
        })));
        assert_wire_format::<EventEnvelope>(envelope(json!({
            "type": "batch_ticket_issued",
            "plant": "plant-walldorf",
            "ticket_no": "WAL-000002",
            "truck": "truck-25",
            "event": "loaded",
            "mix": "C30/37-XC4",
            "volume_m3": 8.0,
            "load": null
        })));
        assert_wire_format::<EventEnvelope>(envelope(json!({
            "type": "batch_ticket_issued",
            "plant": "plant-walldorf",
            "ticket_no": "WAL-000003",
            "truck": "truck-25",
            "event": "batching",
            "mix": "C30/37-XC4",
            "volume_m3": 9.0,
            "load": { "order": "10001001/BW-4711", "sequence": 2 }
        })));
    }

    #[test]
    fn tickets_from_before_load_references_still_read() {
        let old = envelope(json!({
            "type": "batch_ticket_issued", "plant": "plant-walldorf", "ticket_no": "WAL-000002",
            "truck": "truck-25", "event": "loaded", "mix": "C30/37-XC4", "volume_m3": 8.0
        }));
        let parsed: EventEnvelope = serde_json::from_value(old).unwrap();
        assert!(matches!(
            parsed.event,
            Event::BatchTicketIssued { load: None, .. }
        ));
    }

    #[test]
    fn subjects_partition_by_customer_truck_and_plant() {
        let subject = |wire: Value| {
            serde_json::from_value::<EventEnvelope>(wire)
                .unwrap()
                .subject()
        };
        assert_eq!(subject(order_upserted()), "events.orders.10001002");
        let cancelled = envelope(json!({
            "type": "order_cancelled", "order": "10001002/RB-7779", "customer": "10001002", "source_revision": null
        }));
        assert_eq!(subject(cancelled), "events.orders.10001002");
        let position = envelope(json!({
            "type": "truck_position_reported", "truck": "truck-07",
            "location": { "lat": 49.43, "lon": 8.53 }, "speed_kmh": 0.0
        }));
        assert_eq!(subject(position), "events.trucks.truck-07");
        let ticket = envelope(json!({
            "type": "batch_ticket_issued", "plant": "plant-walldorf", "ticket_no": "WAL-000002",
            "truck": "truck-25", "event": "batching", "mix": "C30/37-XC4", "volume_m3": 8.0
        }));
        assert_eq!(subject(ticket), "events.plants.plant-walldorf");
    }
}
