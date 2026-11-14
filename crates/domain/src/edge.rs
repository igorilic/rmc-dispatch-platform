//! What a plant and the cloud send each other (SPEC §6). The plant-agent builds in its own Cargo
//! workspace, so these types are the contract that keeps both sides agreeing on the JSON.
//!
//! - [`PlantPlan`]: the plant's slice of the latest plan, from `GET /plants/{id}/plan`.
//! - [`PlantTicket`]: a batch ticket as a plant's batch computer sends it on
//!   `plants/{id}/tickets` over MQTT. Ingest reads it with its own parser, so a mistake on the
//!   sending side can't slip through unnoticed.
//! - [`sync_topic`]: where the cloud asks a plant's agent to fetch its plan now.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{DeliveryStatus, Load, MixCode, OrderId, PlantId, TruckId};

/// A plant's loads in the latest plan version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PlantPlan {
    pub plant: PlantId,
    /// The latest version; `None` before dispatch has made its first plan.
    pub version: Option<i64>,
    pub created_at: Option<DateTime<Utc>>,
    /// By planned batch time.
    pub loads: Vec<PlantLoad>,
}

/// A load and the mix it carries, which the batch ticket names.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PlantLoad {
    #[serde(flatten)]
    pub load: Load,
    pub mix: MixCode,
}

/// A batch ticket: `batching`, `loading` or `loaded`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlantTicket {
    pub plant: PlantId,
    pub ticket_no: String,
    pub vehicle: TruckId,
    /// When it happened, in Unix milliseconds. Part of the event id ingest derives.
    pub ts_ms: i64,
    pub event: DeliveryStatus,
    pub mix: MixCode,
    pub volume_m3: f64,
    /// The order and load number the plan gave the batch computer. A ticket for a trip outside
    /// the plan has neither.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order: Option<OrderId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load_no: Option<u32>,
}

impl PlantTicket {
    /// The MQTT topic a plant's batch computer sends its tickets on.
    pub fn topic(&self) -> String {
        format!("plants/{}/tickets", self.plant.0)
    }
}

/// The MQTT topic on which the cloud asks a plant's agent to fetch its plan now. The payload is
/// empty, or a W3C `traceparent` that the agent's fetch continues.
pub fn sync_topic(plant: &PlantId) -> String {
    format!("plants/{}/commands/sync", plant.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::DeserializeOwned;
    use serde_json::{Value, json};

    fn assert_wire_format<T: Serialize + DeserializeOwned>(wire: Value) {
        let parsed: T = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(parsed).unwrap(), wire);
    }

    #[test]
    fn plant_plan_wire_format() {
        assert_wire_format::<PlantPlan>(json!({
            "plant": "plant-walldorf",
            "version": 12,
            "created_at": "2026-11-11T05:00:00Z",
            "loads": [{
                "order": "10001001/BW-4711",
                "sequence": 1,
                "volume_m3": 9.0,
                "planned_plant": "plant-walldorf",
                "planned_truck": "truck-07",
                "planned_batch_at": "2026-11-11T06:00:00Z",
                "eta_on_site": "2026-11-11T06:25:00Z",
                "discharge_window": {
                    "start": "2026-11-11T06:25:00Z",
                    "end": "2026-11-11T07:30:00Z"
                },
                "frozen": false,
                "mix": "C30/37-XC4"
            }]
        }));
        assert_wire_format::<PlantPlan>(json!({
            "plant": "plant-walldorf",
            "version": null,
            "created_at": null,
            "loads": []
        }));
    }

    #[test]
    fn plant_ticket_wire_format() {
        let planned = json!({
            "plant": "plant-walldorf",
            "ticket_no": "WAL-000003",
            "vehicle": "truck-25",
            "ts_ms": 1794236511059_i64,
            "event": "batching",
            "mix": "C30/37-XC4",
            "volume_m3": 9.0,
            "order": "10001001/BW-4711",
            "load_no": 2
        });
        assert_wire_format::<PlantTicket>(planned.clone());
        let ticket: PlantTicket = serde_json::from_value(planned).unwrap();
        assert_eq!(ticket.topic(), "plants/plant-walldorf/tickets");

        // A trip outside the plan leaves both out.
        assert_wire_format::<PlantTicket>(json!({
            "plant": "plant-walldorf",
            "ticket_no": "WAL-000004",
            "vehicle": "truck-25",
            "ts_ms": 1794236511059_i64,
            "event": "loaded",
            "mix": "C30/37-XC4",
            "volume_m3": 8.0
        }));
    }
}
