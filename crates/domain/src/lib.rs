//! Shared domain types for Pourline (SPEC §3).
//!
//! Units: volumes in m³ (`*_m3`), durations in whole minutes, timestamps in UTC.
//! The serde representation is the wire format between services.
//!
//! An [`Order`] is for one mix only (two mixes means two orders). The [`DispatchPlan`] splits it into
//! planned [`Load`]s, one per truck trip. A [`Delivery`] only exists once batching for a load starts.
//! Until then an order has no deliveries at all.

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PlantId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TruckId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SiteId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CustomerId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OrderId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MixCode(pub String);

/// WGS84 coordinates in decimal degrees.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Location {
    pub lat: f64,
    pub lon: f64,
}

impl Location {
    /// Great-circle (haversine) distance in km. Roads are longer; callers apply a road factor.
    pub fn distance_km(self, other: Location) -> f64 {
        const EARTH_RADIUS_KM: f64 = 6371.0;
        let (lat1, lat2) = (self.lat.to_radians(), other.lat.to_radians());
        let dlat = lat2 - lat1;
        let dlon = (other.lon - self.lon).to_radians();
        let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
        2.0 * EARTH_RADIUS_KM * a.sqrt().asin()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlantStatus {
    Online,
    Offline,
}

/// Daily opening hours in the plant's local time (Europe/Berlin).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatingHours {
    pub open: NaiveTime,
    pub close: NaiveTime,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Plant {
    pub id: PlantId,
    pub location: Location,
    /// How many loads the plant can batch in parallel.
    pub loading_bays: u32,
    pub batching_rate_m3_per_h: f64,
    pub operating_hours: OperatingHours,
    pub status: PlantStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TruckStatus {
    Available,
    Loading,
    EnRoute,
    OnSite,
    Discharging,
    Returning,
    OutOfService,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Truck {
    pub id: TruckId,
    pub home_plant: PlantId,
    pub drum_capacity_m3: f64,
    pub status: TruckStatus,
    /// `None` until the first GPS fix arrives.
    pub last_position: Option<Location>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Site {
    pub id: SiteId,
    pub customer: CustomerId,
    pub location: Location,
    pub access_notes: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MixDesign {
    pub code: MixCode,
    /// Maximum time from batching until discharge has ended.
    pub max_minutes_from_batching: u32,
}

/// Confirmation states, then fulfilment states. Partial and returned states are deliberately not
/// modelled (no `partially_shipped`, `returned`, `halted`, `partially_returned`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    /// Dispatch centre confirmed, customer has not.
    Unconfirmed,
    /// Customer confirmed, dispatch centre has not.
    Pending,
    /// Both dispatch centre and customer confirmed.
    Confirmed,
    /// Batching or loading has started and a delivery exists.
    InProgress,
    /// Loading has finished and the truck has left the plant.
    Shipped,
    /// Every delivery has been unloaded.
    Delivered,
    /// Cancelled by the dispatch centre or the customer.
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Order {
    pub id: OrderId,
    pub customer: CustomerId,
    pub site: SiteId,
    pub mix: MixCode,
    pub total_volume_m3: f64,
    pub requested_start: DateTime<Utc>,
    pub pour_rate_m3_per_h: f64,
    /// Longest allowed gap between two truck arrivals before a cold joint risk.
    pub max_gap_minutes: u32,
    pub status: OrderStatus,
}

/// Closed time interval `[start, end]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeWindow {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

/// A planned truck trip for an order. Identified by `(order, sequence)`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Load {
    pub order: OrderId,
    /// 1-based position of this load within its order.
    pub sequence: u32,
    pub volume_m3: f64,
    pub planned_plant: PlantId,
    pub planned_truck: TruckId,
    pub planned_batch_at: DateTime<Utc>,
    pub eta_on_site: DateTime<Utc>,
    /// From the ETA on site until the mix design's limit after batching.
    pub discharge_window: TimeWindow,
}

/// Trucking-system events in lifecycle order. Washing and returns are deliberately not modelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryStatus {
    Batching,
    Loading,
    Loaded,
    LeftPlant,
    ArrivedAtCustomer,
    Unloading,
    Unloaded,
}

/// What actually happens to a [`Load`]. Created when batching starts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Delivery {
    pub order: OrderId,
    /// Sequence of the [`Load`] this delivery fulfils.
    pub sequence: u32,
    pub plant: PlantId,
    pub truck: TruckId,
    pub volume_m3: f64,
    /// When each trucking event happened.
    pub events: BTreeMap<DeliveryStatus, DateTime<Utc>>,
}

impl Delivery {
    /// The furthest lifecycle event seen so far. Uses lifecycle order, not arrival order, so a
    /// late or duplicate telematics message never moves the status backwards.
    pub fn status(&self) -> DeliveryStatus {
        // A delivery is created when batching starts, so `Batching` is the floor.
        self.events
            .keys()
            .next_back()
            .copied()
            .unwrap_or(DeliveryStatus::Batching)
    }
}

/// A versioned set of loads. Every re-dispatch produces a new version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DispatchPlan {
    pub version: u64,
    pub created_at: DateTime<Utc>,
    pub loads: Vec<Load>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Serialize, de::DeserializeOwned};
    use serde_json::{Value, json};

    /// The JSON is the contract between services: it must parse, and serialise back unchanged.
    fn assert_wire_format<T: Serialize + DeserializeOwned>(wire: Value) {
        let parsed: T = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(parsed).unwrap(), wire);
    }

    #[test]
    fn statuses_are_snake_case() {
        let truck = [
            (TruckStatus::Available, "available"),
            (TruckStatus::Loading, "loading"),
            (TruckStatus::EnRoute, "en_route"),
            (TruckStatus::OnSite, "on_site"),
            (TruckStatus::Discharging, "discharging"),
            (TruckStatus::Returning, "returning"),
            (TruckStatus::OutOfService, "out_of_service"),
        ];
        for (status, wire) in truck {
            assert_eq!(serde_json::to_value(status).unwrap(), wire);
        }
        let order = [
            (OrderStatus::Unconfirmed, "unconfirmed"),
            (OrderStatus::Pending, "pending"),
            (OrderStatus::Confirmed, "confirmed"),
            (OrderStatus::InProgress, "in_progress"),
            (OrderStatus::Shipped, "shipped"),
            (OrderStatus::Delivered, "delivered"),
            (OrderStatus::Cancelled, "cancelled"),
        ];
        for (status, wire) in order {
            assert_eq!(serde_json::to_value(status).unwrap(), wire);
        }
        let delivery = [
            (DeliveryStatus::Batching, "batching"),
            (DeliveryStatus::Loading, "loading"),
            (DeliveryStatus::Loaded, "loaded"),
            (DeliveryStatus::LeftPlant, "left_plant"),
            (DeliveryStatus::ArrivedAtCustomer, "arrived_at_customer"),
            (DeliveryStatus::Unloading, "unloading"),
            (DeliveryStatus::Unloaded, "unloaded"),
        ];
        for (status, wire) in delivery {
            assert_eq!(serde_json::to_value(status).unwrap(), wire);
        }
        assert_eq!(
            serde_json::to_value(PlantStatus::Offline).unwrap(),
            "offline"
        );
    }

    #[test]
    fn distance_is_great_circle_km() {
        let mannheim = Location {
            lat: 49.4875,
            lon: 8.466,
        };
        let heidelberg = Location {
            lat: 49.3988,
            lon: 8.6724,
        };
        // Reference value computed independently (haversine, R = 6371 km).
        assert!((mannheim.distance_km(heidelberg) - 17.8874).abs() < 0.001);
        assert!((heidelberg.distance_km(mannheim) - 17.8874).abs() < 0.001);
        assert_eq!(mannheim.distance_km(mannheim), 0.0);
    }

    #[test]
    fn plant_wire_format() {
        assert_wire_format::<Plant>(json!({
            "id": "plant-walldorf",
            "location": { "lat": 49.3064, "lon": 8.6428 },
            "loading_bays": 2,
            "batching_rate_m3_per_h": 120.0,
            "operating_hours": { "open": "05:00:00", "close": "20:00:00" },
            "status": "online"
        }));
    }

    #[test]
    fn truck_wire_format() {
        assert_wire_format::<Truck>(json!({
            "id": "truck-07",
            "home_plant": "plant-walldorf",
            "drum_capacity_m3": 9.0,
            "status": "en_route",
            "last_position": { "lat": 49.41, "lon": 8.69 }
        }));
        assert_wire_format::<Truck>(json!({
            "id": "truck-08",
            "home_plant": "plant-walldorf",
            "drum_capacity_m3": 8.0,
            "status": "available",
            "last_position": null
        }));
    }

    #[test]
    fn site_and_mix_wire_format() {
        assert_wire_format::<Site>(json!({
            "id": "site-heidelberg-bahnstadt",
            "customer": "bauwerk-ag",
            "location": { "lat": 49.4, "lon": 8.67 },
            "access_notes": "Gate 3, pump on the north side"
        }));
        assert_wire_format::<MixDesign>(json!({
            "code": "C30/37-XC4",
            "max_minutes_from_batching": 90
        }));
    }

    #[test]
    fn order_wire_format() {
        assert_wire_format::<Order>(json!({
            "id": "bauwerk-ag-4711",
            "customer": "bauwerk-ag",
            "site": "site-heidelberg-bahnstadt",
            "mix": "C30/37-XC4",
            "total_volume_m3": 42.5,
            "requested_start": "2026-11-09T06:30:00Z",
            "pour_rate_m3_per_h": 30.0,
            "max_gap_minutes": 20,
            "status": "confirmed"
        }));
    }

    #[test]
    fn delivery_wire_format() {
        assert_wire_format::<Delivery>(json!({
            "order": "bauwerk-ag-4711",
            "sequence": 1,
            "plant": "plant-walldorf",
            "truck": "truck-07",
            "volume_m3": 9.0,
            "events": {
                "batching": "2026-11-09T06:02:00Z",
                "loading": "2026-11-09T06:04:00Z",
                "left_plant": "2026-11-09T06:09:00Z"
            }
        }));
    }

    fn delivery_with(events: &[(DeliveryStatus, &str)]) -> Delivery {
        Delivery {
            order: OrderId("o-1".into()),
            sequence: 1,
            plant: PlantId("p-1".into()),
            truck: TruckId("t-1".into()),
            volume_m3: 9.0,
            events: events
                .iter()
                .map(|(status, at)| (*status, at.parse().unwrap()))
                .collect(),
        }
    }

    #[test]
    fn delivery_status_is_the_furthest_trucking_event() {
        // A delivery only exists once batching has started.
        assert_eq!(delivery_with(&[]).status(), DeliveryStatus::Batching);

        // Telematics arrive out of order: a late `left_plant` must not move the status back.
        let delivery = delivery_with(&[
            (DeliveryStatus::Unloading, "2026-11-09T06:40:00Z"),
            (DeliveryStatus::Batching, "2026-11-09T06:02:00Z"),
            (DeliveryStatus::LeftPlant, "2026-11-09T06:09:00Z"),
        ]);
        assert_eq!(delivery.status(), DeliveryStatus::Unloading);
    }

    #[test]
    fn dispatch_plan_wire_format() {
        assert_wire_format::<DispatchPlan>(json!({
            "version": 3,
            "created_at": "2026-11-09T05:00:00Z",
            "loads": [{
                "order": "bauwerk-ag-4711",
                "sequence": 1,
                "volume_m3": 9.0,
                "planned_plant": "plant-walldorf",
                "planned_truck": "truck-07",
                "planned_batch_at": "2026-11-09T06:00:00Z",
                "eta_on_site": "2026-11-09T06:25:00Z",
                "discharge_window": {
                    "start": "2026-11-09T06:25:00Z",
                    "end": "2026-11-09T07:30:00Z"
                }
            }]
        }));
    }
}
