//! The console's one copy of the world, shared by every open page. Master data comes from the
//! seed; live state is merged from the EVENTS stream, dispatch's `/world` and plan versions, and
//! the edge agent's `/status`.
//!
//! The merge rules keep the copy right whatever arrives first:
//! - a truck field keeps its newest value by report time, so a `/world` read while dispatch is
//!   still behind never undoes a newer event, and an event that arrives late changes nothing.
//!   A value from before the world's clock moved back gives way to the next report
//!   ([`domain::clock::trusted_until`]);
//! - a truck's route shows only while the truck is en route or returning and the route is no
//!   older than that status, so a route from an earlier drive, stale or replayed, is never drawn:
//!   a line can be missing, never wrong;
//! - a plant keeps its newest ticket;
//! - plan versions only move forward, so a version announced twice is pushed once.

use std::collections::{BTreeMap, VecDeque};

use chrono::{DateTime, Utc};
use domain::{DeliveryStatus, Location, PlantStatus, Seed, TruckStatus};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// How many actions the log keeps.
const LOG: usize = 50;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PlantView {
    pub id: String,
    pub name: String,
    pub location: Location,
    pub bays: u32,
    pub rate_m3_per_h: f64,
    pub status: PlantStatus,
    pub last_ticket: Option<Ticket>,
}

/// A batch ticket as the map shows it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Ticket {
    pub ticket_no: String,
    pub truck: String,
    pub event: DeliveryStatus,
    pub volume_m3: f64,
    /// The order and load number, when the plant's batch computer knows them.
    pub order: Option<String>,
    pub load: Option<u32>,
    pub at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SiteView {
    pub id: String,
    pub name: String,
    pub customer: String,
    pub customer_name: String,
    /// Its own coordinates, or its postal code's centroid.
    pub location: Location,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TruckView {
    pub id: String,
    pub home: String,
    pub capacity_m3: f64,
    pub status: TruckStatus,
    /// When the status was reported; `None` before the first report.
    pub status_at: Option<DateTime<Utc>>,
    pub position: Option<Location>,
    pub position_at: Option<DateTime<Utc>>,
}

/// What a page draws for a truck's road: the route it reported for the drive it is on, or
/// `None` when no line shows.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RouteView {
    pub truck: String,
    pub path: Option<Vec<Location>>,
}

/// A truck's newest route, and when it was reported.
#[derive(Debug, Clone)]
struct Route {
    at: DateTime<Utc>,
    path: Vec<Location>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PlanView {
    pub version: i64,
    pub created_at: DateTime<Utc>,
    /// Dispatch's diff from the version before; `null` for the first version.
    pub diff: Value,
    /// The trace the version was made in, for the page's link to Jaeger.
    pub trace_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EdgeView {
    /// Whether the agent answered the last poll. When it didn't, `status` is the last one it gave.
    pub reachable: bool,
    /// When `status` was read.
    pub at: Option<DateTime<Utc>>,
    /// The agent's `/status`: links, outbox, sync lag, plan version and age.
    pub status: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize)]
pub struct UplinkView {
    /// Whether the console can cut the plant's uplink at all (a plant and its uplink exist).
    pub available: bool,
    /// Whether one of the uplink's proxies is disabled, as Toxiproxy says.
    pub cut: bool,
    /// The Make chaos run going on, or the last one.
    pub run: Option<Run>,
}

/// Where a Make chaos run is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Step {
    Ordering,
    Planning,
    Syncing,
    Cut,
    Draining,
    Done,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Run {
    pub step: Step,
    pub orders: Vec<String>,
    pub plan_version: Option<i64>,
    pub cut_until: Option<DateTime<Utc>>,
    /// Tickets waiting in the outbox when the cut ended.
    pub held: Option<u64>,
    pub report: Option<String>,
    pub trace_id: Option<String>,
}

impl Run {
    pub fn active(&self) -> bool {
        !matches!(self.step, Step::Done | Step::Failed)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LogEntry {
    pub at: DateTime<Utc>,
    pub action: String,
    pub target: String,
    /// What came of it, in a few words: `done`, or why it was refused or failed.
    pub outcome: String,
    pub trace_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Features {
    /// The plant's edge agent can be watched.
    pub edge: bool,
    /// Make chaos can cut the plant's uplink.
    pub chaos: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct Links {
    pub jaeger: String,
    pub grafana: String,
    pub api: String,
}

/// Everything a page starts from.
#[derive(Debug, Clone, Serialize)]
pub struct Snapshot {
    /// The world clock's time; the page measures ages against it, not against its own clock.
    pub now: DateTime<Utc>,
    pub edge_plant: String,
    pub features: Features,
    pub links: Links,
    pub plants: Vec<PlantView>,
    pub sites: Vec<SiteView>,
    pub trucks: Vec<TruckView>,
    /// The roads that show, by truck id.
    pub routes: BTreeMap<String, Vec<Location>>,
    pub plan: Option<PlanView>,
    pub edge: Option<EdgeView>,
    pub uplink: UplinkView,
    pub log: Vec<LogEntry>,
}

/// Dispatch's `GET /world`, as far as the console reads it.
#[derive(Debug, Clone, Deserialize)]
pub struct World {
    pub plan_version: Option<i64>,
    pub plants: Vec<WorldPlant>,
    pub trucks: Vec<WorldTruck>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WorldPlant {
    pub id: String,
    pub status: PlantStatus,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WorldTruck {
    pub id: String,
    pub status: TruckStatus,
    pub status_at: Option<DateTime<Utc>>,
    pub position: Option<Location>,
    pub position_at: Option<DateTime<Utc>>,
}

pub struct State {
    pub plants: BTreeMap<String, PlantView>,
    pub sites: Vec<SiteView>,
    pub trucks: BTreeMap<String, TruckView>,
    routes: BTreeMap<String, Route>,
    /// What the pages were last told each truck shows: when its route was reported, and the
    /// status a page draws it as.
    shown: BTreeMap<String, (DateTime<Utc>, TruckStatus)>,
    pub plan: Option<PlanView>,
    pub edge: Option<EdgeView>,
    pub uplink: UplinkView,
    pub log: VecDeque<LogEntry>,
}

/// `plant-walldorf` → `Walldorf`.
fn plant_name(id: &str) -> String {
    let name = id.strip_prefix("plant-").unwrap_or(id);
    let mut chars = name.chars();
    chars
        .next()
        .map(|first| first.to_uppercase().chain(chars).collect())
        .unwrap_or_default()
}

/// Whether a report at `at` replaces the one stored at `stored`: it is newer, or the stored one is
/// from later than [`domain::clock::trusted_until`], before the world's clock moved back.
fn newer(at: Option<DateTime<Utc>>, stored: Option<DateTime<Utc>>) -> bool {
    match (at, stored) {
        (Some(at), Some(stored)) => at > stored || stored > domain::clock::trusted_until(),
        (Some(_), None) => true,
        (None, _) => false,
    }
}

impl State {
    /// The world as the seed has it, before anything live arrives.
    pub fn from_seed(seed: &Seed, uplink: bool) -> State {
        let plants = seed
            .plants
            .iter()
            .map(|p| {
                let view = PlantView {
                    id: p.id.0.clone(),
                    name: plant_name(&p.id.0),
                    location: p.location,
                    bays: p.loading_bays,
                    rate_m3_per_h: p.batching_rate_m3_per_h,
                    status: p.status,
                    last_ticket: None,
                };
                (view.id.clone(), view)
            })
            .collect();
        let sites = seed
            .sites
            .iter()
            .filter_map(|s| {
                Some(SiteView {
                    id: s.id.0.clone(),
                    name: s.name.clone(),
                    customer: s.customer.0.clone(),
                    customer_name: seed
                        .customer(&s.customer.0)
                        .map(|c| c.name.clone())
                        .unwrap_or_default(),
                    location: seed.geocoder.locate(s)?,
                })
            })
            .collect();
        let trucks = seed
            .trucks
            .iter()
            .map(|t| {
                let view = TruckView {
                    id: t.id.0.clone(),
                    home: t.home_plant.0.clone(),
                    capacity_m3: t.drum_capacity_m3,
                    status: t.status,
                    status_at: None,
                    position: t.last_position,
                    position_at: None,
                };
                (view.id.clone(), view)
            })
            .collect();
        State {
            plants,
            sites,
            trucks,
            routes: BTreeMap::new(),
            shown: BTreeMap::new(),
            plan: None,
            edge: None,
            uplink: UplinkView {
                available: uplink,
                ..UplinkView::default()
            },
            log: VecDeque::new(),
        }
    }

    /// A status report, unless the stored one is newer. Returns the truck when it changed.
    pub fn truck_status(
        &mut self,
        id: &str,
        status: TruckStatus,
        at: DateTime<Utc>,
    ) -> Option<TruckView> {
        let truck = self.trucks.get_mut(id)?;
        if !newer(Some(at), truck.status_at) {
            return None;
        }
        truck.status = status;
        truck.status_at = Some(at);
        let view = truck.clone();
        self.forget_route_from_before_the_move(id);
        Some(view)
    }

    /// A drive's route goes out right after its status, so once a status is taken, a held route
    /// stamped later than the world's now belongs to no drive since the clock moved back. It goes,
    /// so the next drive's route is taken and nothing older stands in for it meanwhile.
    fn forget_route_from_before_the_move(&mut self, id: &str) {
        if self
            .routes
            .get(id)
            .is_some_and(|r| r.at > domain::clock::now())
        {
            self.routes.remove(id);
        }
    }

    /// A GPS fix, unless the stored one is newer. Returns the truck when it changed.
    pub fn truck_position(
        &mut self,
        id: &str,
        position: Location,
        at: DateTime<Utc>,
    ) -> Option<TruckView> {
        let truck = self.trucks.get_mut(id)?;
        if !newer(Some(at), truck.position_at) {
            return None;
        }
        truck.position = Some(position);
        truck.position_at = Some(at);
        Some(truck.clone())
    }

    /// A route report, unless the stored one is newer. Returns what a page should draw for the
    /// truck when that changed.
    pub fn truck_route(
        &mut self,
        id: &str,
        path: Vec<Location>,
        at: DateTime<Utc>,
    ) -> Option<RouteView> {
        if !self.trucks.contains_key(id) {
            return None;
        }
        if !newer(Some(at), self.routes.get(id).map(|r| r.at)) {
            return None;
        }
        self.routes.insert(id.to_owned(), Route { at, path });
        self.route_shown(id)
    }

    /// What a page should draw for the truck, when it differs from what the pages were told.
    /// Call it after every change to the truck's status or route.
    pub fn route_shown(&mut self, id: &str) -> Option<RouteView> {
        // A page draws a line in its truck's status, so a new status under the same route sends
        // the line again, after the truck.
        let status = self.trucks.get(id).map(|t| t.status);
        let shows = self.shows(id).map(|r| r.at).zip(status);
        if shows == self.shown.get(id).copied() {
            return None;
        }
        match shows {
            Some(drawn) => self.shown.insert(id.to_owned(), drawn),
            None => self.shown.remove(id),
        };
        Some(RouteView {
            truck: id.to_owned(),
            path: self.shows(id).map(|r| r.path.clone()),
        })
    }

    /// The route a truck shows: its newest, while the truck drives the drive it began. A route
    /// goes out right after the status that starts its drive, so it is never older than that
    /// status; one that is older belongs to an earlier drive, and any later status means the
    /// drive is over. So a report from an earlier drive, or a lost one, leaves no line, never a
    /// wrong one. A route from later than [`domain::clock::trusted_until`] comes from before the
    /// world's clock moved back, so it would look no older than any status since: it never shows.
    fn shows(&self, id: &str) -> Option<&Route> {
        let truck = self.trucks.get(id)?;
        let route = self.routes.get(id)?;
        let driving = matches!(truck.status, TruckStatus::EnRoute | TruckStatus::Returning);
        (driving
            && truck
                .status_at
                .is_some_and(|status_at| route.at >= status_at)
            && route.at <= domain::clock::trusted_until())
        .then_some(route)
    }

    pub fn plant_status(&mut self, id: &str, status: PlantStatus) -> Option<PlantView> {
        let plant = self.plants.get_mut(id)?;
        if plant.status == status {
            return None;
        }
        plant.status = status;
        Some(plant.clone())
    }

    /// A batch ticket, unless the plant's last one is newer.
    pub fn ticket(&mut self, plant: &str, ticket: Ticket) -> Option<PlantView> {
        let plant = self.plants.get_mut(plant)?;
        let stored = plant.last_ticket.as_ref().map(|t| t.at);
        if !newer(Some(ticket.at), stored) {
            return None;
        }
        plant.last_ticket = Some(ticket);
        Some(plant.clone())
    }

    /// Dispatch's `/world`: plant statuses as dispatch has them, and each truck field where its
    /// report is newer than the one the console holds. Returns what changed.
    pub fn world(&mut self, world: &World) -> (Vec<PlantView>, Vec<TruckView>) {
        let plants = world
            .plants
            .iter()
            .filter_map(|p| self.plant_status(&p.id, p.status))
            .collect();
        let mut trucks = Vec::new();
        for reported in &world.trucks {
            let Some(truck) = self.trucks.get_mut(&reported.id) else {
                continue;
            };
            let before = truck.clone();
            let status_taken = newer(reported.status_at, truck.status_at);
            if status_taken {
                truck.status = reported.status;
                truck.status_at = reported.status_at;
            }
            if newer(reported.position_at, truck.position_at) {
                truck.position = reported.position;
                truck.position_at = reported.position_at;
            }
            if *truck != before {
                trucks.push(truck.clone());
            }
            if status_taken {
                self.forget_route_from_before_the_move(&reported.id);
            }
        }
        (plants, trucks)
    }

    pub fn plan_version(&self) -> Option<i64> {
        self.plan.as_ref().map(|p| p.version)
    }

    /// A plan version, if it is newer than the one held, or the held version again with the
    /// trace it lacked (a version caught up from `/world` before its announcement came). Returns
    /// it when it was taken.
    pub fn plan(&mut self, plan: PlanView) -> Option<PlanView> {
        let taken = self.plan.as_ref().is_none_or(|held| {
            held.version < plan.version
                || (held.version == plan.version
                    && held.trace_id.is_none()
                    && plan.trace_id.is_some())
        });
        if !taken {
            return None;
        }
        self.plan = Some(plan.clone());
        Some(plan)
    }

    /// The edge's latest poll. Returns it when anything but the time changed.
    pub fn edge(&mut self, edge: EdgeView) -> Option<EdgeView> {
        let changed = self
            .edge
            .as_ref()
            .is_none_or(|held| held.reachable != edge.reachable || held.status != edge.status);
        self.edge = Some(edge.clone());
        changed.then_some(edge)
    }

    /// The uplink's state as Toxiproxy reports it. Returns it when it changed.
    pub fn uplink_cut(&mut self, cut: bool) -> Option<UplinkView> {
        if self.uplink.cut == cut {
            return None;
        }
        self.uplink.cut = cut;
        Some(self.uplink.clone())
    }

    /// Changes the Make chaos run, and returns the uplink to push.
    pub fn run(&mut self, change: impl FnOnce(&mut Run)) -> Option<UplinkView> {
        change(self.uplink.run.as_mut()?);
        Some(self.uplink.clone())
    }

    pub fn log(&mut self, entry: LogEntry) -> LogEntry {
        if self.log.len() == LOG {
            self.log.pop_front();
        }
        self.log.push_back(entry.clone());
        entry
    }

    pub fn snapshot(
        &self,
        now: DateTime<Utc>,
        edge_plant: &str,
        features: Features,
        links: Links,
    ) -> Snapshot {
        Snapshot {
            now,
            edge_plant: edge_plant.to_owned(),
            features,
            links,
            plants: self.plants.values().cloned().collect(),
            sites: self.sites.clone(),
            trucks: self.trucks.values().cloned().collect(),
            routes: self
                .trucks
                .keys()
                .filter_map(|id| Some((id.clone(), self.shows(id)?.path.clone())))
                .collect(),
            plan: self.plan.clone(),
            edge: self.edge.clone(),
            uplink: self.uplink.clone(),
            log: self.log.iter().rev().cloned().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeDelta;

    fn state() -> State {
        State::from_seed(&Seed::rhine_neckar().unwrap(), true)
    }

    fn at(minute: i64) -> DateTime<Utc> {
        "2026-11-10T09:00:00Z".parse::<DateTime<Utc>>().unwrap() + TimeDelta::minutes(minute)
    }

    fn world(truck: WorldTruck) -> World {
        World {
            plan_version: None,
            plants: Vec::new(),
            trucks: vec![truck],
        }
    }

    #[test]
    fn the_seed_gives_every_plant_site_and_truck() {
        let s = state();
        assert_eq!((s.plants.len(), s.sites.len(), s.trucks.len()), (3, 40, 25));
        assert_eq!(s.plants["plant-walldorf"].name, "Walldorf");
        assert_eq!(s.sites[0].customer_name, "Bauwerk AG");
    }

    #[test]
    fn the_newest_report_wins_whichever_way_it_comes() {
        let mut s = state();
        let lagging = WorldTruck {
            id: "truck-07".into(),
            status: TruckStatus::EnRoute,
            status_at: Some(at(1)),
            position: None,
            position_at: None,
        };
        assert!(
            s.truck_status("truck-07", TruckStatus::OutOfService, at(2))
                .is_some()
        );
        assert_eq!(
            s.world(&world(lagging.clone())).1,
            [],
            "an older /world changes nothing"
        );
        assert_eq!(s.trucks["truck-07"].status, TruckStatus::OutOfService);
        assert!(
            s.truck_status("truck-07", TruckStatus::Returning, at(1))
                .is_none(),
            "a late event changes nothing either"
        );

        let newer = WorldTruck {
            status_at: Some(at(3)),
            position: Some(Location {
                lat: 49.4,
                lon: 8.6,
            }),
            position_at: Some(at(3)),
            ..lagging
        };
        let (_, changed) = s.world(&world(newer));
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].status, TruckStatus::EnRoute);
        assert_eq!(changed[0].position_at, Some(at(3)));
    }

    #[test]
    fn a_report_from_before_the_clock_moved_back_gives_way() {
        // Held from before CLOCK_OFFSET_HOURS moved the world's clock 6 hours back.
        let now = domain::clock::now();
        let earlier_world = now + TimeDelta::hours(6);
        let here = Location {
            lat: 49.4,
            lon: 8.6,
        };
        let ticket = |at| Ticket {
            ticket_no: "WAL-000001".into(),
            truck: "truck-18".into(),
            event: DeliveryStatus::Batching,
            volume_m3: 8.0,
            order: None,
            load: None,
            at,
        };
        let mut s = state();
        s.world(&world(WorldTruck {
            id: "truck-07".into(),
            status: TruckStatus::Returning,
            status_at: Some(earlier_world),
            position: Some(here),
            position_at: Some(earlier_world),
        }));
        s.truck_route("truck-07", road(), earlier_world);
        s.ticket("plant-walldorf", ticket(earlier_world));

        s.truck_route("truck-07", road(), now);
        assert_eq!(s.routes["truck-07"].at, now);
        assert!(
            s.truck_status("truck-07", TruckStatus::Available, now)
                .is_some()
        );
        assert!(s.truck_position("truck-07", here, now).is_some());
        assert!(s.ticket("plant-walldorf", ticket(now)).is_some());

        let mut s = state();
        s.truck_status("truck-07", TruckStatus::EnRoute, earlier_world);
        let (_, changed) = s.world(&world(WorldTruck {
            id: "truck-07".into(),
            status: TruckStatus::Loading,
            status_at: Some(now),
            position: None,
            position_at: None,
        }));
        assert_eq!(changed[0].status, TruckStatus::Loading, "/world too");

        // A route from before the move is never drawn, not even for the next drive before that
        // drive's own route comes.
        let mut s = state();
        s.truck_status("truck-07", TruckStatus::EnRoute, earlier_world);
        s.truck_route("truck-07", road(), earlier_world);
        s.truck_status("truck-07", TruckStatus::EnRoute, now);
        s.route_shown("truck-07");
        assert_eq!(snapshot(&s).routes, BTreeMap::new());

        // Within 5 minutes of the time a route from before the move carries, it is trusted again;
        // still, the next drive never shows it, and that drive's own route is taken. The same
        // when the status comes by /world.
        let other: Vec<Location> = road().into_iter().rev().collect();
        for by_world in [false, true] {
            let mut s = state();
            s.truck_route("truck-07", road(), now + TimeDelta::minutes(3));
            if by_world {
                s.world(&world(WorldTruck {
                    id: "truck-07".into(),
                    status: TruckStatus::EnRoute,
                    status_at: Some(now),
                    position: None,
                    position_at: None,
                }));
            } else {
                s.truck_status("truck-07", TruckStatus::EnRoute, now);
            }
            assert_eq!(
                snapshot(&s).routes,
                BTreeMap::new(),
                "by /world: {by_world}"
            );
            s.truck_route("truck-07", other.clone(), now);
            assert_eq!(
                snapshot(&s).routes,
                BTreeMap::from([("truck-07".to_owned(), other.clone())]),
                "by /world: {by_world}"
            );
        }

        // A clock a minute fast still orders reports.
        let mut s = state();
        let fast = now + TimeDelta::minutes(1);
        s.truck_status("truck-07", TruckStatus::OutOfService, fast);
        assert!(
            s.truck_status("truck-07", TruckStatus::Available, now)
                .is_none()
        );
    }

    fn road() -> Vec<Location> {
        vec![
            Location {
                lat: 49.4875,
                lon: 8.466,
            },
            Location {
                lat: 49.5512,
                lon: 8.6589,
            },
        ]
    }

    /// Whether a change draws the line (`Some(true)`), takes it away (`Some(false)`), or is none.
    fn draws(change: Option<RouteView>) -> Option<bool> {
        change.map(|r| r.path.is_some())
    }

    fn snapshot(s: &State) -> Snapshot {
        let links = Links {
            jaeger: String::new(),
            grafana: String::new(),
            api: String::new(),
        };
        let features = Features {
            edge: true,
            chaos: true,
        };
        s.snapshot(at(0), "plant-walldorf", features, links)
    }

    #[test]
    fn a_route_shows_only_while_its_truck_drives_it() {
        let mut s = state();
        // A drive's route goes out right after its status: one that lands while the truck still
        // loads waits for the status, and a status at the same millisecond shows it.
        s.truck_status("truck-07", TruckStatus::Loading, at(0));
        assert_eq!(draws(s.truck_route("truck-07", road(), at(1))), None);
        s.truck_status("truck-07", TruckStatus::EnRoute, at(1));
        assert_eq!(draws(s.route_shown("truck-07")), Some(true));
        // Arriving takes it away, and a late, older route changes nothing.
        s.truck_status("truck-07", TruckStatus::OnSite, at(2));
        assert_eq!(draws(s.route_shown("truck-07")), Some(false));
        assert_eq!(draws(s.truck_route("truck-07", road(), at(0))), None);
        // The drive back shows its own route, never the one from before it.
        s.truck_status("truck-07", TruckStatus::Returning, at(3));
        assert_eq!(draws(s.route_shown("truck-07")), None);
        assert_eq!(draws(s.truck_route("truck-07", road(), at(3))), Some(true));
        // A simulators restart reports available first.
        s.truck_status("truck-07", TruckStatus::Available, at(4));
        assert_eq!(draws(s.route_shown("truck-07")), Some(false));
        // A breakdown takes the line away; the drive home after the repair brings its own.
        s.truck_status("truck-07", TruckStatus::EnRoute, at(5));
        assert_eq!(draws(s.truck_route("truck-07", road(), at(5))), Some(true));
        s.truck_status("truck-07", TruckStatus::OutOfService, at(6));
        assert_eq!(draws(s.route_shown("truck-07")), Some(false));
        s.truck_status("truck-07", TruckStatus::Returning, at(7));
        assert_eq!(draws(s.route_shown("truck-07")), None);
        let home = vec![road()[1], road()[0]];
        let change = s.truck_route("truck-07", home.clone(), at(7));
        assert_eq!(change.and_then(|r| r.path), Some(home));
        // A /world status changes what shows as an event does.
        let arrived = WorldTruck {
            id: "truck-07".into(),
            status: TruckStatus::OnSite,
            status_at: Some(at(8)),
            position: None,
            position_at: None,
        };
        assert_eq!(s.world(&world(arrived)).1.len(), 1);
        assert_eq!(draws(s.route_shown("truck-07")), Some(false));

        // After a console restart, /world brings the status and the stream replays each truck's
        // last route: one from an earlier drive stays hidden.
        let mut restarted = state();
        let driving = WorldTruck {
            id: "truck-07".into(),
            status: TruckStatus::EnRoute,
            status_at: Some(at(5)),
            position: None,
            position_at: None,
        };
        restarted.world(&world(driving));
        assert_eq!(draws(restarted.route_shown("truck-07")), None);
        assert_eq!(
            draws(restarted.truck_route("truck-07", road(), at(3))),
            None
        );
    }

    #[test]
    fn a_route_is_pushed_only_when_what_shows_changes() {
        let mut s = state();
        s.truck_status("truck-07", TruckStatus::EnRoute, at(1));
        assert_eq!(
            draws(s.route_shown("truck-07")),
            None,
            "nothing to draw yet"
        );
        assert_eq!(draws(s.truck_route("truck-07", road(), at(1))), Some(true));
        assert_eq!(
            draws(s.truck_route("truck-07", road(), at(1))),
            None,
            "a duplicate"
        );
        assert_eq!(
            draws(s.truck_route("truck-07", road(), at(0))),
            None,
            "a late, older route changes nothing"
        );
        s.truck_position("truck-07", road()[0], at(2));
        assert_eq!(
            draws(s.route_shown("truck-07")),
            None,
            "a fix changes nothing"
        );
        s.truck_status("truck-08", TruckStatus::Returning, at(2));
        assert_eq!(
            draws(s.truck_route("truck-08", road(), at(1))),
            None,
            "older than its drive"
        );
        assert_eq!(
            draws(s.truck_route("truck-99", road(), at(1))),
            None,
            "no such truck"
        );
        assert!(!s.routes.contains_key("truck-99"));
        s.truck_status("truck-09", TruckStatus::Loading, at(1));
        assert_eq!(
            draws(s.truck_route("truck-09", road(), at(2))),
            None,
            "loading: not driving yet"
        );
        // A page styles a line by its truck's status, so a new status under the line that shows
        // sends it again: here the next drive's route showed under an en_route the console had
        // from before, and returning lands after it.
        s.truck_status("truck-10", TruckStatus::EnRoute, at(1));
        assert_eq!(draws(s.truck_route("truck-10", road(), at(3))), Some(true));
        s.truck_status("truck-10", TruckStatus::Returning, at(3));
        assert_eq!(
            draws(s.route_shown("truck-10")),
            Some(true),
            "drawn again, as returning"
        );
        let routes = snapshot(&s).routes;
        assert_eq!(routes.keys().collect::<Vec<_>>(), ["truck-07", "truck-10"]);
        assert_eq!(routes["truck-07"], road());
    }

    #[test]
    fn plan_versions_only_move_forward() {
        let mut s = state();
        let plan = |version| PlanView {
            version,
            created_at: at(0),
            diff: Value::Null,
            trace_id: None,
        };
        assert!(s.plan(plan(2)).is_some());
        assert!(s.plan(plan(2)).is_none(), "announced twice, pushed once");
        assert!(s.plan(plan(1)).is_none());
        assert_eq!(s.plan_version(), Some(2));

        let traced = PlanView {
            trace_id: Some("t".into()),
            ..plan(2)
        };
        assert!(
            s.plan(traced.clone()).is_some(),
            "the trace a caught-up version lacked"
        );
        assert!(s.plan(traced).is_none());
        assert!(s.plan(plan(2)).is_none(), "never loses its trace");
    }

    #[test]
    fn the_edge_is_pushed_only_when_it_changes() {
        let mut s = state();
        let edge = |reachable, at| EdgeView {
            reachable,
            at: Some(at),
            status: None,
        };
        assert!(s.edge(edge(true, at(0))).is_some());
        assert!(s.edge(edge(true, at(1))).is_none(), "only the time changed");
        assert!(s.edge(edge(false, at(2))).is_some());
    }

    #[test]
    fn the_log_keeps_the_latest_actions_newest_first() {
        let mut s = state();
        for n in 0..=LOG {
            s.log(LogEntry {
                at: at(n as i64),
                action: format!("action {n}"),
                target: String::new(),
                outcome: "done".into(),
                trace_id: None,
            });
        }
        let links = Links {
            jaeger: String::new(),
            grafana: String::new(),
            api: String::new(),
        };
        let features = Features {
            edge: true,
            chaos: true,
        };
        let snapshot = s.snapshot(at(0), "plant-walldorf", features, links);
        assert_eq!(snapshot.log.len(), LOG);
        assert_eq!(snapshot.log[0].action, format!("action {LOG}"));
    }
}
