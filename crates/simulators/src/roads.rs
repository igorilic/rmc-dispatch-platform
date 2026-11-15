//! Roads the simulated trucks drive along.
//!
//! A [`Road`] is a polyline with the cumulative distance to each of its points. [`Road::at`] finds
//! the segment a distance falls in with a binary search and interpolates linearly inside it, in
//! lat/lon, which is close enough over a segment's few hundred metres. A distance at or past either
//! end gives that end exactly, so drives start and end on the plant's and the site's own
//! coordinates, and a distance that lands on a point gives that point. Zero-length segments and
//! roads give their point: the search never lands inside one, so nothing divides by zero.
//!
//! [`Road::straight`] is the fallback where there is no road. It is the two end points, measured as
//! haversine × [`ROAD_FACTOR`], so a whole drive on it moves and reports its speed as before roads.
//! A drive home from part-way along it reports the km it covers along the line, within 0.15 % of
//! the haversine figure.
//! [`Roads`] looks a road up by plant and site; a truck drives the way back on the same road.
//! [`Roads::parse`] reads them from the file `make routes` writes, `src/bin/routes.rs`.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, ensure};
use domain::{Location, PlantId, SiteId};
use serde::Deserialize;

use crate::fleet::ROAD_FACTOR;

/// A road as points, with the distance driven to reach each one.
#[derive(Debug, Clone, PartialEq)]
pub struct Road {
    points: Vec<Location>,
    /// Cumulative km to each point: 0 first, the road's length last.
    km: Vec<f64>,
}

impl Road {
    /// The road through `points`, measured point to point; `None` with fewer than two.
    pub fn new(points: Vec<Location>) -> Option<Road> {
        if points.len() < 2 {
            return None;
        }
        let mut km = Vec::with_capacity(points.len());
        let mut total = 0.0;
        km.push(total);
        for pair in points.windows(2) {
            total += pair[0].distance_km(pair[1]);
            km.push(total);
        }
        Some(Road { points, km })
    }

    /// The straight line, measured as haversine × [`ROAD_FACTOR`] like a drive before roads.
    pub fn straight(from: Location, to: Location) -> Road {
        Road {
            points: vec![from, to],
            km: vec![0.0, from.distance_km(to) * ROAD_FACTOR],
        }
    }

    /// The road's length in km.
    pub fn length(&self) -> f64 {
        self.km[self.km.len() - 1]
    }

    /// The point `km` along the road, or the first or last point at or past either end.
    pub fn at(&self, km: f64) -> Location {
        // The first point at or beyond `km`. With 0 < i < len, km[i - 1] < km <= km[i].
        let i = self.km.partition_point(|&k| k < km);
        if i == 0 {
            return self.points[0];
        }
        if i == self.points.len() {
            return self.points[i - 1];
        }
        let (k0, k1) = (self.km[i - 1], self.km[i]);
        if km >= k1 {
            return self.points[i];
        }
        let t = (km - k0) / (k1 - k0);
        let (a, b) = (self.points[i - 1], self.points[i]);
        Location {
            lat: a.lat + (b.lat - a.lat) * t,
            lon: a.lon + (b.lon - a.lon) * t,
        }
    }

    /// The stretch a drive from `from_km` to `to_km` covers, in its direction: the point at
    /// `from_km`, the road's points strictly between, and the point at `to_km`. It never has more
    /// points than the road, and a drive that goes nowhere is its one point twice.
    pub fn path(&self, from_km: f64, to_km: f64) -> Vec<Location> {
        // Kms off the road stop at its ends, as at() does, so the bound on points always holds.
        let (low, high) = (
            from_km.min(to_km).max(0.0),
            from_km.max(to_km).min(self.length()),
        );
        let mut path = vec![self.at(low)];
        path.extend(
            self.points
                .iter()
                .zip(&self.km)
                .filter(|&(_, &km)| low < km && km < high)
                .map(|(point, _)| *point),
        );
        path.push(self.at(high));
        if from_km > to_km {
            path.reverse();
        }
        path
    }
}

/// Roads by plant and site. The default has none, so every drive is the straight line.
#[derive(Debug, Default)]
pub struct Roads(HashMap<(PlantId, SiteId), Arc<Road>>);

/// A roads file: the ODbL notice, which only the tests read, then the roads.
#[derive(Deserialize)]
struct RoadsFile {
    roads: Vec<FileRoad>,
}

#[derive(Deserialize)]
struct FileRoad {
    plant: PlantId,
    site: SiteId,
    /// `[lat, lon]` pairs.
    path: Vec<[f64; 2]>,
}

impl Roads {
    /// The roads in `text`, a roads file like `deploy/seed/rhine-neckar.routes.json`. A pair given
    /// twice is an error, so an edit can't hide that another road is gone.
    pub fn parse(text: &str) -> anyhow::Result<Roads> {
        let file: RoadsFile = serde_json::from_str(text).context("read the roads file")?;
        let mut roads = HashMap::new();
        for r in file.roads {
            let pair = format!("{} to {}", r.plant.0, r.site.0);
            let points = r
                .path
                .into_iter()
                .map(|[lat, lon]| Location { lat, lon })
                .collect();
            let road = Road::new(points)
                .with_context(|| format!("the road from {pair} has fewer than 2 points"))?;
            ensure!(
                roads.insert((r.plant, r.site), Arc::new(road)).is_none(),
                "two roads from {pair}"
            );
        }
        Ok(Roads(roads))
    }

    /// The road from `plant` at `from` to `site` at `to`, or the straight line without one.
    pub fn between(
        &self,
        plant: &PlantId,
        site: &SiteId,
        from: Location,
        to: Location,
    ) -> Arc<Road> {
        self.0
            .get(&(plant.clone(), site.clone()))
            .cloned()
            .unwrap_or_else(|| Arc::new(Road::straight(from, to)))
    }
}

/// Metres from `p` to the nearest point of `line`, which needs at least two points. It measures on a
/// flat projection around `p`, which is close enough within a few kilometres.
pub fn metres_off(p: Location, line: &[Location]) -> f64 {
    const M_PER_DEGREE: f64 = 111_195.0;
    let k = p.lat.to_radians().cos();
    let xy = |l: Location| {
        (
            (l.lon - p.lon) * k * M_PER_DEGREE,
            (l.lat - p.lat) * M_PER_DEGREE,
        )
    };
    line.windows(2)
        .map(|w| {
            let ((ax, ay), (bx, by)) = (xy(w[0]), xy(w[1]));
            let (dx, dy) = (bx - ax, by - ay);
            let len2 = dx * dx + dy * dy;
            let t = if len2 > 0.0 {
                (-(ax * dx + ay * dy) / len2).clamp(0.0, 1.0)
            } else {
                0.0
            };
            (ax + t * dx).hypot(ay + t * dy)
        })
        .fold(f64::INFINITY, f64::min)
}

impl FromIterator<(PlantId, SiteId, Road)> for Roads {
    fn from_iter<I: IntoIterator<Item = (PlantId, SiteId, Road)>>(roads: I) -> Self {
        Roads(
            roads
                .into_iter()
                .map(|(plant, site, road)| ((plant, site), Arc::new(road)))
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fleet::{MAX_ROUTE_MESSAGE, route_message};
    use domain::Seed;

    /// Only the simulators binary compiles the roads in; the library's tests read them here.
    const FILE: &str = include_str!("../../../deploy/seed/rhine-neckar.routes.json");

    #[test]
    fn a_stretch_of_road_keeps_the_drive_s_direction() {
        // Two points between the ends, so the order of the points in between shows as well.
        const D: Location = Location {
            lat: 49.45,
            lon: 8.80,
        };
        let road = Road::new(vec![A, B, C, D]).unwrap();
        let (corner, end) = (A.distance_km(B), road.length());
        assert_eq!(road.path(0.0, end), [A, B, C, D], "out: the whole road");
        assert_eq!(
            road.path(end, 0.0),
            [D, C, B, A],
            "back: the same road reversed"
        );
        let halfway = road.at(corner / 2.0);
        assert_eq!(
            road.path(corner / 2.0, 0.0),
            [halfway, A],
            "home from part-way out"
        );
        assert_eq!(
            road.path(corner / 2.0, end),
            [halfway, B, C, D],
            "on from part-way out"
        );
        let late = end - C.distance_km(D) / 2.0;
        assert_eq!(
            road.path(late, 0.0),
            [road.at(late), C, B, A],
            "home from near the site"
        );
        assert_eq!(road.path(0.0, 0.0), [A, A], "standing at the plant");
        assert_eq!(
            road.path(-1.0, end + 1.0),
            [A, B, C, D],
            "kms off the road stop at its ends"
        );
    }

    #[test]
    fn every_plant_has_a_road_to_every_site_ending_exactly_there() {
        let seed = Seed::rhine_neckar().unwrap();
        let roads = Roads::parse(FILE).unwrap();
        assert_eq!(roads.0.len(), seed.plants.len() * seed.sites.len());
        for plant in &seed.plants {
            for site in &seed.sites {
                let road = roads
                    .0
                    .get(&(plant.id.clone(), site.id.clone()))
                    .unwrap_or_else(|| panic!("no road from {} to {}", plant.id.0, site.id.0));
                assert_eq!(road.points[0], plant.location, "it starts at the plant");
                assert_eq!(
                    road.points[road.points.len() - 1],
                    seed.geocoder.locate(site).unwrap(),
                    "it ends where the simulators place {}",
                    site.id.0
                );
            }
        }
        // The data is ODbL: a regeneration must never drop its notice.
        let notice: serde_json::Value = serde_json::from_str(FILE).unwrap();
        assert!(
            notice["attribution"]
                .as_str()
                .unwrap()
                .contains("OpenStreetMap")
        );
        assert_eq!(notice["license"], "ODbL-1.0");
        assert_eq!(
            notice["license_url"],
            "https://opendatacommons.org/licenses/odbl/1-0/"
        );
        assert!(!notice["source"].as_str().unwrap().is_empty());
    }

    #[test]
    fn a_roads_file_with_a_pair_twice_is_rejected() {
        let road = r#"{"plant":"plant-a","site":"site-1","path":[[49.4,8.6],[49.5,8.7]]}"#;
        assert!(Roads::parse(&format!(r#"{{"roads":[{road}]}}"#)).is_ok());
        assert!(
            Roads::parse(&format!(r#"{{"roads":[{road},{road}]}}"#)).is_err(),
            "a pair twice, as from a hand edit or a merge, would hide that one road is gone"
        );
    }

    #[test]
    fn every_road_fits_one_route_message() {
        let seed = Seed::rhine_neckar().unwrap();
        let vehicle = seed
            .trucks
            .iter()
            .map(|t| t.id.0.as_str())
            .max_by_key(|id| id.len())
            .unwrap();
        let roads = Roads::parse(FILE).unwrap();
        assert_eq!(
            roads.0.len(),
            seed.plants.len() * seed.sites.len(),
            "measures every road"
        );
        for ((plant, site), road) in &roads.0 {
            let bytes = route_message(vehicle, &road.points).unwrap().len();
            assert!(
                bytes <= MAX_ROUTE_MESSAGE,
                "the road from {} to {} makes a {bytes}-byte route message",
                plant.0,
                site.0
            );
        }
    }

    const A: Location = Location {
        lat: 49.40,
        lon: 8.60,
    };
    const B: Location = Location {
        lat: 49.40,
        lon: 8.70,
    };
    const C: Location = Location {
        lat: 49.45,
        lon: 8.70,
    };

    #[test]
    fn a_point_along_a_road_lies_on_it() {
        let road = Road::new(vec![A, B, C]).unwrap();
        let corner = A.distance_km(B);
        assert_eq!(road.length(), corner + B.distance_km(C));
        assert_eq!(road.at(0.0), A, "the start is exact");
        assert_eq!(road.at(road.length()), C, "the end is exact");
        assert_eq!(road.at(corner), B, "the corner's km gives the corner");
        let halfway = road.at(corner / 2.0);
        assert!(
            (halfway.lat - A.lat).abs() < 1e-12 && (halfway.lon - 8.65).abs() < 1e-9,
            "{halfway:?}"
        );
        assert_eq!(road.at(-1.0), A, "before the start clamps to it");
        assert_eq!(road.at(road.length() + 1.0), C, "past the end clamps to it");

        let straight = Road::straight(A, C);
        assert_eq!(
            straight.length(),
            A.distance_km(C) * ROAD_FACTOR,
            "the straight line is measured like a drive without roads"
        );
        let home = Road::straight(A, A);
        assert_eq!(
            (home.length(), home.at(0.0), home.at(1.0)),
            (0.0, A, A),
            "a zero-length road gives its point, never NaN"
        );
    }
}
