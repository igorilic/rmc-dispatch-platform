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

use std::collections::HashMap;
use std::sync::Arc;

use domain::{Location, PlantId, SiteId};

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
}

/// Roads by plant and site. The default has none, so every drive is the straight line.
#[derive(Debug, Default)]
pub struct Roads(HashMap<(PlantId, SiteId), Arc<Road>>);

impl Roads {
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
