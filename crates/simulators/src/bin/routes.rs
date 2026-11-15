//! `make routes`: the road from each plant to each site, from a local OSRM, into
//! `deploy/seed/rhine-neckar.routes.json`. `deploy/seed/routes.sh` runs it by hand; CI and
//! `make test` never do. The file is ODbL, as its notice says.
//!
//! For each pair it asks OSRM for the full car route without ferries. OSRM snaps both ends to the
//! nearest road, so the generator adds the plant's and the site's own coordinates before and after
//! the snapped ones: the site's postal-code centroid when it has no coordinates, as the simulators
//! place it. Then Douglas-Peucker drops points as long as none of them ends up more than
//! [`TOLERANCE_M`] off the line, and the points in between are rounded to 5 decimals, about a
//! metre; the seed's own ends keep theirs. A road whose route message would pass [`MAX_MESSAGE`]
//! bytes fails the run, because rumqttc drops a packet over 10 KiB; a larger tolerance is the knob.
//! The file holds the ODbL notice, then one road per line, sorted by plant and site, so a
//! regeneration diffs road by road. A summary for review goes to stderr.
//!
//! `routes --bounds` prints the seed's extent plus about 10 km on each side, as osmium's `-b` box.

use anyhow::{Context, bail, ensure};
use domain::{Location, Seed, Site};
use serde::{Deserialize, Serialize};
use simulators::fleet::route_message;
use simulators::roads::{Road, metres_off};

/// How far Douglas-Peucker may move the line: invisible below zoom 18.
const TOLERANCE_M: f64 = 5.0;
/// The largest route message a road may make, under rumqttc's 10,240-byte packet limit.
const MAX_MESSAGE: usize = 8 * 1024;
const MARGIN_KM: f64 = 10.0;
const KM_PER_DEGREE: f64 = 111.195;
const ATTRIBUTION: &str = "© OpenStreetMap contributors";
const LICENSE: &str = "ODbL-1.0";
const LICENSE_URL: &str = "https://opendatacommons.org/licenses/odbl/1-0/";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let seed = Seed::rhine_neckar()?;
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [flag] if flag == "--bounds" => {
            println!("{}", bounds(&seed)?);
            Ok(())
        }
        [osrm, source] => generate(&seed, osrm, source).await,
        _ => bail!("usage: routes --bounds | routes <osrm url> <source of the data>"),
    }
}

fn site_location(seed: &Seed, site: &Site) -> anyhow::Result<Location> {
    seed.geocoder.locate(site).with_context(|| {
        format!(
            "ship-to {} has neither coordinates nor a known postal code",
            site.id.0
        )
    })
}

/// Every plant and site, placed as the simulators place them.
fn places(seed: &Seed) -> anyhow::Result<Vec<Location>> {
    let mut places: Vec<Location> = seed.plants.iter().map(|p| p.location).collect();
    for site in &seed.sites {
        places.push(site_location(seed, site)?);
    }
    Ok(places)
}

/// The seed's extent plus [`MARGIN_KM`] on each side, rounded outwards: osmium's `-b` box.
fn bounds(seed: &Seed) -> anyhow::Result<String> {
    let places = places(seed)?;
    let edge = |pick: fn(&Location) -> f64, max: bool| {
        let values = places.iter().map(pick);
        if max {
            values.fold(f64::NEG_INFINITY, f64::max)
        } else {
            values.fold(f64::INFINITY, f64::min)
        }
    };
    let (south, north) = (edge(|p| p.lat, false), edge(|p| p.lat, true));
    let (west, east) = (edge(|p| p.lon, false), edge(|p| p.lon, true));
    let lat = MARGIN_KM / KM_PER_DEGREE;
    // A degree of longitude is shortest at the northern edge, so measure the margin there.
    let lon = MARGIN_KM / (KM_PER_DEGREE * (north + lat).to_radians().cos());
    let (down, up) = (
        |x: f64| (x * 100.0).floor() / 100.0,
        |x: f64| (x * 100.0).ceil() / 100.0,
    );
    Ok(format!(
        "{},{},{},{}",
        down(west - lon),
        down(south - lat),
        up(east + lon),
        up(north + lat)
    ))
}

/// Douglas-Peucker: keeps the point farthest from each span's chord until no dropped point is more
/// than `tolerance_m` off the line. It is greedy, so not always the fewest points. The ends stay.
fn simplify(points: &[Location], tolerance_m: f64) -> Vec<Location> {
    let last = points.len().saturating_sub(1);
    let mut keep = vec![false; points.len()];
    if let Some(first) = keep.first_mut() {
        *first = true;
    }
    if let Some(end) = keep.last_mut() {
        *end = true;
    }
    // ponytail: the farthest point splits each span, O(n²) at worst; roads have ~1,500 points.
    let mut spans = vec![(0, last)];
    while let Some((a, b)) = spans.pop() {
        let (mut far, mut far_m) = (a, 0.0);
        for (i, p) in points.iter().enumerate().take(b).skip(a + 1) {
            let off = metres_off(*p, &[points[a], points[b]]);
            if off > far_m {
                (far, far_m) = (i, off);
            }
        }
        if far_m > tolerance_m {
            keep[far] = true;
            spans.extend([(a, far), (far, b)]);
        }
    }
    points
        .iter()
        .zip(keep)
        .filter_map(|(p, kept)| kept.then_some(*p))
        .collect()
}

/// One road as the file holds it.
#[derive(Serialize)]
struct Entry {
    plant: String,
    site: String,
    path: Vec<[f64; 2]>,
}

/// What the summary reports about one road.
struct Checked {
    pair: String,
    from: Location,
    to: Location,
    points: usize,
    message: usize,
    snap_m: f64,
    ratio: f64,
}

async fn generate(seed: &Seed, osrm: &str, source: &str) -> anyhow::Result<()> {
    // Only ever a local OSRM, so no proxy.
    let client = reqwest::Client::builder().no_proxy().build()?;
    let vehicle = seed
        .trucks
        .iter()
        .map(|t| t.id.0.as_str())
        .max_by_key(|id| id.len())
        .context("the seed has no trucks")?;
    let round = |x: f64| (x * 1e5).round() / 1e5;
    let (mut roads, mut checked) = (Vec::new(), Vec::new());
    for plant in &seed.plants {
        for site in &seed.sites {
            let (from, to) = (plant.location, site_location(seed, site)?);
            let pair = format!("{} to {}", plant.id.0, site.id.0);
            let answer = ask(&client, osrm, from, to)
                .await
                .with_context(|| format!("no road from {pair}"))?;
            let mut points = Vec::with_capacity(answer.points.len() + 2);
            points.push(from);
            points.extend(answer.points);
            points.push(to);
            let mut path = simplify(&points, TOLERANCE_M);
            let last = path.len() - 1;
            for p in &mut path[1..last] {
                *p = Location {
                    lat: round(p.lat),
                    lon: round(p.lon),
                };
            }
            let message = route_message(vehicle, &path)?.len();
            ensure!(
                message <= MAX_MESSAGE,
                "the road from {pair} makes a {message}-byte route message, over {MAX_MESSAGE}: raise TOLERANCE_M"
            );
            let km = Road::new(path.clone()).map_or(0.0, |road| road.length());
            checked.push(Checked {
                pair,
                from,
                to,
                points: path.len(),
                message,
                snap_m: answer.snap_m,
                ratio: km / from.distance_km(to),
            });
            roads.push(Entry {
                plant: plant.id.0.clone(),
                site: site.id.0.clone(),
                path: path.iter().map(|p| [p.lat, p.lon]).collect(),
            });
        }
    }
    roads.sort_by(|a, b| (&a.plant, &a.site).cmp(&(&b.plant, &b.site)));
    let text = file(source, &roads)?;
    print!("{text}");
    summarise(&checked, text.len());
    Ok(())
}

/// The file: the ODbL notice, then one road per line.
fn file(source: &str, roads: &[Entry]) -> serde_json::Result<String> {
    let source = format!("{source}, simplified to {TOLERANCE_M} m");
    let mut text = String::from("{\n");
    for (key, value) in [
        ("attribution", ATTRIBUTION),
        ("license", LICENSE),
        ("license_url", LICENSE_URL),
        ("source", source.as_str()),
    ] {
        text += &format!("  \"{key}\": {},\n", serde_json::to_string(value)?);
    }
    text += "  \"roads\": [\n";
    for (i, road) in roads.iter().enumerate() {
        let comma = if i + 1 < roads.len() { "," } else { "" };
        text += &format!("    {}{comma}\n", serde_json::to_string(road)?);
    }
    text += "  ]\n}\n";
    Ok(text)
}

fn summarise(checked: &[Checked], bytes: usize) {
    let points: usize = checked.iter().map(|c| c.points).sum();
    eprintln!(
        "{} roads, {points} points, {} KB",
        checked.len(),
        bytes / 1000
    );
    let by = |key: fn(&Checked) -> f64| {
        checked
            .iter()
            .max_by(|a, b| key(a).total_cmp(&key(b)))
            .map(|c| (c.pair.as_str(), key(c)))
    };
    if let Some((pair, message)) = by(|c| c.message as f64) {
        eprintln!("largest route message: {message} of {MAX_MESSAGE} bytes ({pair})");
    }
    if let Some((pair, snap)) = by(|c| c.snap_m) {
        eprintln!("largest snap to a road: {snap:.0} m ({pair})");
    }
    if let Some((pair, ratio)) = by(|c| c.ratio) {
        eprintln!("worst road to straight line: {ratio:.2} ({pair})");
    }
    // Snaps over 300 m and detours over 2.5 deserve a look, and so do the five worst detours.
    // Each link opens OpenStreetMap's own car route between the same two points.
    let mut worst: Vec<&Checked> = checked.iter().collect();
    worst.sort_by(|a, b| b.ratio.total_cmp(&a.ratio));
    eprintln!("compare these with OpenStreetMap's own route:");
    for (i, c) in worst.iter().enumerate() {
        if i < 5 || c.snap_m > 300.0 || c.ratio > 2.5 {
            eprintln!(
                "  {}: {:.2} × the straight line, snapped {:.0} m  https://www.openstreetmap.org/directions?engine=fossgis_osrm_car&route={},{};{},{}",
                c.pair, c.ratio, c.snap_m, c.from.lat, c.from.lon, c.to.lat, c.to.lon
            );
        }
    }
}

/// OSRM's road between two points, and how far it snapped the farther end.
struct Answer {
    points: Vec<Location>,
    snap_m: f64,
}

#[derive(Deserialize)]
struct Reply {
    code: String,
    message: Option<String>,
    #[serde(default)]
    routes: Vec<ReplyRoute>,
    #[serde(default)]
    waypoints: Vec<Waypoint>,
}

#[derive(Deserialize)]
struct ReplyRoute {
    geometry: Geometry,
}

/// GeoJSON: `[lon, lat]` pairs.
#[derive(Deserialize)]
struct Geometry {
    coordinates: Vec<[f64; 2]>,
}

#[derive(Deserialize)]
struct Waypoint {
    /// Metres from the asked point to the road.
    distance: f64,
}

async fn ask(
    client: &reqwest::Client,
    osrm: &str,
    from: Location,
    to: Location,
) -> anyhow::Result<Answer> {
    let url = format!(
        "{osrm}/route/v1/driving/{},{};{},{}?overview=full&geometries=geojson&exclude=ferry",
        from.lon, from.lat, to.lon, to.lat
    );
    let body = client.get(&url).send().await?.bytes().await?;
    let reply: Reply = serde_json::from_slice(&body)
        .with_context(|| format!("OSRM answered {}", String::from_utf8_lossy(&body)))?;
    ensure!(
        reply.code == "Ok",
        "OSRM answered {}: {}",
        reply.code,
        reply.message.unwrap_or_default()
    );
    let route = reply
        .routes
        .into_iter()
        .next()
        .context("OSRM found no route")?;
    Ok(Answer {
        points: route
            .geometry
            .coordinates
            .into_iter()
            .map(|[lon, lat]| Location { lat, lon })
            .collect(),
        snap_m: reply
            .waypoints
            .iter()
            .map(|w| w.distance)
            .fold(0.0, f64::max),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(lat: f64, lon: f64) -> Location {
        Location { lat, lon }
    }

    #[test]
    fn douglas_peucker_keeps_corners_and_drops_the_rest() {
        // East along a parallel, with points on the way: only the ends stay.
        let east: Vec<Location> = (0..=10)
            .map(|i| at(49.4, 8.6 + 0.01 * f64::from(i)))
            .collect();
        assert_eq!(simplify(&east, TOLERANCE_M), vec![east[0], east[10]]);
        // East, then north: the corner stays too.
        let mut corner = east.clone();
        corner.extend((1..=10).map(|i| at(49.4 + 0.01 * f64::from(i), 8.7)));
        assert_eq!(
            simplify(&corner, TOLERANCE_M),
            vec![corner[0], corner[10], corner[20]]
        );
        // East, north, east again: whichever corner splits first, the other needs a second split.
        let mut zigzag = corner.clone();
        zigzag.extend((1..=10).map(|i| at(49.5, 8.7 + 0.01 * f64::from(i))));
        assert_eq!(
            simplify(&zigzag, TOLERANCE_M),
            vec![zigzag[0], zigzag[10], zigzag[20], zigzag[30]]
        );
        // A point 3 m off the line goes; one 10 m off stays.
        let metre = 1.0 / (KM_PER_DEGREE * 1000.0);
        let near = vec![at(49.4, 8.6), at(49.4 + 3.0 * metre, 8.65), at(49.4, 8.7)];
        assert_eq!(simplify(&near, TOLERANCE_M), vec![near[0], near[2]]);
        let far = vec![at(49.4, 8.6), at(49.4 + 10.0 * metre, 8.65), at(49.4, 8.7)];
        assert_eq!(simplify(&far, TOLERANCE_M), far);
        // The ends always stay, even when they coincide.
        let still = vec![at(49.4, 8.6), at(49.4, 8.6)];
        assert_eq!(simplify(&still, TOLERANCE_M), still);
    }

    #[test]
    fn the_clip_box_holds_every_plant_and_site_with_about_10_km_to_spare() {
        let seed = Seed::rhine_neckar().unwrap();
        let edges: Vec<f64> = bounds(&seed)
            .unwrap()
            .split(',')
            .map(|x| x.parse().unwrap())
            .collect();
        let &[west, south, east, north] = edges.as_slice() else {
            panic!("osmium's box is west,south,east,north: {edges:?}");
        };
        let places = places(&seed).unwrap();
        for p in &places {
            assert!(
                (west..east).contains(&p.lon) && (south..north).contains(&p.lat),
                "{p:?} is outside {edges:?}"
            );
        }
        // The outermost places on each side sit about 10 km in, never less.
        let nearest = |edge: &dyn Fn(Location) -> Location| {
            places
                .iter()
                .map(|p| p.distance_km(edge(*p)))
                .fold(f64::INFINITY, f64::min)
        };
        let margins = [
            nearest(&|p| at(p.lat, west)),
            nearest(&|p| at(p.lat, east)),
            nearest(&|p| at(south, p.lon)),
            nearest(&|p| at(north, p.lon)),
        ];
        for km in margins {
            assert!(
                (9.99..12.0).contains(&km),
                "a margin of {km} km in {edges:?}"
            );
        }
    }
}
