#!/usr/bin/env bash
# make routes (SPEC §11): deploy/seed/rhine-neckar.routes.json, the road from each plant to each site,
# from OpenStreetMap through a local OSRM. Run it by hand after the seed's plants or sites change;
# the file is committed, and CI and make test never run this. The file is ODbL, as its notice says,
# while the code and the seed stay MIT.
#
# It downloads three Geofabrik extracts (741 MB) into target/routes, merges them, clips them to the
# seed's extent plus about 10 km, builds an OSRM car graph there and asks it for every road
# (crates/simulators/src/bin/routes.rs, which prints a summary to check). target/routes keeps about
# 2 GB of work files afterwards; rm -r target/routes frees them.
#
# Nothing bumps OSRM_IMAGE (Dependabot doesn't read scripts): check it at each milestone.
set -euo pipefail
cd "$(dirname "$0")/../.."

OSRM_IMAGE=ghcr.io/project-osrm/osrm-backend:v26.11.0-debian # multi-arch; there is no plain v26.11.0
GEOFABRIK=https://download.geofabrik.de/europe/germany
EXTRACTS=(baden-wuerttemberg/karlsruhe-regbez hessen rheinland-pfalz) # the seed spans all three states
PORT=5005 # macOS's AirPlay Receiver holds 5000, and the k3d registry 5050
OSRM=pourline-routes-osrm
WORK=target/routes
OUT=deploy/seed/rhine-neckar.routes.json

for tool in docker curl osmium cargo; do
  command -v "$tool" >/dev/null || {
    echo "make routes needs $tool. On macOS: brew install osmium-tool (and Docker and Rust)."
    exit 1
  }
done
mkdir -p "$WORK"
trap 'docker rm -f "$OSRM" >/dev/null 2>&1 || true; rm -f "$OUT.tmp"' EXIT

files=()
dates=()
for extract in "${EXTRACTS[@]}"; do
  file="$WORK/$(basename "$extract").osm.pbf"
  url="$GEOFABRIK/$extract-latest.osm.pbf"
  echo "Downloading $url, unless the copy in $WORK is as new"
  if [ -f "$file" ]; then
    curl -fLR --retry 3 -z "$file" -o "$file.part" "$url"
  else
    curl -fLR --retry 3 -o "$file.part" "$url"
  fi
  if [ -s "$file.part" ]; then mv "$file.part" "$file"; else rm -f "$file.part"; fi
  files+=("$file")
  dates+=("$(osmium fileinfo -g header.option.osmosis_replication_timestamp "$file")")
done
# Merging keeps one copy of what the extracts share only if it is the same version in each.
if [ "$(printf '%s\n' "${dates[@]}" | sort -u | wc -l)" -ne 1 ]; then
  echo "The extracts are from different updates (${dates[*]}): Geofabrik is mid-update. Run make routes again later."
  exit 1
fi

box=$(cargo run -q -p simulators --bin routes -- --bounds)
echo "Merging the extracts of ${dates[0]} and clipping them to $box"
osmium merge --overwrite -o "$WORK/merged.osm.pbf" "${files[@]}"
osmium extract --overwrite -b "$box" -o "$WORK/clip.osm.pbf" "$WORK/merged.osm.pbf"

echo "Building the OSRM car graph"
osrm() { docker run --rm --init --user "$(id -u):$(id -g)" -v "$PWD/$WORK:/data" "$OSRM_IMAGE" "$@"; }
osrm osrm-extract -p /opt/car.lua /data/clip.osm.pbf
osrm osrm-partition /data/clip.osrm
osrm osrm-customize /data/clip.osrm
docker rm -f "$OSRM" >/dev/null 2>&1 || true
docker run -d --name "$OSRM" --user "$(id -u):$(id -g)" -p "127.0.0.1:$PORT:5000" \
  -v "$PWD/$WORK:/data" "$OSRM_IMAGE" osrm-routed --algorithm mld /data/clip.osrm >/dev/null
ready() { curl -fs "http://127.0.0.1:$PORT/nearest/v1/driving/8.466,49.4875" >/dev/null; }
for _ in $(seq 60); do
  ready && break
  sleep 1
done
ready || {
  echo "OSRM didn't answer on 127.0.0.1:$PORT within 60 s:"
  docker logs "$OSRM"
  exit 1
}

echo "Asking OSRM for every road"
cargo run -q -p simulators --bin routes -- "http://127.0.0.1:$PORT" \
  "OSRM ${OSRM_IMAGE##*:} car profile on Geofabrik extracts of ${dates[0]}" >"$OUT.tmp"
mv "$OUT.tmp" "$OUT"
echo "Wrote $OUT. Check the summary above and the diff before committing it."
