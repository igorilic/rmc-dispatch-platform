# ADR 0006: Haversine travel time behind a trait (and when to switch to OSRM)

- **Status:** Accepted
- **Date:** 2026-11-09 (milestone 4)

## Context

Everything in the plan hangs on travel time. It sets the batch time worked back from a site's
requested arrival, whether a load is discharged within its mix design's limit, how long a truck is
busy, and how many kilometres it drives empty. The planner and local search ask for travel times
many times per plan, for every candidate plant, truck and slot.

A real routing engine knows roads, bridges and traffic, but it's a service to run, with map data
to load and update, and a network call per question unless the answers are cached. The demo region
is small (the Rhine-Neckar area, 3 plants and about 40 sites), and the data is simulated anyway.

## Decision

Travel time is a trait, `TravelTimeProvider`
([`crates/dispatch/src/travel.rs`](../../crates/dispatch/src/travel.rs)), with two questions:
the road distance between two points, and the driving time when leaving at a given moment. The
only implementation, `HaversineTravelTime`, is:

- great-circle distance × a road factor of 1.3;
- ÷ 45 km/h, or 30 km/h when leaving between 07:00–09:00 or 16:00–18:00 Berlin time.

The planner, local search, the checker and the re-dispatch worker see only the trait.

## Consequences

- The planner is fast and deterministic: no I/O, no cache, and the property tests and benchmarks
  run with no network.
- Swapping in a routing engine touches one constructor in the worker, not the algorithm.
- **Accuracy:** straight lines ignore the road network. That matters most here because of the
  rivers: the Rhine and the Neckar have few bridges, so two places 5 km apart across the river can
  be 20 minutes apart by road. The road factor averages that out, and doesn't capture it.
- Rush hours are a fixed step function. There's no traffic data, no day-of-week effect, and no
  difference between a truck that is loaded and one that is empty.

## When to switch to OSRM (or another routing engine)

Switch when one of these is true:

1. Real telematics show planned ETAs off by more than the slack the plan relies on. The pour gap
   threshold and the mix design's limit are the slack. A useful trigger: the median error of a
   planned arrival against the GPS arrival exceeds 5 minutes, or arrivals are late in a pattern by
   route, such as across a river.
2. Sites are added in places where straight lines are badly wrong: across rivers or motorways
   without a nearby crossing, or in hilly terrain.
3. Dispatchers stop trusting the plan's times. That is the real cost, and it shows up before any
   metric does.

How: an `OsrmTravelTime` that uses OSRM's `table` service to precompute a plant × site × time-band
matrix when the world changes, and answers the trait from memory. The planner's inner loop must
never wait on the network. Haversine stays as the fallback when OSRM is unavailable, so a routing
outage degrades the plan instead of stopping dispatch.

## Alternatives considered

- **OSRM from day one.** It's more accurate, but it means another service in compose, map data to
  download and process, and a matrix cache, all before the planner exists. That's premature while
  every truck and order is simulated.
- **A hosted routing API** (Google, HERE, Mapbox). It has traffic-aware times, but it costs per
  request, needs credentials, and the tests and CI would depend on the internet and a key.
- **A fixed travel-time matrix in the seed data.** It's simple, and it could even be accurate for
  the seeded sites. But it breaks for every site that isn't in the matrix, and its numbers would be
  invented anyway.
