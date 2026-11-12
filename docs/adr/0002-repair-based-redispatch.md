# ADR 0002: Repair-based re-dispatch instead of full re-optimisation

- **Status:** Accepted
- **Date:** 2026-11-10 (milestone 5)

## Context

A dispatch plan is out of date as soon as reality moves. A truck breaks down, a site's pump is
30 minutes late, a plant goes offline, a customer changes the volume mid-pour, or a new order
arrives. Each of these triggers a re-dispatch (SPEC §4). The new plan has to:

- respect every hard constraint (one load per truck, drum capacity, loading bays, the mix design's
  discharge limit, no out-of-service trucks or offline plants);
- never move a load the plant has already started batching, because that concrete exists;
- change as little as possible. Drivers, plant operators and site foremen act on the plan, and
  reshuffling the whole fleet over one late pump costs more in confusion than it saves in
  kilometres;
- explain every change, so a dispatcher can trust it;
- arrive within about a second. Triggers come in bursts, and the plan is useless if it is stale.

## Decision

Re-dispatch **repairs** the previous version instead of planning from scratch
([`crates/dispatch/src/redispatch.rs`](../../crates/dispatch/src/redispatch.rs)):

1. **Freeze.** Loads with a delivery (batching has started) stay exactly as they are.
2. **Keep.** Loads of orders that no trigger touched stay where they were, if they still meet
   every hard constraint at the same plant, truck and time.
3. **Construct.** The greedy construction heuristic
   ([`planner.rs`](../../crates/dispatch/src/planner.rs)) plans the touched, new and unplanned
   orders around the frozen and kept loads.
4. **Improve.** Local search ([`search.rs`](../../crates/dispatch/src/search.rs)) relocates and
   swaps loads that aren't frozen, under a time budget (`LOCAL_SEARCH_BUDGET_MS`, 500 ms). Its
   cost includes a churn term (`WEIGHT_CHURN_LOAD`) for every load that differs from the previous
   version.
5. **Verify.** An independent checker ([`verify.rs`](../../crates/dispatch/src/verify.rs))
   re-checks the hard constraints. If the repair fails it, a full re-plan of every load that isn't
   frozen is tried. If that fails too, no new version is stored.

Every version is stored with a **diff**: which loads moved, which truck or plant changed, and a
reason per change (the trigger, batching having started, or an earlier load of the same order that
moved). One worker runs re-dispatches one at a time, and merges a burst of triggers into a single
version ([`worker.rs`](../../crates/dispatch/src/worker.rs)).

## Consequences

- Plans are stable. A late pump moves that site's loads and, at most, the loads they collide with.
- Every diff can explain itself, because the repair knows which trigger touched which order.
- Latency is predictable. A repair at 1,000 loads takes about 0.9 s end to end, 500 ms of which is
  the search budget (README, *Benchmarks*).
- The checker is independent of the planner, so a planner bug shows up as a rejected plan rather
  than a truck in two places. Property tests run it on random worlds.
- **Cost:** the result is a good plan, not an optimal one. Greedy construction plus hill climbing
  can get stuck in a local optimum, and the churn term deliberately keeps it close to the previous
  plan even when a global re-plan would be cheaper. A dispatcher can still ask for a full re-plan
  (`POST /plans`).
- **Risk:** the weights (`WEIGHT_*`) encode business trade-offs, such as how much one cold-joint
  minute is worth in empty kilometres. They are guesses until real dispatchers tune them.

## Alternatives considered

- **Full re-optimisation on every trigger.** It's the simplest to reason about, but it reshuffles
  loads nobody asked to move, it can't say why a load moved, and its run time grows with the whole
  plan rather than with the change.
- **A MILP or CP solver** (for example OR-Tools CP-SAT through FFI). It would give provably better
  plans for the static problem, but it adds a large native dependency, makes the time budget harder
  to guarantee, and still needs freezing and churn modelled as constraints. Worth revisiting for
  overnight planning of the next day, where time is plentiful.
- **Metaheuristics with more exploration** (simulated annealing, tabu search). These are a small
  step from the current hill climbing, because the same moves and cost would serve. Left until the
  benchmarks show local optima costing real cold joints.
