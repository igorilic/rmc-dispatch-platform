# ADR 0003: Declarative per-customer mappings instead of per-customer code

- **Status:** Accepted
- **Date:** 2026-11-09 (milestone 3)

## Context

Every contractor sends orders their own way (SPEC §5):

| Customer | Channel | Format |
|---|---|---|
| bauwerk-ag | REST webhook | JSON with their own field names, full replacements on update |
| rheinbau-gmbh | SFTP drop | Semicolon CSV, decimal commas, local times |
| neckar-infra | REST webhook | XML, with schema v1 and v2 live at the same time |

There will be more customers, and each one changes its format now and then. If every integration
is Rust code, every onboarding and every format change needs a developer, a review and a release
of ingest. It also invites customer-specific shortcuts into the core.

## Decision

Each integration is described in YAML, in `customers/<name>/mapping.yaml`, and one generic engine
interprets it ([`crates/ingest/src/mapping.rs`](../../crates/ingest/src/mapping.rs)). A mapping
declares:

- the channel (webhook or SFTP) and the master-data customer it may order for;
- schema versions, each with a `when` condition, so v1 and v2 run side by side;
- for each canonical field, where it comes from (a CSV column, a JSON pointer, or an XML path)
  and how to convert it: value maps, decimal commas, scales such as litres to m³, and timestamps
  in the mapping's time zone;
- which fields make the idempotency key, or a hash of the content.

Every record is checked against master data, and every problem in a record is collected, not just
the first. `ingest-cli validate --customer <name> --file <sample>` runs the same engine on a sample
before go-live. Records that fail land in the DLQ with every reason, and
`ingest-cli dlq replay --customer <name>` reprocesses them after the mapping is fixed. In
Kubernetes, the operator parses a changed mapping with the same parser and refuses to roll out one
that doesn't parse.

The rule is enforced by convention and review: ingest has no customer-specific Rust. Customer
names appear in its tests and comments, never in its logic.

## Consequences

- Onboarding a customer, or following a format change, means editing YAML and running `validate`.
  In Kubernetes it means applying a ConfigMap, a Secret and a `CustomerIntegration`, with no new
  ingest release.
- The DLQ and `validate` speak the mapping's language (field, row, reason), so the person who
  writes the mapping can read the errors.
- The engine has to grow when a format needs something new, such as another number format or a
  conditional field. That growth has to stay generic, which is slower than a one-off hack, but
  every later customer benefits.
- **Limit:** some formats won't fit a declarative mapping, for example ones that need state across
  records or a lookup in another system. The answer then is a new generic feature, or a
  pre-processing step outside ingest, not a Rust branch for one customer.
- Truck telematics are the exception. There is one vendor with a fixed format, so they have a small
  Rust mapper ([`telematics.rs`](../../crates/ingest/src/telematics.rs)) and no YAML.

## Alternatives considered

- **A Rust module per customer behind a trait.** It's the fastest way to handle each format, and
  the compiler checks it. But every change needs a developer and a release, and nothing stops
  customer logic from leaking into shared code.
- **A general transformation language** (JSONata, jq or XSLT per customer). It's more expressive,
  but it's a second language to learn and debug, it's hard to validate against master data with
  useful messages, and CSV and XML would each need their own.
- **An integration platform** (Apache Camel, an iPaaS). It's powerful, but it means another runtime
  to operate next to a Rust service, for three formats.
