//! Telemetry shared by the services (SPEC §8): console logs, spans exported to Jaeger over
//! OTLP/HTTP, Prometheus metrics on their own port, and the W3C trace context carried in NATS
//! message headers, so one trace follows an event from ingest into dispatch. Where there are no
//! NATS headers (HTTP, an MQTT payload), the context travels as a `traceparent` string.
//!
//! - Only the service's own spans are exported, at `info` and above. Library internals (NATS,
//!   sqlx, hyper, the exporter's own HTTP calls) stay out. `RUST_LOG` controls the console only.
//! - Every trace is kept, except one whose root span is created with the field `noise = true`
//!   (GPS and drum messages, about two a second). Children follow their parent's decision, also
//!   across NATS: an unsampled `traceparent` makes dispatch drop its side of the trace too.
//! - If Jaeger is unreachable, spans are dropped and the exporter logs a warning. Nothing waits
//!   for tracing.

use std::collections::HashMap;
use std::net::SocketAddr;

use async_nats::HeaderMap;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator};
use opentelemetry::trace::{Link, SpanKind, TraceContextExt, TraceId, TracerProvider as _};
use opentelemetry::{Context, KeyValue, Value};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{
    Sampler, SamplingDecision, SamplingResult, SdkTracerProvider, ShouldSample,
};
use tracing::{Level, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

/// Histogram buckets for every metric whose name ends in `_seconds`, around dispatch's 500 ms
/// local search budget.
const SECONDS_BUCKETS: &[f64] = &[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 10.0];

#[derive(Debug, thiserror::Error)]
pub enum TelemetryError {
    #[error("span exporter: {0}")]
    Spans(#[from] opentelemetry_otlp::ExporterBuildError),
    #[error("metrics listener: {0}")]
    Metrics(#[from] metrics_exporter_prometheus::BuildError),
    #[error("logging: {0}")]
    Logging(#[from] tracing_subscriber::util::TryInitError),
}

/// Keeps tracing running. Dropping it flushes the spans still queued, so a clean shutdown loses none.
#[must_use = "dropping it shuts tracing down"]
pub struct Telemetry(SdkTracerProvider);

impl Drop for Telemetry {
    fn drop(&mut self) {
        if let Err(error) = self.0.shutdown() {
            tracing::warn!(%error, "flushing spans at shutdown failed");
        }
    }
}

/// Sets up console logs, span export and the metrics listener for `service`. The service's name
/// is also its crate name, which selects the spans to export. The OTLP endpoint comes from the
/// standard `OTEL_EXPORTER_OTLP_ENDPOINT` (default `http://localhost:4318`).
pub fn init(service: &'static str, metrics_addr: SocketAddr) -> Result<Telemetry, TelemetryError> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build()?;
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_sampler(sampler())
        .with_resource(Resource::builder().with_service_name(service).build())
        .build();
    let console = tracing_subscriber::fmt::layer()
        .with_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")));
    let spans = tracing_opentelemetry::layer()
        .with_tracer(provider.tracer(service))
        .with_filter(Targets::new().with_target(service, Level::INFO));
    tracing_subscriber::registry()
        .with(console)
        .with(spans)
        .try_init()?;
    PrometheusBuilder::new()
        .with_http_listener(metrics_addr)
        .set_buckets_for_metric(Matcher::Suffix("_seconds".into()), SECONDS_BUCKETS)?
        .install()?;
    Ok(Telemetry(provider))
}

fn sampler() -> Sampler {
    Sampler::ParentBased(Box::new(DropNoise))
}

/// The root sampler: keeps every trace except those whose root is marked `noise = true`.
#[derive(Debug, Clone)]
struct DropNoise;

impl ShouldSample for DropNoise {
    fn should_sample(
        &self,
        _parent: Option<&Context>,
        _trace_id: TraceId,
        _name: &str,
        _kind: &SpanKind,
        attributes: &[KeyValue],
        _links: &[Link],
    ) -> SamplingResult {
        let noise = attributes
            .iter()
            .any(|kv| kv.key.as_str() == "noise" && kv.value == Value::Bool(true));
        SamplingResult {
            decision: if noise {
                SamplingDecision::Drop
            } else {
                SamplingDecision::RecordAndSample
            },
            attributes: Vec::new(),
            trace_state: Default::default(),
        }
    }
}

/// Writes the current span's trace context into `headers` as `traceparent`, for the consumer to
/// continue the trace.
pub fn inject(headers: &mut HeaderMap) {
    TraceContextPropagator::new()
        .inject_context(&Span::current().context(), &mut HeaderInjector(headers));
}

/// Makes `span` a child of the trace context in `headers`, if they carry one. Call it before the
/// span is first entered; without a context, `span` stays a root.
pub fn continue_trace(span: &Span, headers: Option<&HeaderMap>) {
    let Some(headers) = headers else { return };
    let cx = TraceContextPropagator::new().extract(&HeaderExtractor(headers));
    if cx.span().span_context().is_valid() {
        // Fails only when tracing is off for this span, or it already started (a bug the tests catch).
        let _ = span.set_parent(cx);
    }
}

/// `span`'s trace context as a W3C `traceparent` value, for carriers other than NATS headers (an
/// HTTP header, an MQTT payload), or `None` when it isn't traced.
pub fn traceparent(span: &Span) -> Option<String> {
    let mut carrier = HashMap::new();
    TraceContextPropagator::new().inject_context(&span.context(), &mut carrier);
    carrier.remove("traceparent")
}

/// Makes `span` a child of the trace in a W3C `traceparent` value. Call it before the span is
/// first entered. A value that isn't a valid `traceparent` is ignored, and `span` stays a root.
pub fn continue_from(span: &Span, traceparent: &str) {
    let carrier = HashMap::from([("traceparent".to_owned(), traceparent.to_owned())]);
    let cx = TraceContextPropagator::new().extract(&carrier);
    if cx.span().span_context().is_valid() {
        // Fails only when tracing is off for this span, or it already started (a bug the tests catch).
        let _ = span.set_parent(cx);
    }
}

/// The id of the trace `span` belongs to, as Jaeger shows it, or `None` when it isn't traced.
pub fn trace_id(span: &Span) -> Option<String> {
    let cx = span.context();
    let span_context = cx.span().span_context().clone();
    span_context
        .is_valid()
        .then(|| span_context.trace_id().to_string())
}

struct HeaderInjector<'a>(&'a mut HeaderMap);

impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key, value);
    }
}

struct HeaderExtractor<'a>(&'a HeaderMap);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|value| value.as_str())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.iter().map(|(name, _)| name.as_ref()).collect()
    }
}

/// Span capture for tests, with the services' sampler.
#[cfg(any(test, feature = "testing"))]
pub mod testing {
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::Layer;
    use tracing_subscriber::filter::LevelFilter;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    pub use opentelemetry_sdk::trace::{InMemorySpanExporter, SpanData};

    /// Records every span at `info` and above in memory, as the services would export them, while
    /// the guard lives. The guard is per thread, so use a current-thread runtime and create spans
    /// for blocking threads before handing them over.
    pub fn capture() -> (DefaultGuard, InMemorySpanExporter) {
        // tracing caches per callsite whether anyone listens. A parallel test thread without a
        // subscriber could cache "no" for a span this test needs, so the process-wide default
        // listens at `info` too (and drops everything).
        let _ = tracing::subscriber::set_global_default(
            tracing_subscriber::registry().with(LevelFilter::INFO),
        );
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .with_sampler(super::sampler())
            .build();
        let layer = tracing_opentelemetry::layer()
            .with_tracer(provider.tracer("test"))
            .with_filter(LevelFilter::INFO);
        (
            tracing_subscriber::registry().with(layer).set_default(),
            exporter,
        )
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use tracing::info_span;

    use super::testing::{SpanData, capture};
    use super::*;

    fn named<'a>(spans: &'a [SpanData], name: &str) -> &'a SpanData {
        spans.iter().find(|s| s.name == name).unwrap()
    }

    #[test]
    fn a_trace_crosses_message_headers() {
        let (_guard, exporter) = capture();
        let mut headers = HeaderMap::new();
        {
            let publish = info_span!("publish");
            let _entered = publish.enter();
            inject(&mut headers);
        }
        assert!(headers.get("traceparent").is_some());
        let apply = info_span!("apply event");
        continue_trace(&apply, Some(&headers));
        drop(apply.enter());
        drop(apply);

        let spans = exporter.get_finished_spans().unwrap();
        let (publish, apply) = (named(&spans, "publish"), named(&spans, "apply event"));
        assert_eq!(
            apply.span_context.trace_id(),
            publish.span_context.trace_id()
        );
        assert_eq!(apply.parent_span_id, publish.span_context.span_id());
    }

    #[test]
    fn without_a_traceparent_a_span_starts_its_own_trace() {
        let (_guard, exporter) = capture();
        let apply = info_span!("apply event");
        continue_trace(&apply, Some(&HeaderMap::new()));
        drop(apply);
        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(
            named(&spans, "apply event").parent_span_id,
            opentelemetry::trace::SpanId::INVALID
        );
    }

    #[test]
    fn a_trace_crosses_a_traceparent_string() {
        let (_guard, exporter) = capture();
        let click = info_span!("console action");
        let traceparent = traceparent(&click).unwrap();
        let fetch = info_span!("plan sync");
        continue_from(&fetch, &traceparent);
        drop(fetch);
        drop(click);

        let spans = exporter.get_finished_spans().unwrap();
        let (click, fetch) = (named(&spans, "console action"), named(&spans, "plan sync"));
        assert_eq!(fetch.span_context.trace_id(), click.span_context.trace_id());
        assert_eq!(fetch.parent_span_id, click.span_context.span_id());
    }

    #[test]
    fn an_invalid_traceparent_string_leaves_a_root() {
        let (_guard, exporter) = capture();
        for (i, junk) in [
            "",
            "garbage",
            "00-00000000000000000000000000000000-0000000000000000-01",
        ]
        .into_iter()
        .enumerate()
        {
            let span = info_span!("plan sync", i);
            continue_from(&span, junk);
        }
        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 3);
        assert!(
            spans
                .iter()
                .all(|s| s.parent_span_id == opentelemetry::trace::SpanId::INVALID)
        );
    }

    #[test]
    fn noise_is_dropped_on_both_sides_of_the_stream() {
        let (_guard, exporter) = capture();
        let mut headers = HeaderMap::new();
        {
            let root = info_span!("mqtt message", noise = true);
            let _root = root.enter();
            let publish = info_span!("publish");
            let _publish = publish.enter();
            inject(&mut headers);
        }
        // The consumer only sees the header, which says "not sampled".
        let traceparent = headers.get("traceparent").unwrap().as_str().to_owned();
        assert!(traceparent.ends_with("-00"), "{traceparent}");
        let apply = info_span!("apply event");
        continue_trace(&apply, Some(&headers));
        drop(apply);
        drop(info_span!("mqtt message", noise = false));

        let spans = exporter.get_finished_spans().unwrap();
        let names: Vec<&str> = spans.iter().map(|s| s.name.as_ref()).collect();
        assert_eq!(names, ["mqtt message"], "only the root without noise");
    }
}
