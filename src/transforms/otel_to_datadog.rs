use bytes::Bytes;
use vector_lib::config::clone_input_definitions;
use vector_lib::configurable::configurable_component;
use vector_lib::transform::{SyncTransform, TransformOutputsBuf};
use vrl::event_path;

use crate::config::OutputId;
use crate::{
    config::{DataType, GenerateConfig, Input, TransformConfig, TransformContext, TransformOutput},
    event::{Event, LogEvent, ObjectMap, TraceEvent, Value},
    schema::Definition,
    transforms::Transform,
};

pub const SPAN_EVENTS_OUTPUT: &str = "span_events";

/// Configuration for the `otel_to_datadog` transform.
///
/// Converts an OpenTelemetry `TraceEvent` (one event per span) into the format
/// expected by the Datadog traces sink. The OTel span fields are mapped to
/// Datadog span fields and wrapped in a `spans` array on a new `TraceEvent`.
///
/// Span events (timestamped annotations on a span) are emitted as `LogEvent`s
/// on the named output `span_events`, suitable for routing to the Datadog logs sink.
#[configurable_component(transform(
    "otel_to_datadog",
    "Convert OpenTelemetry trace events to Datadog trace format."
))]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct OtelToDatadogConfig {}

impl GenerateConfig for OtelToDatadogConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {}).unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "otel_to_datadog")]
impl TransformConfig for OtelToDatadogConfig {
    async fn build(&self, _context: &TransformContext) -> crate::Result<Transform> {
        Ok(Transform::synchronous(OtelToDatadog))
    }

    fn enable_concurrency(&self) -> bool {
        true
    }

    fn input(&self) -> Input {
        Input::trace()
    }

    fn outputs(
        &self,
        _: &TransformContext,
        input_definitions: &[(OutputId, Definition)],
    ) -> Vec<TransformOutput> {
        vec![
            // Default output: the Datadog-formatted TraceEvent (→ datadog_traces sink)
            TransformOutput::new(DataType::Trace, clone_input_definitions(input_definitions)),
            // Named output: one LogEvent per OTel span event (→ datadog_logs sink)
            TransformOutput::new(DataType::Log, clone_input_definitions(input_definitions))
                .with_port(SPAN_EVENTS_OUTPUT),
        ]
    }
}

#[derive(Clone, Debug)]
pub struct OtelToDatadog;

/// Parse a hex string as a u64, returning 0 on any error.
fn hex_to_i64(hex: &str) -> i64 {
    u64::from_str_radix(hex, 16).unwrap_or(0) as i64
}

/// Extract a `&str` from a `Value::Bytes`, returning `None` if wrong type or
/// if the bytes are not valid UTF-8.
fn bytes_as_str(value: &Value) -> Option<&str> {
    if let Value::Bytes(b) = value {
        std::str::from_utf8(b).ok()
    } else {
        None
    }
}

/// Route one attribute value into the correct map.
///
/// Integers and floats go to `metrics` (as `Value::Float` — the Datadog sink
/// panics on non-Float values in the metrics map).  Everything else goes to
/// `meta` (the sink calls `to_string_lossy` on all meta values, so any type
/// is safe there).
fn attr_into_maps(
    k: vrl::value::KeyString,
    v: &Value,
    meta: &mut ObjectMap,
    metrics: &mut ObjectMap,
) {
    match v {
        Value::Integer(i) => {
            // SAFETY: i64 → f64 is always finite (never NaN), so Value::from is safe.
            metrics.insert(k, Value::from(*i as f64));
        }
        Value::Float(_) => {
            metrics.insert(k, v.clone());
        }
        _ => {
            meta.insert(k, v.clone());
        }
    }
}

impl SyncTransform for OtelToDatadog {
    fn transform(&mut self, event: Event, output: &mut TransformOutputsBuf) {
        let Event::Trace(trace) = event else {
            return;
        };

        // --- trace_id: take lower 64 bits (last 16 hex chars) ---
        let trace_id_hex: String = trace
            .get(event_path!("trace_id"))
            .and_then(|v| bytes_as_str(v))
            .map(|s| s.trim().to_owned())
            .unwrap_or_default();

        let trace_id: i64 = {
            let s = trace_id_hex.as_str();
            if s.len() >= 16 {
                hex_to_i64(s.get(s.len() - 16..).unwrap_or(s))
            } else {
                hex_to_i64(s)
            }
        };

        // --- span_id: all 16 hex chars ---
        let span_id_hex: String = trace
            .get(event_path!("span_id"))
            .and_then(|v| bytes_as_str(v))
            .map(|s| s.trim().to_owned())
            .unwrap_or_default();

        let span_id: i64 = hex_to_i64(span_id_hex.trim());

        // --- parent_id: all 16 hex chars; empty string → 0 (root span) ---
        let parent_id: i64 = trace
            .get(event_path!("parent_span_id"))
            .and_then(|v| bytes_as_str(v))
            .map(|s| {
                let s = s.trim();
                if s.is_empty() { 0 } else { hex_to_i64(s) }
            })
            .unwrap_or(0);

        // --- start / end timestamps ---
        let start_ts = trace
            .get(event_path!("start_time_unix_nano"))
            .and_then(|v| {
                if let Value::Timestamp(ts) = v {
                    Some(*ts)
                } else {
                    None
                }
            });

        let end_ts = trace.get(event_path!("end_time_unix_nano")).and_then(|v| {
            if let Value::Timestamp(ts) = v {
                Some(*ts)
            } else {
                None
            }
        });

        let duration: i64 = match (start_ts, end_ts) {
            (Some(start), Some(end)) => (end - start).num_nanoseconds().unwrap_or(0).max(0),
            _ => 0,
        };

        // --- error: status.code == 2 → 1, else 0 ---
        let error: i64 = trace
            .get(event_path!("status"))
            .and_then(|v| {
                if let Value::Object(map) = v {
                    map.get("code").and_then(|c| {
                        if let Value::Integer(code) = c {
                            Some(*code)
                        } else {
                            None
                        }
                    })
                } else {
                    None
                }
            })
            .map(|code| if code == 2 { 1i64 } else { 0i64 })
            .unwrap_or(0);

        // --- name ---
        let name: Value = trace
            .get(event_path!("name"))
            .cloned()
            .unwrap_or_else(|| Value::Bytes(Bytes::from_static(b"")));

        // --- service: resources["service.name"] (OTel spec), fallback to attributes["service.name"] ---
        // Per OTel semantic conventions, service.name is a Resource attribute, not a span attribute.
        let service: Value = trace
            .get(event_path!("resources"))
            .and_then(|v| v.as_object())
            .and_then(|map| map.get("service.name").cloned())
            .filter(|v| matches!(v, Value::Bytes(_)))
            .or_else(|| {
                trace
                    .get(event_path!("attributes"))
                    .and_then(|v| v.as_object())
                    .and_then(|map| map.get("service.name").cloned())
                    .filter(|v| matches!(v, Value::Bytes(_)))
            })
            .unwrap_or_else(|| Value::Bytes(Bytes::from_static(b"")));

        // --- Build the Datadog span object ---
        let mut span_map = ObjectMap::new();
        span_map.insert("trace_id".into(), Value::Integer(trace_id));
        span_map.insert("span_id".into(), Value::Integer(span_id));
        span_map.insert("parent_id".into(), Value::Integer(parent_id));
        if let Some(ts) = start_ts {
            span_map.insert("start".into(), Value::Timestamp(ts));
        }
        span_map.insert("duration".into(), Value::Integer(duration));
        span_map.insert("error".into(), Value::Integer(error));
        span_map.insert("name".into(), name);
        span_map.insert("service".into(), service.clone());

        // --- meta (string tags) + metrics (numeric tags) ---
        // Resource attrs provide a baseline; span attrs override on key collision.
        let mut meta: ObjectMap = ObjectMap::new();
        let mut metrics: ObjectMap = ObjectMap::new();

        if let Some(Value::Object(res)) = trace.get(event_path!("resources")) {
            for (k, v) in res {
                attr_into_maps(k.clone(), v, &mut meta, &mut metrics);
            }
        }
        if let Some(Value::Object(attrs)) = trace.get(event_path!("attributes")) {
            for (k, v) in attrs {
                attr_into_maps(k.clone(), v, &mut meta, &mut metrics);
            }
        }

        // error.message from status.message (only for error spans with a non-empty message)
        if error == 1
            && let Some(Value::Bytes(b)) = trace
                .get(event_path!("status"))
                .and_then(|v| v.as_object())
                .and_then(|o| o.get("message"))
            && !b.is_empty()
        {
            meta.insert("error.message".into(), Value::Bytes(b.clone()));
        }

        // span.kind as a human-readable string for visibility in Datadog tags.
        // OTel kind is stored as Value::Integer (i32 proto enum cast to i64).
        let kind: i64 = trace
            .get(event_path!("kind"))
            .and_then(|v| v.as_integer())
            .unwrap_or(0);
        let kind_str: &'static str = match kind {
            1 => "internal",
            2 => "server",
            3 => "client",
            4 => "producer",
            5 => "consumer",
            _ => "unspecified",
        };
        meta.insert(
            "span.kind".into(),
            Value::Bytes(Bytes::from_static(kind_str.as_bytes())),
        );

        // _top_level = 1.0 marks root spans for APM stats weight calculation.
        // MUST be Value::Float — the aggregator panics on any other type at this key.
        if parent_id == 0 {
            metrics.insert("_top_level".into(), Value::from(1.0f64));
        }

        if !meta.is_empty() {
            span_map.insert("meta".into(), Value::Object(meta));
        }
        if !metrics.is_empty() {
            span_map.insert("metrics".into(), Value::Object(metrics));
        }

        // --- type: technology category derived from attributes, not kind ---
        let attrs_obj = trace
            .get(event_path!("attributes"))
            .and_then(|v| v.as_object());
        let has_attr = |key: &str| attrs_obj.map(|a| a.contains_key(key)).unwrap_or(false);

        let span_type: Option<&'static str> = if has_attr("db.system") {
            Some("db")
        } else if has_attr("messaging.system") || matches!(kind, 4 | 5) {
            Some("queue")
        } else if has_attr("http.method") {
            if kind == 2 { Some("web") } else { Some("http") }
        } else {
            None
        };
        if let Some(t) = span_type {
            span_map.insert(
                "type".into(),
                Value::Bytes(Bytes::from_static(t.as_bytes())),
            );
        }

        // --- resource: specific endpoint / query / operation ---
        // Try HTTP route first, then DB operation, then fall back to span name.
        let resource_name: Option<String> = attrs_obj
            .and_then(|a| {
                if let Some(method) = a.get("http.method").and_then(|v| bytes_as_str(v)) {
                    let path = a
                        .get("http.route")
                        .or_else(|| a.get("http.target"))
                        .and_then(|v| bytes_as_str(v))
                        .unwrap_or("");
                    return Some(format!("{method} {path}"));
                }
                if let Some(op) = a.get("db.operation").and_then(|v| bytes_as_str(v)) {
                    let db = a
                        .get("db.name")
                        .and_then(|v| bytes_as_str(v))
                        .map(|n| format!(" {n}"))
                        .unwrap_or_default();
                    return Some(format!("{op}{db}"));
                }
                None
            })
            .or_else(|| {
                trace
                    .get(event_path!("name"))
                    .and_then(|v| bytes_as_str(v))
                    .map(str::to_owned)
            });
        if let Some(r) = resource_name {
            span_map.insert("resource".into(), Value::Bytes(Bytes::from(r)));
        }

        let span = Value::Object(span_map);

        // --- resource attributes: deployment.environment → env, host.name → hostname ---
        let resources = trace.get(event_path!("resources")).and_then(|v| {
            if let Value::Object(map) = v {
                Some(map.clone())
            } else {
                None
            }
        });

        let env = resources
            .as_ref()
            .and_then(|r| r.get("deployment.environment").cloned())
            .filter(|v| matches!(v, Value::Bytes(_)));

        let hostname = resources
            .as_ref()
            .and_then(|r| r.get("host.name").cloned())
            .filter(|v| matches!(v, Value::Bytes(_)));

        // --- TracerPayload enrichment fields (read by DD traces sink at trace root level) ---
        // telemetry.sdk.language is the OTel resource attribute for the SDK language ("go", "python", etc.)
        // telemetry.sdk.version is the OTel SDK version; scope.version is per-library and less reliable here.
        let language_name = resources
            .as_ref()
            .and_then(|r| r.get("telemetry.sdk.language").cloned())
            .filter(|v| matches!(v, Value::Bytes(_)));
        let tracer_version = resources
            .as_ref()
            .and_then(|r| r.get("telemetry.sdk.version").cloned())
            .filter(|v| matches!(v, Value::Bytes(_)));
        let app_version = resources
            .as_ref()
            .and_then(|r| r.get("service.version").cloned())
            .filter(|v| matches!(v, Value::Bytes(_)));
        let container_id = resources
            .as_ref()
            .and_then(|r| r.get("container.id").cloned())
            .filter(|v| matches!(v, Value::Bytes(_)));
        let runtime_id = resources
            .as_ref()
            .and_then(|r| r.get("process.runtime.description").cloned())
            .filter(|v| matches!(v, Value::Bytes(_)));

        // --- Span events → LogEvents on the "span_events" named output ---
        // Each OTel span event is a timestamped annotation stored in the `.events` array.
        // We emit one LogEvent per event so they can be routed to the DD logs sink, where
        // dd.trace_id / dd.span_id enable log-trace correlation in the Datadog UI.
        //
        // dd.trace_id: decimal string of the lower 64 bits of the trace_id (Datadog injection format)
        // dd.span_id: decimal string of the span_id
        let dd_trace_id = (trace_id as u64).to_string();
        let dd_span_id = (span_id as u64).to_string();

        if let Some(Value::Array(span_events)) = trace.get(event_path!("events")) {
            for span_event in span_events {
                let Value::Object(ev) = span_event else {
                    continue;
                };

                let mut log = LogEvent::default();

                // message = span event name
                if let Some(name_val) = ev.get("name") {
                    log.insert(event_path!("message"), name_val.clone());
                }

                // timestamp from the span event's time_unix_nano
                if let Some(Value::Timestamp(ts)) = ev.get("time_unix_nano") {
                    log.insert(event_path!("timestamp"), Value::Timestamp(*ts));
                } else if let Some(ts) = start_ts {
                    // fall back to span start if event has no timestamp
                    log.insert(event_path!("timestamp"), Value::Timestamp(ts));
                }

                // span event attributes — nested under "attributes" on the log
                if let Some(attrs) = ev.get("attributes") {
                    log.insert(event_path!("attributes"), attrs.clone());
                }

                // trace/span context for log-trace correlation
                log.insert(
                    event_path!("trace_id"),
                    Value::Bytes(Bytes::from(trace_id_hex.clone())),
                );
                log.insert(
                    event_path!("span_id"),
                    Value::Bytes(Bytes::from(span_id_hex.clone())),
                );
                // Datadog log-trace injection format: decimal strings at dd.trace_id / dd.span_id
                log.insert(
                    event_path!("dd", "trace_id"),
                    Value::Bytes(Bytes::from(dd_trace_id.clone())),
                );
                log.insert(
                    event_path!("dd", "span_id"),
                    Value::Bytes(Bytes::from(dd_span_id.clone())),
                );

                // propagate service and hostname so DD logs intake routes them correctly
                log.insert(event_path!("service"), service.clone());
                if let Some(ref h) = hostname {
                    log.insert(event_path!("hostname"), h.clone());
                }
                if let Some(ref e) = env {
                    log.insert(event_path!("ddtags"), e.clone());
                }

                output.push(Some(SPAN_EVENTS_OUTPUT), Event::Log(log));
            }
        }

        // --- Build the output TraceEvent ---
        let mut out = TraceEvent::default();
        out.insert(event_path!("spans"), Value::Array(vec![span]));
        if let Some(v) = env {
            out.insert(event_path!("env"), v);
        }
        if let Some(v) = hostname {
            // "hostname" is read by the TracerPayload builder; "host" is read by the
            // PartitionKey (via log_schema().host_key()) for trace batching/routing.
            out.insert(event_path!("host"), v.clone());
            out.insert(event_path!("hostname"), v);
        }
        if let Some(v) = language_name {
            out.insert(event_path!("language_name"), v);
        }
        if let Some(v) = tracer_version {
            out.insert(event_path!("tracer_version"), v);
        }
        if let Some(v) = app_version {
            out.insert(event_path!("app_version"), v);
        }
        if let Some(v) = container_id {
            out.insert(event_path!("container_id"), v);
        }
        if let Some(v) = runtime_id {
            out.insert(event_path!("runtime_id"), v);
        }

        output.push(None, Event::Trace(out));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::components::assert_transform_compliance;
    use crate::transforms::test::create_topology;
    use chrono::TimeZone;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<OtelToDatadogConfig>();
    }

    #[tokio::test]
    async fn transform_otel_span() {
        assert_transform_compliance(async move {
            // Build the input OTel TraceEvent
            let mut trace = TraceEvent::default();

            // trace_id: 32-char hex; lower 64 bits = "d269b633813fc60c"
            trace.insert(
                event_path!("trace_id"),
                Value::Bytes(Bytes::from_static(b"5b8efff798038103d269b633813fc60c")),
            );
            // span_id: 16-char hex
            trace.insert(
                event_path!("span_id"),
                Value::Bytes(Bytes::from_static(b"eee19b7ec3c1b174")),
            );
            // empty parent → root span
            trace.insert(
                event_path!("parent_span_id"),
                Value::Bytes(Bytes::from_static(b"")),
            );

            // start = 1_000_000_000 ns, end = 1_000_100_000 ns → duration = 100_000 ns
            trace.insert(
                event_path!("start_time_unix_nano"),
                Value::Timestamp(chrono::Utc.timestamp_nanos(1_000_000_000i64)),
            );
            trace.insert(
                event_path!("end_time_unix_nano"),
                Value::Timestamp(chrono::Utc.timestamp_nanos(1_000_100_000i64)),
            );

            // status code 2 → error = 1
            let mut status_map = ObjectMap::new();
            status_map.insert("code".into(), Value::Integer(2));
            trace.insert(event_path!("status"), Value::Object(status_map));

            trace.insert(
                event_path!("name"),
                Value::Bytes(Bytes::from_static(b"my-span")),
            );

            // attributes with service.name
            let mut attrs = ObjectMap::new();
            attrs.insert(
                "service.name".into(),
                Value::Bytes(Bytes::from("my-service")),
            );
            trace.insert(event_path!("attributes"), Value::Object(attrs));

            // Run through the transform
            let config = OtelToDatadogConfig {};
            let (tx, rx) = mpsc::channel(1);
            let (topology, mut out) = create_topology(ReceiverStream::new(rx), config).await;

            tx.send(trace.into()).await.unwrap();

            let result = out.recv().await;

            drop(tx);
            topology.stop().await;
            assert_eq!(out.recv().await, None);

            // Validate output
            let output_event = result.expect("transform should produce an event");
            let output_trace = output_event.into_trace();

            let spans = output_trace
                .get(event_path!("spans"))
                .expect("output should have 'spans' field");

            let spans_array = match spans {
                Value::Array(arr) => arr,
                other => panic!("expected Value::Array for spans, got {other:?}"),
            };

            assert_eq!(spans_array.len(), 1, "spans array should have one element");

            let span = match &spans_array[0] {
                Value::Object(map) => map,
                other => panic!("expected Value::Object for span, got {other:?}"),
            };

            // trace_id: last 16 hex chars of "5b8efff798038103d269b633813fc60c"
            // = "d269b633813fc60c" = 0xd269b633813fc60c as u64 cast to i64
            let expected_trace_id = u64::from_str_radix("d269b633813fc60c", 16).unwrap() as i64;
            assert_eq!(
                span.get("trace_id"),
                Some(&Value::Integer(expected_trace_id)),
                "trace_id mismatch"
            );

            let expected_span_id = u64::from_str_radix("eee19b7ec3c1b174", 16).unwrap() as i64;
            assert_eq!(
                span.get("span_id"),
                Some(&Value::Integer(expected_span_id)),
                "span_id mismatch"
            );

            // root span → parent_id == 0
            assert_eq!(
                span.get("parent_id"),
                Some(&Value::Integer(0)),
                "parent_id should be 0 for root span"
            );

            // 100µs = 100_000 ns
            assert_eq!(
                span.get("duration"),
                Some(&Value::Integer(100_000)),
                "duration mismatch"
            );

            // status code 2 → error = 1
            assert_eq!(
                span.get("error"),
                Some(&Value::Integer(1)),
                "error mismatch"
            );

            assert_eq!(
                span.get("service"),
                Some(&Value::Bytes(Bytes::from("my-service"))),
                "service mismatch"
            );

            // span.kind present (kind not set → "unspecified")
            assert_eq!(
                span.get("meta")
                    .and_then(|v| v.as_object())
                    .and_then(|m| m.get("span.kind")),
                Some(&Value::Bytes(Bytes::from_static(b"unspecified"))),
                "span.kind mismatch"
            );

            // root span → _top_level = 1.0
            assert_eq!(
                span.get("metrics")
                    .and_then(|v| v.as_object())
                    .and_then(|m| m.get("_top_level")),
                Some(&Value::from(1.0f64)),
                "_top_level mismatch"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn transform_otel_span_rich() {
        assert_transform_compliance(async move {
            let mut trace = TraceEvent::default();

            trace.insert(
                event_path!("trace_id"),
                Value::Bytes(Bytes::from_static(b"5b8efff798038103d269b633813fc60c")),
            );
            trace.insert(
                event_path!("span_id"),
                Value::Bytes(Bytes::from_static(b"eee19b7ec3c1b174")),
            );
            // empty parent → root span
            trace.insert(
                event_path!("parent_span_id"),
                Value::Bytes(Bytes::from_static(b"")),
            );
            trace.insert(
                event_path!("start_time_unix_nano"),
                Value::Timestamp(chrono::Utc.timestamp_nanos(1_000_000_000i64)),
            );
            trace.insert(
                event_path!("end_time_unix_nano"),
                Value::Timestamp(chrono::Utc.timestamp_nanos(1_000_100_000i64)),
            );

            // status code 2 (ERROR) + message
            let mut status_map = ObjectMap::new();
            status_map.insert("code".into(), Value::Integer(2));
            status_map.insert(
                "message".into(),
                Value::Bytes(Bytes::from_static(b"something failed")),
            );
            trace.insert(event_path!("status"), Value::Object(status_map));

            trace.insert(
                event_path!("name"),
                Value::Bytes(Bytes::from_static(b"my-span")),
            );

            // kind = 2 (SERVER)
            trace.insert(event_path!("kind"), Value::Integer(2));

            // span attributes: HTTP server span + numeric attr
            let mut attrs = ObjectMap::new();
            attrs.insert(
                "service.name".into(),
                Value::Bytes(Bytes::from_static(b"my-service")),
            );
            attrs.insert(
                "http.method".into(),
                Value::Bytes(Bytes::from_static(b"GET")),
            );
            attrs.insert(
                "http.route".into(),
                Value::Bytes(Bytes::from_static(b"/api/v1/foo")),
            );
            attrs.insert("http.status_code".into(), Value::Integer(200));
            attrs.insert("response_time_ms".into(), Value::Integer(42));
            trace.insert(event_path!("attributes"), Value::Object(attrs));

            // resource attributes
            let mut res = ObjectMap::new();
            res.insert(
                "host.name".into(),
                Value::Bytes(Bytes::from_static(b"myhost")),
            );
            res.insert("cpu_count".into(), Value::Integer(4));
            trace.insert(event_path!("resources"), Value::Object(res));

            let config = OtelToDatadogConfig {};
            let (tx, rx) = mpsc::channel(1);
            let (topology, mut out) = create_topology(ReceiverStream::new(rx), config).await;

            tx.send(trace.into()).await.unwrap();
            let result = out.recv().await;
            drop(tx);
            topology.stop().await;
            assert_eq!(out.recv().await, None);

            let output_trace = result
                .expect("transform should produce an event")
                .into_trace();

            let span = match output_trace
                .get(event_path!("spans"))
                .expect("spans field missing")
            {
                Value::Array(arr) => match &arr[0] {
                    Value::Object(m) => m.clone(),
                    other => panic!("expected object in spans, got {other:?}"),
                },
                other => panic!("expected array for spans, got {other:?}"),
            };

            // type derived from http.method + kind==SERVER
            assert_eq!(
                span.get("type"),
                Some(&Value::Bytes(Bytes::from_static(b"web"))),
                "type mismatch"
            );

            // resource from http.method + http.route
            assert_eq!(
                span.get("resource"),
                Some(&Value::Bytes(Bytes::from_static(b"GET /api/v1/foo"))),
                "resource mismatch"
            );

            let meta = span
                .get("meta")
                .and_then(|v| v.as_object())
                .expect("meta missing");

            assert_eq!(
                meta.get("http.method"),
                Some(&Value::Bytes(Bytes::from_static(b"GET"))),
                "meta http.method"
            );
            assert_eq!(
                meta.get("http.route"),
                Some(&Value::Bytes(Bytes::from_static(b"/api/v1/foo"))),
                "meta http.route"
            );
            // host.name from resources (overridable by span attrs)
            assert_eq!(
                meta.get("host.name"),
                Some(&Value::Bytes(Bytes::from_static(b"myhost"))),
                "meta host.name from resources"
            );
            assert_eq!(
                meta.get("error.message"),
                Some(&Value::Bytes(Bytes::from_static(b"something failed"))),
                "meta error.message"
            );
            assert_eq!(
                meta.get("span.kind"),
                Some(&Value::Bytes(Bytes::from_static(b"server"))),
                "meta span.kind"
            );

            let metrics = span
                .get("metrics")
                .and_then(|v| v.as_object())
                .expect("metrics missing");

            assert_eq!(
                metrics.get("response_time_ms"),
                Some(&Value::from(42.0f64)),
                "metrics response_time_ms"
            );
            assert_eq!(
                metrics.get("http.status_code"),
                Some(&Value::from(200.0f64)),
                "metrics http.status_code"
            );
            assert_eq!(
                metrics.get("cpu_count"),
                Some(&Value::from(4.0f64)),
                "metrics cpu_count from resources"
            );
            assert_eq!(
                metrics.get("_top_level"),
                Some(&Value::from(1.0f64)),
                "metrics _top_level for root span"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn non_root_span_has_no_top_level() {
        assert_transform_compliance(async move {
            let mut trace = TraceEvent::default();
            trace.insert(
                event_path!("trace_id"),
                Value::Bytes(Bytes::from_static(b"5b8efff798038103d269b633813fc60c")),
            );
            trace.insert(
                event_path!("span_id"),
                Value::Bytes(Bytes::from_static(b"eee19b7ec3c1b174")),
            );
            // non-empty parent → not a root span
            trace.insert(
                event_path!("parent_span_id"),
                Value::Bytes(Bytes::from_static(b"aaaa000000000001")),
            );
            trace.insert(
                event_path!("start_time_unix_nano"),
                Value::Timestamp(chrono::Utc.timestamp_nanos(1_000_000_000i64)),
            );
            trace.insert(
                event_path!("end_time_unix_nano"),
                Value::Timestamp(chrono::Utc.timestamp_nanos(1_000_100_000i64)),
            );

            let config = OtelToDatadogConfig {};
            let (tx, rx) = mpsc::channel(1);
            let (topology, mut out) = create_topology(ReceiverStream::new(rx), config).await;

            tx.send(trace.into()).await.unwrap();
            let result = out.recv().await;
            drop(tx);
            topology.stop().await;
            assert_eq!(out.recv().await, None);

            let output_trace = result.expect("event expected").into_trace();
            let span = match output_trace
                .get(event_path!("spans"))
                .expect("spans missing")
            {
                Value::Array(arr) => match &arr[0] {
                    Value::Object(m) => m.clone(),
                    other => panic!("expected object, got {other:?}"),
                },
                other => panic!("expected array, got {other:?}"),
            };

            // non-root span must NOT have _top_level
            let top_level = span
                .get("metrics")
                .and_then(|v| v.as_object())
                .and_then(|m| m.get("_top_level"));
            assert!(
                top_level.is_none(),
                "_top_level should be absent for non-root spans, got {top_level:?}"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn span_events_emitted_to_named_output() {
        use crate::config::{ConfigBuilder, build_unit_tests};
        use crate::test_util::components::init_test;
        use tokio_stream::wrappers::ReceiverStream;
        use vector_lib::transform::TransformOutputsBuf;

        init_test();

        let mut trace = TraceEvent::default();
        trace.insert(
            event_path!("trace_id"),
            Value::Bytes(Bytes::from_static(b"5b8efff798038103d269b633813fc60c")),
        );
        trace.insert(
            event_path!("span_id"),
            Value::Bytes(Bytes::from_static(b"eee19b7ec3c1b174")),
        );
        trace.insert(
            event_path!("parent_span_id"),
            Value::Bytes(Bytes::from_static(b"")),
        );
        trace.insert(
            event_path!("start_time_unix_nano"),
            Value::Timestamp(chrono::Utc.timestamp_nanos(1_000_000_000i64)),
        );
        trace.insert(
            event_path!("end_time_unix_nano"),
            Value::Timestamp(chrono::Utc.timestamp_nanos(1_000_100_000i64)),
        );
        trace.insert(
            event_path!("name"),
            Value::Bytes(Bytes::from_static(b"my-span")),
        );

        // one span event
        let mut ev_attrs = ObjectMap::new();
        ev_attrs.insert(
            "db.statement".into(),
            Value::Bytes(Bytes::from_static(b"SELECT 1")),
        );
        let mut span_event = ObjectMap::new();
        span_event.insert(
            "name".into(),
            Value::Bytes(Bytes::from_static(b"query executed")),
        );
        span_event.insert(
            "time_unix_nano".into(),
            Value::Timestamp(chrono::Utc.timestamp_nanos(1_000_050_000i64)),
        );
        span_event.insert("attributes".into(), Value::Object(ev_attrs));
        trace.insert(
            event_path!("events"),
            Value::Array(vec![Value::Object(span_event)]),
        );

        let config = OtelToDatadogConfig {};
        let outputs = config.outputs(
            &TransformContext::default(),
            &[(
                OutputId::from("in"),
                crate::schema::Definition::default_legacy_namespace(),
            )],
        );

        let mut buf = TransformOutputsBuf::new_with_capacity(outputs, 10);
        let mut transform = OtelToDatadog;
        transform.transform(Event::Trace(trace), &mut buf);

        // default output: one TraceEvent
        let traces: Vec<_> = buf.drain_default().collect();
        assert_eq!(
            traces.len(),
            1,
            "expected one trace event on default output"
        );

        // span_events output: one LogEvent
        let logs: Vec<_> = buf.drain_named(SPAN_EVENTS_OUTPUT).collect();
        assert_eq!(
            logs.len(),
            1,
            "expected one log event on span_events output"
        );

        let log = logs.into_iter().next().unwrap().into_log();
        assert_eq!(
            log.get(event_path!("message")),
            Some(&Value::Bytes(Bytes::from_static(b"query executed"))),
            "message mismatch"
        );
        assert_eq!(
            log.get(event_path!("attributes", "db.statement")),
            Some(&Value::Bytes(Bytes::from_static(b"SELECT 1"))),
            "span event attribute not propagated"
        );
        // dd.trace_id = decimal of lower 64 bits
        let expected_dd_trace_id =
            (u64::from_str_radix("d269b633813fc60c", 16).unwrap()).to_string();
        assert_eq!(
            log.get(event_path!("dd", "trace_id")),
            Some(&Value::Bytes(Bytes::from(expected_dd_trace_id))),
            "dd.trace_id mismatch"
        );
        let expected_dd_span_id =
            (u64::from_str_radix("eee19b7ec3c1b174", 16).unwrap()).to_string();
        assert_eq!(
            log.get(event_path!("dd", "span_id")),
            Some(&Value::Bytes(Bytes::from(expected_dd_span_id))),
            "dd.span_id mismatch"
        );
    }
}
