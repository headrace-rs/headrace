//! OTLP <-> Record conversion. Metrics only: Gauge and Sum number datapoints.
//!
//! Cumulative sums are converted to per-interval deltas via a [`Normalizer`] threaded
//! through [`decode`], so downstream windows aggregate increments, not running totals.
//! Gauges and delta sums pass through unchanged.

use super::normalize::Normalizer;
use crate::record::{AttrValue, Attrs, Record};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric,
    number_data_point,
};
use std::collections::BTreeMap;

/// Decode an OTLP metrics request into records: one per Gauge/Sum datapoint.
///
/// Cumulative sums are converted to per-interval deltas through `norm`; gauges and
/// delta sums pass through unchanged.
pub fn decode(req: ExportMetricsServiceRequest, norm: &mut Normalizer) -> Vec<Record> {
    let mut out = Vec::new();
    for rm in req.resource_metrics {
        let resource = rm
            .resource
            .map(|r| attrs(&r.attributes))
            .unwrap_or_default();
        for sm in rm.scope_metrics {
            decode_scope(sm, &resource, norm, &mut out);
        }
    }
    out
}

fn decode_scope(sm: ScopeMetrics, resource: &Attrs, norm: &mut Normalizer, out: &mut Vec<Record>) {
    let scope = sm.scope.map(|s| s.name);
    for m in sm.metrics {
        decode_metric(m, resource, &scope, norm, out);
    }
}

fn decode_metric(
    m: Metric,
    resource: &Attrs,
    scope: &Option<String>,
    norm: &mut Normalizer,
    out: &mut Vec<Record>,
) {
    let name = m.name;
    // `Some(is_monotonic)` marks a cumulative sum (needs delta conversion); `None` is a
    // gauge or delta sum, whose values are aggregated as-is.
    let (points, cumulative) = match m.data {
        Some(metric::Data::Gauge(g)) => (g.data_points, None),
        Some(metric::Data::Sum(s)) => {
            let cumulative = (s.aggregation_temporality
                == AggregationTemporality::Cumulative as i32)
                .then_some(s.is_monotonic);
            (s.data_points, cumulative)
        }
        _ => return, // histograms and others are not handled yet
    };
    for p in points {
        let Some(raw) = number_value(&p) else {
            continue;
        };
        let attrs = attrs(&p.attributes);
        let value = match cumulative {
            None => raw,
            Some(monotonic) => {
                let key = series_key(resource, scope, &name, &attrs);
                match norm.delta(key, raw, monotonic) {
                    Some(delta) => delta,
                    None => continue, // first sample only sets the baseline
                }
            }
        };
        out.push(Record {
            ts_nanos: p.time_unix_nano,
            start_ts_nanos: (p.start_time_unix_nano != 0).then_some(p.start_time_unix_nano),
            resource: resource.clone(),
            scope: scope.clone(),
            name: name.clone(),
            value,
            attrs,
        });
    }
}

/// A stable identity for a metric series (resource + scope + name + datapoint attrs),
/// used to key the per-series state that cumulative-to-delta conversion needs.
fn series_key(resource: &Attrs, scope: &Option<String>, name: &str, attrs: &Attrs) -> String {
    use std::fmt::Write;
    let mut k = format!("{name}\u{1f}{}", scope.as_deref().unwrap_or(""));
    for (key, val) in resource {
        let _ = write!(k, "\u{1f}r:{key}={val}");
    }
    for (key, val) in attrs {
        let _ = write!(k, "\u{1f}a:{key}={val}");
    }
    k
}

/// Encode records into an OTLP metrics request: one Gauge metric per record name.
pub fn encode(records: Vec<Record>) -> ExportMetricsServiceRequest {
    let mut by_name: BTreeMap<String, Vec<NumberDataPoint>> = BTreeMap::new();
    for r in records {
        by_name.entry(r.name).or_default().push(NumberDataPoint {
            attributes: key_values(&r.attrs),
            start_time_unix_nano: r.start_ts_nanos.unwrap_or(0),
            time_unix_nano: r.ts_nanos,
            value: Some(number_data_point::Value::AsDouble(r.value)),
            exemplars: Vec::new(),
            flags: 0,
        });
    }
    let metrics = by_name
        .into_iter()
        .map(|(name, data_points)| Metric {
            name,
            description: String::new(),
            unit: String::new(),
            metadata: Vec::new(),
            data: Some(metric::Data::Gauge(Gauge { data_points })),
        })
        .collect();
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: "headrace".into(),
                    ..Default::default()
                }),
                metrics,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn number_value(p: &NumberDataPoint) -> Option<f64> {
    match p.value {
        Some(number_data_point::Value::AsDouble(d)) => Some(d),
        Some(number_data_point::Value::AsInt(i)) => Some(i as f64),
        None => None,
    }
}

fn attrs(kvs: &[KeyValue]) -> Attrs {
    kvs.iter()
        .filter_map(|kv| Some((kv.key.clone(), from_any(kv.value.as_ref()?)?)))
        .collect()
}

fn from_any(v: &AnyValue) -> Option<AttrValue> {
    match v.value.as_ref()? {
        any_value::Value::StringValue(s) => Some(AttrValue::Str(s.clone())),
        any_value::Value::BoolValue(b) => Some(AttrValue::Bool(*b)),
        any_value::Value::IntValue(i) => Some(AttrValue::Int(*i)),
        any_value::Value::DoubleValue(d) => Some(AttrValue::Double(*d)),
        _ => None, // arrays, kvlists, bytes are not mapped
    }
}

fn key_values(a: &Attrs) -> Vec<KeyValue> {
    a.iter()
        .map(|(k, v)| KeyValue {
            key: k.clone(),
            value: Some(to_any(v)),
            key_strindex: 0,
        })
        .collect()
}

fn to_any(v: &AttrValue) -> AnyValue {
    let value = match v {
        AttrValue::Str(s) => any_value::Value::StringValue(s.clone()),
        AttrValue::Bool(b) => any_value::Value::BoolValue(*b),
        AttrValue::Int(i) => any_value::Value::IntValue(*i),
        AttrValue::Double(d) => any_value::Value::DoubleValue(*d),
    };
    AnyValue { value: Some(value) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::metrics::v1::Sum;

    #[test]
    fn decode_encode_round_trip() {
        let mut a = Attrs::new();
        a.insert("service.name".into(), AttrValue::Str("checkout".into()));
        let rec = Record {
            ts_nanos: 100,
            start_ts_nanos: Some(40),
            resource: Attrs::new(),
            scope: None,
            name: "http.server.duration".into(),
            value: 42.0,
            attrs: a,
        };
        let back = decode(encode(vec![rec]), &mut Normalizer::default());
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].name, "http.server.duration");
        assert_eq!(back[0].value, 42.0);
        assert_eq!(back[0].ts_nanos, 100);
        assert_eq!(back[0].start_ts_nanos, Some(40));
        assert_eq!(
            back[0].attrs.get("service.name"),
            Some(&AttrValue::Str("checkout".into()))
        );
    }

    #[test]
    fn cumulative_sum_decodes_to_deltas() {
        let mut norm = Normalizer::default();
        // The first reading is only a baseline - no record emitted.
        assert!(decode(cumulative_req("req.count", 100.0), &mut norm).is_empty());
        // The next reading yields the increment.
        let out = decode(cumulative_req("req.count", 175.0), &mut norm);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].name, "req.count");
        assert_eq!(out[0].value, 75.0, "delta of 175 over baseline 100");
    }

    /// A one-datapoint request for a cumulative monotonic sum named `name`.
    fn cumulative_req(name: &str, value: f64) -> ExportMetricsServiceRequest {
        let point = NumberDataPoint {
            attributes: Vec::new(),
            start_time_unix_nano: 0,
            time_unix_nano: 1,
            value: Some(number_data_point::Value::AsDouble(value)),
            exemplars: Vec::new(),
            flags: 0,
        };
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: name.into(),
                        description: String::new(),
                        unit: String::new(),
                        metadata: Vec::new(),
                        data: Some(metric::Data::Sum(Sum {
                            data_points: vec![point],
                            aggregation_temporality: AggregationTemporality::Cumulative as i32,
                            is_monotonic: true,
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }
}
