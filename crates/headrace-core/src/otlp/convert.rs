//! OTLP <-> Record conversion. Metrics only: Gauge and Sum number datapoints.
//!
//! Sum values are decoded as-is; cumulative-to-delta normalization (needed to aggregate
//! cumulative counters correctly) is a follow-up and lives outside this module.

use crate::record::{AttrValue, Attrs, Record};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
};
use std::collections::BTreeMap;

/// Decode an OTLP metrics request into records: one per Gauge/Sum datapoint.
pub fn decode(req: ExportMetricsServiceRequest) -> Vec<Record> {
    let mut out = Vec::new();
    for rm in req.resource_metrics {
        let resource = rm
            .resource
            .map(|r| attrs(&r.attributes))
            .unwrap_or_default();
        for sm in rm.scope_metrics {
            let scope = sm.scope.map(|s| s.name);
            for m in sm.metrics {
                let points = match m.data {
                    Some(metric::Data::Gauge(g)) => g.data_points,
                    Some(metric::Data::Sum(s)) => s.data_points,
                    _ => continue, // histograms and others are not handled yet
                };
                for p in points {
                    let Some(value) = number_value(&p) else {
                        continue;
                    };
                    out.push(Record {
                        ts_nanos: p.time_unix_nano,
                        start_ts_nanos: (p.start_time_unix_nano != 0)
                            .then_some(p.start_time_unix_nano),
                        resource: resource.clone(),
                        scope: scope.clone(),
                        name: m.name.clone(),
                        value,
                        attrs: attrs(&p.attributes),
                    });
                }
            }
        }
    }
    out
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
        let back = decode(encode(vec![rec]));
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
}
