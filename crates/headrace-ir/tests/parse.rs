//! Integration tests for the IR - the contract a future authoring agent generates against.

use headrace_ir::*;

const EXAMPLE: &str = include_str!("../../../examples/latency.yaml");

#[test]
fn parses_the_shipped_example() {
    let p: Pipeline = serde_yaml::from_str(EXAMPLE).expect("example parses");
    assert_eq!(p.sources.len(), 1);
    assert_eq!(p.transforms.len(), 2);
    assert_eq!(p.sinks.len(), 1);

    assert!(
        matches!(&p.transforms[0], Transform::Filter { key, equals, .. }
        if key == "service.name" && equals.as_deref() == Some("checkout"))
    );

    let Transform::Window {
        aggregate,
        group_by,
        ..
    } = &p.transforms[1]
    else {
        panic!("second transform should be a window");
    };
    assert_eq!(aggregate.op, AggregateOp::Avg);
    assert_eq!(aggregate.field.as_deref(), Some("value"));
    assert_eq!(aggregate.on_missing, OnMissing::Skip); // defaulted
    assert_eq!(group_by, &["service.name", "http.route"]);
}

#[test]
fn applies_defaults_to_a_minimal_source() {
    let p: Pipeline = serde_yaml::from_str(
        "sources: [{ type: generator, id: g }]\nsinks: [{ type: stdout, id: o, input: g }]",
    )
    .unwrap();
    let Source::Generator {
        metric, interval, ..
    } = &p.sources[0]
    else {
        panic!("expected generator");
    };
    assert_eq!(metric, "demo.metric");
    assert_eq!(interval, "500ms");
}

#[test]
fn parses_otlp_source_and_sink() {
    let p: Pipeline = serde_yaml::from_str(
        r#"
        sources: [{ type: otlp, id: in }]
        sinks: [{ type: otlp, id: out, input: in, endpoint: "http://collector:4317" }]
        "#,
    )
    .unwrap();
    let Source::Otlp { listen, .. } = &p.sources[0] else {
        panic!("expected otlp source");
    };
    assert_eq!(listen, "0.0.0.0:4317"); // defaulted
    let Sink::Otlp {
        input, endpoint, ..
    } = &p.sinks[0]
    else {
        panic!("expected otlp sink");
    };
    assert_eq!(input, "in");
    assert_eq!(endpoint, "http://collector:4317");
}

#[test]
fn pipeline_roundtrips_through_json() {
    let p: Pipeline = serde_yaml::from_str(EXAMPLE).unwrap();
    let json = serde_json::to_string(&p).unwrap();
    let back: Pipeline = serde_json::from_str(&json).unwrap();
    assert_eq!(p, back);
}

#[test]
fn schema_advertises_the_node_catalog() {
    // The JSON Schema is the agent-facing surface; guard the key names it must expose.
    let schema = json_schema();
    for needle in [
        "Pipeline",
        "generator",
        "otlp",
        "window",
        "on_missing",
        "AggregateOp",
    ] {
        assert!(schema.contains(needle), "schema missing `{needle}`");
    }
}
