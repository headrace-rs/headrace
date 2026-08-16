//! Integration tests for the IR - the contract a future authoring agent generates against.

use headrace_ir::*;

const EXAMPLE: &str = include_str!("../../../examples/latency.yaml");

#[test]
fn parses_the_shipped_example() {
    let p: Pipeline = serde_norway::from_str(EXAMPLE).expect("example parses");
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
    assert_eq!(aggregate.on_missing, FaultAction::Skip); // defaulted
    assert_eq!(group_by, &["service.name", "http.route"]);
}

#[test]
fn applies_defaults_to_a_minimal_source() {
    let p: Pipeline = serde_norway::from_str(
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
    let p: Pipeline = serde_norway::from_str(
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
fn window_options_parse_and_default() {
    let p: Pipeline = serde_norway::from_str(
        r#"
        sources: [{ type: generator, id: g }]
        transforms:
          - { type: window, id: w, input: g, size: 5s, slide: 2s, allowed_lateness: 2s, idle_timeout: 30s, aggregate: { op: count } }
          - { type: window, id: w2, input: w, size: 5s, aggregate: { op: count } }
        sinks: [{ type: stdout, id: o, input: w2 }]
        "#,
    )
    .unwrap();
    let Transform::Window {
        slide,
        allowed_lateness,
        idle_timeout,
        ..
    } = &p.transforms[0]
    else {
        panic!("expected window");
    };
    assert_eq!(slide.as_deref(), Some("2s"));
    assert_eq!(allowed_lateness.as_deref(), Some("2s"));
    assert_eq!(idle_timeout.as_deref(), Some("30s"));
    // Omitting them leaves them unset: tumbling, no grace, no idle flush.
    let Transform::Window {
        slide,
        allowed_lateness,
        idle_timeout,
        ..
    } = &p.transforms[1]
    else {
        panic!("expected window");
    };
    assert_eq!(*slide, None);
    assert_eq!(*allowed_lateness, None);
    assert_eq!(*idle_timeout, None);
}

#[test]
fn parses_map_expression() {
    let p: Pipeline = serde_norway::from_str(
        r#"
        sources: [{ type: generator, id: g }]
        transforms: [{ type: map, id: m, input: g, value: "errors / total" }]
        sinks: [{ type: stdout, id: o, input: m }]
        "#,
    )
    .unwrap();
    let Transform::Map {
        value,
        on_missing,
        on_invalid,
        ..
    } = &p.transforms[0]
    else {
        panic!("expected map");
    };
    assert_eq!(value, "errors / total");
    assert_eq!(*on_missing, FaultAction::Skip); // defaulted
    assert_eq!(*on_invalid, FaultAction::Skip); // defaulted
}

#[test]
fn parses_join() {
    let p: Pipeline = serde_norway::from_str(
        r#"
        sources:
          - { type: generator, id: s1 }
          - { type: generator, id: s2 }
        transforms:
          - { type: window, id: w1, input: s1, size: 5s, aggregate: { op: avg } }
          - { type: window, id: w2, input: s2, size: 5s, aggregate: { op: avg } }
          - { type: join, id: j, inputs: [w1, w2], name: "diff", value: "w1 - w2" }
        sinks: [{ type: stdout, id: out, input: j }]
        "#,
    )
    .unwrap();
    let Transform::Join {
        inputs,
        name,
        value,
        ..
    } = &p.transforms[2]
    else {
        panic!("expected join");
    };
    assert_eq!(inputs, &["w1", "w2"]);
    assert_eq!(name.as_deref(), Some("diff"));
    assert_eq!(value.as_deref(), Some("w1 - w2"));
}

#[test]
fn window_and_join_parse_max_groups() {
    let p: Pipeline = serde_norway::from_str(
        r#"
        sources:
          - { type: generator, id: s1, interval: 1s }
          - { type: generator, id: s2, interval: 1s }
        transforms:
          - { type: window, id: w1, input: s1, size: 5s, group_by: [k],
              aggregate: { op: count }, max_groups: 1000 }
          - { type: window, id: w2, input: s2, size: 5s, group_by: [k], aggregate: { op: count } }
          - { type: join, id: j, inputs: [w1, w2], value: "w1 + w2", max_groups: 500 }
        sinks: [{ type: stdout, id: out, input: j }]
        "#,
    )
    .unwrap();
    let Transform::Window { max_groups, .. } = &p.transforms[0] else {
        panic!("expected a window");
    };
    assert_eq!(*max_groups, Some(1000));
    let Transform::Window { max_groups, .. } = &p.transforms[1] else {
        panic!("expected a window");
    };
    assert_eq!(*max_groups, None, "unset defaults to unbounded");
    let Transform::Join { max_groups, .. } = &p.transforms[2] else {
        panic!("expected a join");
    };
    assert_eq!(*max_groups, Some(500));
}

#[test]
fn pipeline_roundtrips_through_json() {
    let p: Pipeline = serde_norway::from_str(EXAMPLE).unwrap();
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
        "map",
        "join",
        "slide",
        "allowed_lateness",
        "on_missing",
        "AggregateOp",
    ] {
        assert!(schema.contains(needle), "schema missing `{needle}`");
    }
}
