//! Pipeline IR: the declarative graph a human (or, later, an agent) targets.
//! This is *not* the data model - records in flight are OTel-shaped (see headrace-core::Record).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Pipeline {
    #[serde(default)]
    pub version: u32,
    pub sources: Vec<Source>,
    #[serde(default)]
    pub transforms: Vec<Transform>,
    pub sinks: Vec<Sink>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum Source {
    /// Synthetic metrics, for demos and tests.
    Generator {
        id: String,
        #[serde(default = "d_metric")]
        metric: String,
        #[serde(default = "d_interval")]
        interval: String,
        #[serde(default)]
        services: Vec<String>,
        #[serde(default)]
        routes: Vec<String>,
    },
    /// One JSON-encoded Record per line on stdin.
    Stdin { id: String },
    /// OTLP/gRPC receiver; `listen` is the bind address (default `0.0.0.0:4317`).
    Otlp {
        id: String,
        #[serde(default = "d_otlp_listen")]
        listen: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum Transform {
    /// Keep records where `key` exists (and equals `equals`, if set).
    Filter {
        id: String,
        input: String,
        key: String,
        #[serde(default)]
        equals: Option<String>,
    },
    /// Group records and reduce each group over a time window.
    Window {
        id: String,
        input: String,
        /// Window length.
        size: String,
        /// Step between window starts. Omitted (or equal to `size`) makes the windows
        /// tumbling (non-overlapping); a value smaller than `size` makes them sliding, so
        /// a record can fall in several overlapping windows.
        #[serde(default)]
        slide: Option<String>,
        /// Grace period, in event time, to keep waiting for late records past a
        /// window's end before firing it. Defaults to none (fire as soon as the
        /// watermark reaches the window end).
        #[serde(default)]
        allowed_lateness: Option<String>,
        /// Force-close open windows after this much wall-clock time with no records
        /// arriving. Defaults to off, so windows close only on the event-time
        /// watermark - set this for streams that go quiet but must still emit.
        #[serde(default)]
        idle_timeout: Option<String>,
        #[serde(default)]
        group_by: Vec<String>,
        aggregate: Aggregate,
    },
    /// Rewrite each record's `value` from a closed numeric expression over `value` and
    /// numeric attributes, e.g. `errors / total` or `value / 1000`.
    Map {
        id: String,
        input: String,
        /// The expression, evaluated per record and assigned to `value`.
        value: String,
        /// What to do when the expression references an absent field.
        #[serde(default)]
        on_missing: FaultAction,
        /// What to do when a referenced field is present but non-numeric, or the result
        /// is non-finite (e.g. divide by zero).
        #[serde(default)]
        on_invalid: FaultAction,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Aggregate {
    pub op: AggregateOp,
    /// Numeric attribute to reduce; defaults to the record's value.
    #[serde(default)]
    pub field: Option<String>,
    /// What to do when `field` is absent on a record.
    #[serde(default)]
    pub on_missing: FaultAction,
    /// What to do when `field` is present but non-numeric.
    #[serde(default)]
    pub on_invalid: FaultAction,
}

/// What to do with a record when a field cannot be read as a number, used for both an
/// absent field (`on_missing`) and a present-but-non-numeric one (`on_invalid`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FaultAction {
    /// Drop the record; nodes meter it as dropped.
    #[default]
    Skip,
    /// Fail the pipeline.
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AggregateOp {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum Sink {
    Stdout {
        id: String,
        input: String,
        #[serde(default)]
        format: Format,
    },
    /// OTLP/gRPC exporter to `endpoint` (e.g. `http://collector:4317`).
    Otlp {
        id: String,
        input: String,
        endpoint: String,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    #[default]
    Text,
    Json,
}

impl Source {
    pub fn id(&self) -> &str {
        match self {
            Source::Generator { id, .. } | Source::Stdin { id } | Source::Otlp { id, .. } => id,
        }
    }
}

impl Transform {
    pub fn id(&self) -> &str {
        match self {
            Transform::Filter { id, .. }
            | Transform::Window { id, .. }
            | Transform::Map { id, .. } => id,
        }
    }
    pub fn input(&self) -> &str {
        match self {
            Transform::Filter { input, .. }
            | Transform::Window { input, .. }
            | Transform::Map { input, .. } => input,
        }
    }
}

impl Sink {
    pub fn id(&self) -> &str {
        match self {
            Sink::Stdout { id, .. } | Sink::Otlp { id, .. } => id,
        }
    }
    pub fn input(&self) -> &str {
        match self {
            Sink::Stdout { input, .. } | Sink::Otlp { input, .. } => input,
        }
    }
}

/// JSON Schema for the IR - the contract a future authoring agent generates against.
pub fn json_schema() -> String {
    let schema = schemars::schema_for!(Pipeline);
    serde_json::to_string_pretty(&schema).expect("schema serializes")
}

fn d_metric() -> String {
    "demo.metric".into()
}
fn d_interval() -> String {
    "500ms".into()
}
fn d_otlp_listen() -> String {
    "0.0.0.0:4317".into()
}
