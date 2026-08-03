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
    // Otlp { id, listen } - next.
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
        #[serde(default)]
        kind: WindowKind,
        size: String,
        #[serde(default)]
        group_by: Vec<String>,
        aggregate: Aggregate,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum WindowKind {
    #[default]
    Tumbling,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Aggregate {
    pub op: AggregateOp,
    /// Numeric attribute to reduce; defaults to the record's value.
    #[serde(default)]
    pub field: Option<String>,
    /// What to do when `field` is absent or non-numeric on a record.
    #[serde(default)]
    pub on_missing: OnMissing,
}

/// Policy for records whose aggregate `field` is absent or non-numeric.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnMissing {
    /// Drop the record from the aggregate; the window warns with a per-flush count.
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
    // Otlp { id, input, endpoint } - next.
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
            Source::Generator { id, .. } | Source::Stdin { id } => id,
        }
    }
}

impl Transform {
    pub fn id(&self) -> &str {
        match self {
            Transform::Filter { id, .. } | Transform::Window { id, .. } => id,
        }
    }
    pub fn input(&self) -> &str {
        match self {
            Transform::Filter { input, .. } | Transform::Window { input, .. } => input,
        }
    }
}

impl Sink {
    pub fn id(&self) -> &str {
        match self {
            Sink::Stdout { id, .. } => id,
        }
    }
    pub fn input(&self) -> &str {
        match self {
            Sink::Stdout { input, .. } => input,
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
