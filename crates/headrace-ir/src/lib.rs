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
        /// Reject any single OTLP request whose encoded size exceeds this many bytes,
        /// bounding the memory one request can force the receiver to buffer and decode.
        /// Defaults to 4 MiB.
        #[serde(default = "d_otlp_max_recv_bytes")]
        max_recv_bytes: usize,
        /// Cap concurrent HTTP/2 streams per connection, bounding the fan-in a single
        /// client can open. Defaults to 256.
        #[serde(default = "d_otlp_max_concurrent_streams")]
        max_concurrent_streams: u32,
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
        /// Rename the emitted metric; defaults to keeping the input's name.
        #[serde(default)]
        name: Option<String>,
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
        /// Cap on distinct groups held per open window. Records that would form a new group
        /// past the cap are dropped and metered (`headrace.records.capped`). Unset is unbounded.
        #[serde(default)]
        max_groups: Option<usize>,
    },
    /// Rewrite each record's `value` from a closed numeric expression over `value` and
    /// numeric attributes, e.g. `errors / total` or `value / 1000`.
    Map {
        id: String,
        input: String,
        /// Rename the emitted metric; defaults to keeping the input's name.
        #[serde(default)]
        name: Option<String>,
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
    /// Combine several inputs, aligned on their shared `group_by` and window, into one
    /// record that carries each input's value as an attribute named by input id. An
    /// optional `value` expression reduces them (`a - b`); otherwise a downstream `map`
    /// or `wasm` does. Inputs must be windowed at the same size (ADR-0012).
    Join {
        id: String,
        /// Upstream node ids to align; they must share a `group_by` and window size.
        inputs: Vec<String>,
        /// Rename the emitted metric.
        #[serde(default)]
        name: Option<String>,
        /// Reduce the aligned inputs to the output value (an expression over the input
        /// ids). Omit to carry each input's value as an attribute for a downstream node.
        #[serde(default)]
        value: Option<String>,
        /// Cap on distinct aligned groups (open buckets). New groups past the cap are dropped
        /// and metered (`headrace.records.capped`). Unset is unbounded.
        #[serde(default)]
        max_groups: Option<usize>,
    },
    /// Run a WebAssembly module as a stateless transform (ADR-0018): one record in, zero or
    /// more out. The module owns its output (including record names), so there is no `name`
    /// override here. The ABI and authoring SDK are in the wasm docs.
    Wasm {
        id: String,
        input: String,
        /// Path to the `.wasm` module on disk.
        module: String,
        /// Optional SHA-256 (hex) of the module, verified when it is loaded.
        #[serde(default)]
        sha256: Option<String>,
        /// Cap on the module's linear memory, e.g. `64Mi`, `256Mi`, `1Gi` (binary units) or a
        /// plain byte count. Defaults to 64Mi.
        #[serde(default)]
        max_memory: Option<String>,
        /// How long the module may run on a single record before that call is stopped (it traps,
        /// then `on_error` applies), e.g. `100ms`, `1s`. Applied per record, not overall.
        /// Defaults to 100ms.
        #[serde(default)]
        timeout: Option<String>,
        /// What to do when the module traps, exceeds a resource limit, or returns output that
        /// does not decode: `skip` drops the record (metered), `error` fails the node.
        #[serde(default)]
        on_error: FaultAction,
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
            | Transform::Map { id, .. }
            | Transform::Join { id, .. }
            | Transform::Wasm { id, .. } => id,
        }
    }
    /// The upstream node ids this transform reads. One for every transform except
    /// `join`, which fans in several.
    pub fn inputs(&self) -> Vec<&str> {
        match self {
            Transform::Filter { input, .. }
            | Transform::Window { input, .. }
            | Transform::Map { input, .. }
            | Transform::Wasm { input, .. } => vec![input],
            Transform::Join { inputs, .. } => inputs.iter().map(String::as_str).collect(),
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
fn d_otlp_max_recv_bytes() -> usize {
    4 * 1024 * 1024
}
fn d_otlp_max_concurrent_streams() -> u32 {
    256
}
