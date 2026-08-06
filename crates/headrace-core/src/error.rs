use thiserror::Error;

/// Static pipeline errors - surfaced by `headrace validate` before anything runs.
#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("duplicate node id `{0}`")]
    DuplicateId(String),
    #[error("input `{0}` references no source or transform")]
    UnknownInput(String),
    #[error("output `{0}` has more than one consumer (fan-out is not yet supported)")]
    MultipleConsumers(String),
    #[error("node `{0}` is unreachable from any source (cycle or orphan)")]
    Unreachable(String),
    #[error("node `{node}` has invalid duration `{value}`: {source}")]
    BadDuration {
        node: String,
        value: String,
        #[source]
        source: humantime::DurationError,
    },
    #[error("window `{node}`: {reason}")]
    InvalidWindow { node: String, reason: String },
}
