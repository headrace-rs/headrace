//! The record model lives in the [`headrace_record`] crate so the engine and the wasm guest SDK
//! share one definition. This module re-exports it and adds the host-only wall-clock helper.

pub use headrace_record::{AttrValue, Attrs, Fault, Record};

use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}
