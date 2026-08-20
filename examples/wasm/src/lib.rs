//! The canonical `wasm` transform example: doubles every record's `value`. Built to
//! `wasm32-unknown-unknown`, this is also the committed test fixture the host loads (see
//! README.md).

use headrace_wasm_guest::{Record, transform};

/// Double the record's value, keeping everything else.
#[transform]
fn double(mut rec: Record) -> Vec<Record> {
    rec.value *= 2.0;
    vec![rec]
}
