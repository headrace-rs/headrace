//! Drives the compiled `headrace` binary end to end - the `schema` and `validate`
//! subcommands and their success/failure exits. Under `cargo llvm-cov` the spawned
//! binary's coverage is merged, so this exercises `main`'s dispatch and `load`.

use std::path::{Path, PathBuf};
use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_headrace");

fn write_tmp(name: &str, content: &str) -> PathBuf {
    let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    std::fs::write(&path, content).expect("write temp pipeline");
    path
}

#[test]
fn schema_prints_the_ir_json_schema() {
    let out = Command::new(BIN)
        .arg("schema")
        .output()
        .expect("run schema");
    assert!(out.status.success(), "schema should exit 0");
    let stdout = String::from_utf8(out.stdout).expect("utf8 schema");
    assert!(
        stdout.contains("sources") && stdout.contains("transforms"),
        "schema JSON should describe the pipeline catalog"
    );
}

#[test]
fn validate_accepts_a_good_pipeline() {
    let file = write_tmp("cli_good.yaml", GOOD);
    let out = Command::new(BIN)
        .args(["validate", file.to_str().unwrap()])
        .output()
        .expect("run validate");
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "ok");
}

#[test]
fn validate_rejects_a_dangling_input() {
    let file = write_tmp("cli_bad.yaml", DANGLING);
    let out = Command::new(BIN)
        .args(["validate", file.to_str().unwrap()])
        .output()
        .expect("run validate");
    assert!(
        !out.status.success(),
        "a dangling input must fail validation"
    );
}

#[test]
fn validate_reports_a_missing_file() {
    let out = Command::new(BIN)
        .args(["validate", "/no/such/headrace/pipeline.yaml"])
        .output()
        .expect("run validate");
    assert!(!out.status.success(), "a missing file must be an error");
}

#[test]
fn run_nats_without_url_is_rejected() {
    // The backend selection is validated before anything connects, so this needs no server.
    let file = write_tmp("cli_nats.yaml", GOOD);
    let out = Command::new(BIN)
        .args(["run", file.to_str().unwrap(), "--backend", "nats"])
        .output()
        .expect("run with nats backend");
    assert!(!out.status.success(), "--backend nats needs --nats-url");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("--nats-url"),
        "the error should name the missing flag"
    );
}

const GOOD: &str = "\
sources: [{ type: generator, id: gen, interval: 200ms }]
transforms: [{ type: window, id: w, input: gen, size: 5s, aggregate: { op: count } }]
sinks: [{ type: stdout, id: out, input: w }]
";

const DANGLING: &str = "\
sources: [{ type: generator, id: gen, interval: 200ms }]
sinks: [{ type: stdout, id: out, input: nope }]
";
