//! Transforms: nodes that reshape or aggregate records (`Record -> Record`).
//!
//! `filter` is stateless; `window` is stateful (keyed group state). Each submodule
//! exposes a `run` that reads its input `Consumer` and writes to its output `Producer`;
//! [`run`] here dispatches by IR node type.

pub(crate) mod expr;
mod filter;
mod join;
mod map;
#[cfg(feature = "wasm")]
mod wasm;
mod window;

pub use window::{Window, WindowConfig};

use crate::backend::{Consumer, Producer};
use crate::inspect::Inspect;
use crate::metrics::NodeMetrics;
use anyhow::{Result, bail};
use headrace_ir::Transform;

/// The wasm engine, built once at pipeline setup and shared by every wasm node. Feature-agnostic
/// so the runtime threads it without conditionals; empty without the `wasm` feature. Cloning is
/// cheap (a shared `Arc`); the epoch ticker stops when the last clone drops.
#[derive(Clone, Default)]
pub struct WasmEngine {
    #[cfg(feature = "wasm")]
    inner: Option<std::sync::Arc<WasmEngineInner>>,
}

/// Holds the engine and its ticker's stop flag; dropping it (with the last [`WasmEngine`] clone)
/// stops the epoch ticker thread.
#[cfg(feature = "wasm")]
struct WasmEngineInner {
    engine: wasmtime::Engine,
    ticker_stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(feature = "wasm")]
impl Drop for WasmEngineInner {
    fn drop(&mut self) {
        self.ticker_stop
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

impl WasmEngine {
    /// Build the engine (and start its epoch ticker). Call once when a pipeline has wasm nodes.
    #[cfg(feature = "wasm")]
    pub(crate) fn new() -> Self {
        let (engine, ticker_stop) = wasm::build_engine();
        Self {
            inner: Some(std::sync::Arc::new(WasmEngineInner {
                engine,
                ticker_stop,
            })),
        }
    }

    #[cfg(feature = "wasm")]
    fn engine(&self) -> Result<&wasmtime::Engine> {
        self.inner
            .as_ref()
            .map(|i| &i.engine)
            .ok_or_else(|| anyhow::anyhow!("a wasm node needs an engine, but none was set up"))
    }
}

/// Run one transform node to completion. `rxs` holds one consumer per input - exactly one
/// for every transform except `join`, which fans in several. `inspect` is the node's
/// state-inspection channel when enabled; stateless transforms ignore it.
#[cfg_attr(not(feature = "wasm"), allow(unused_variables))]
pub async fn run(
    t: Transform,
    rxs: Vec<Box<dyn Consumer>>,
    tx: Box<dyn Producer>,
    nm: NodeMetrics,
    inspect: Option<Inspect>,
    wasm: WasmEngine,
) -> Result<()> {
    match t {
        Transform::Filter { key, equals, .. } => filter::run(key, equals, one(rxs), tx, nm).await,
        Transform::Window {
            name,
            size,
            slide,
            allowed_lateness,
            idle_timeout,
            group_by,
            aggregate,
            max_groups,
            ..
        } => {
            let spec = window::Spec {
                size,
                slide,
                allowed_lateness,
                idle_timeout,
                group_by,
                aggregate,
                name,
                max_groups,
            };
            window::run(spec, one(rxs), tx, nm, inspect).await
        }
        Transform::Map {
            name,
            value,
            on_missing,
            on_invalid,
            ..
        } => map::run(value, on_missing, on_invalid, name, one(rxs), tx, nm).await,
        Transform::Join {
            id,
            inputs,
            name,
            value,
            max_groups,
        } => {
            let spec = join::Spec {
                id,
                inputs,
                name,
                value,
                max_groups,
            };
            join::run(spec, rxs, tx, nm, inspect).await
        }
        #[cfg(feature = "wasm")]
        Transform::Wasm {
            module,
            sha256,
            max_memory,
            timeout,
            on_error,
            ..
        } => {
            let spec = wasm::Spec {
                module,
                sha256,
                on_error,
                max_memory,
                timeout,
            };
            wasm::run(spec, wasm.engine()?, one(rxs), tx, nm).await
        }
        // Forward-compat: an IR node type this build does not implement.
        other => bail!("unsupported transform `{}`", other.id()),
    }
}

/// The single consumer of a single-input transform. The IR gives every transform but
/// `join` exactly one `input`, so this cannot be empty for those nodes.
fn one(mut rxs: Vec<Box<dyn Consumer>>) -> Box<dyn Consumer> {
    rxs.pop()
        .expect("single-input transform must have exactly one input")
}

#[cfg(all(test, feature = "wasm"))]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn dropping_the_engine_stops_the_ticker() {
        let engine = WasmEngine::new();
        let stop = engine.inner.as_ref().unwrap().ticker_stop.clone();
        assert!(!stop.load(Ordering::Relaxed));
        // The last handle dropping signals the ticker thread to exit.
        drop(engine);
        assert!(stop.load(Ordering::Relaxed));
    }
}
