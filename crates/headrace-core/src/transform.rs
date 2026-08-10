//! Transforms: nodes that reshape or aggregate records (`Record -> Record`).
//!
//! `filter` is stateless; `window` is stateful (keyed group state). Each submodule
//! exposes a `run` that reads its input `Consumer` and writes to its output `Producer`;
//! [`run`] here dispatches by IR node type.

pub(crate) mod expr;
mod filter;
mod join;
mod map;
mod window;

pub use window::{Window, WindowConfig};

use crate::backend::{Consumer, Producer};
use crate::inspect::Inspect;
use crate::metrics::NodeMetrics;
use anyhow::{Result, bail};
use headrace_ir::Transform;

/// Run one transform node to completion. `rxs` holds one consumer per input - exactly one
/// for every transform except `join`, which fans in several. `inspect` is the node's
/// state-inspection channel when enabled; stateless transforms ignore it.
pub async fn run(
    t: Transform,
    rxs: Vec<Box<dyn Consumer>>,
    tx: Box<dyn Producer>,
    nm: NodeMetrics,
    inspect: Option<Inspect>,
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
        } => {
            let spec = join::Spec {
                id,
                inputs,
                name,
                value,
            };
            join::run(spec, rxs, tx, nm, inspect).await
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
