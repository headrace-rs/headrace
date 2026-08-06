//! Transforms: nodes that reshape or aggregate records (`Record -> Record`).
//!
//! `filter` is stateless; `window` is stateful (keyed group state). Each submodule
//! exposes a `run` that reads its input `Consumer` and writes to its output `Producer`;
//! [`run`] here dispatches by IR node type.

mod filter;
mod window;

pub use window::Window;

use crate::backend::{Consumer, Producer};
use crate::metrics::NodeMetrics;
use anyhow::{Result, bail};
use headrace_ir::Transform;

/// Run one transform node to completion.
pub async fn run(
    t: Transform,
    rx: Box<dyn Consumer>,
    tx: Box<dyn Producer>,
    nm: NodeMetrics,
) -> Result<()> {
    match t {
        Transform::Filter { key, equals, .. } => filter::run(key, equals, rx, tx, nm).await,
        Transform::Window {
            size,
            allowed_lateness,
            idle_timeout,
            group_by,
            aggregate,
            ..
        } => {
            let spec = window::Spec {
                size,
                allowed_lateness,
                idle_timeout,
                group_by,
                aggregate,
            };
            window::run(spec, rx, tx, nm).await
        }
        // Forward-compat: an IR node type this build does not implement.
        other => bail!("unsupported transform `{}`", other.id()),
    }
}
