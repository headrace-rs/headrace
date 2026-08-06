//! `map`: rewrite each record's `value` from a closed numeric expression (see [`expr`]).
//! A record whose expression references a missing/non-numeric field, or whose result is
//! non-finite, follows `on_missing`.
//!
//! [`expr`]: super::expr

use super::expr::Expr;
use crate::backend::{Consumer, Producer};
use crate::metrics::NodeMetrics;
use anyhow::{Result, bail};
use headrace_ir::OnMissing;

pub(super) async fn run(
    expr: String,
    on_missing: OnMissing,
    mut rx: Box<dyn Consumer>,
    tx: Box<dyn Producer>,
    nm: NodeMetrics,
) -> Result<()> {
    // Validated already, but parse defensively so `run` stands alone.
    let expr =
        Expr::parse(&expr).map_err(|e| anyhow::anyhow!("invalid map expression: {}", e.0))?;
    while let Some(mut rec) = rx.recv().await {
        match expr.eval(&rec) {
            Some(v) if v.is_finite() => {
                rec.value = v;
                if tx.send(None, rec).await.is_err() {
                    break;
                }
                nm.out();
            }
            _ => match on_missing {
                OnMissing::Skip => nm.dropped(1),
                OnMissing::Error => {
                    bail!("map: expression hit a missing field or non-finite result")
                }
            },
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{Backend, InProcess};
    use crate::metrics::{NodeKind, NodeMetrics};
    use crate::record::{Attrs, Record};
    use crate::{NoopMetrics, SharedMetrics};
    use std::sync::Arc;

    #[tokio::test]
    async fn rewrites_value_from_the_expression() {
        let mut out = drive("value / 1000", OnMissing::Skip, rec(2000.0)).await;
        let got = out.recv().await.expect("mapped record");
        assert_eq!(got.value, 2.0);
    }

    #[tokio::test]
    async fn skips_records_it_cannot_evaluate() {
        // `missing` is absent, so the record is dropped and nothing is forwarded.
        let mut out = drive("missing * 2", OnMissing::Skip, rec(1.0)).await;
        assert!(out.recv().await.is_none());
    }

    /// Run `expr` over a single `rec` and return the output consumer, input closed.
    async fn drive(expr: &str, on_missing: OnMissing, rec: Record) -> Box<dyn Consumer> {
        let mut be = InProcess::new(8);
        let feed = be.producer("in");
        let rx = be.consumer("in");
        let tx = be.producer("m");
        let out = be.consumer("m");
        drop(be);
        let metrics: SharedMetrics = Arc::new(NoopMetrics);
        let nm = NodeMetrics::bind(&metrics, "m", NodeKind::Map);
        tokio::spawn(run(expr.to_string(), on_missing, rx, tx, nm));
        feed.send(None, rec).await.unwrap();
        drop(feed); // close the input so the task drains and exits
        out
    }

    fn rec(value: f64) -> Record {
        Record {
            ts_nanos: 1,
            start_ts_nanos: None,
            resource: Attrs::new(),
            scope: None,
            name: "m".into(),
            value,
            attrs: Attrs::new(),
        }
    }
}
