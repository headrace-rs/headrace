//! `map`: rewrite each record's `value` from a closed numeric expression (see [`expr`]).
//! An absent field follows `on_missing`; a non-numeric field or a non-finite result
//! follows `on_invalid`.
//!
//! [`expr`]: super::expr

use super::expr::Expr;
use crate::backend::{Consumer, Producer};
use crate::metrics::NodeMetrics;
use crate::record::Fault;
use anyhow::{Result, bail};
use headrace_ir::FaultAction;

pub(super) async fn run(
    expr: String,
    on_missing: FaultAction,
    on_invalid: FaultAction,
    name: Option<String>,
    mut rx: Box<dyn Consumer>,
    tx: Box<dyn Producer>,
    nm: NodeMetrics,
) -> Result<()> {
    // Validated already, but parse defensively so `run` stands alone.
    let expr =
        Expr::parse(&expr).map_err(|e| anyhow::anyhow!("invalid map expression: {}", e.0))?;
    while let Some(mut rec) = rx.recv().await {
        // An absent field is `on_missing`; a non-numeric field or a non-finite result is
        // `on_invalid`.
        let policy = match expr.eval(&rec) {
            Ok(v) if v.is_finite() => {
                rec.value = v;
                if let Some(name) = &name {
                    rec.name = name.clone();
                }
                if tx.send(None, rec).await.is_err() {
                    break;
                }
                nm.out();
                continue;
            }
            Ok(_) => on_invalid,
            Err(Fault::Missing) => on_missing,
            Err(Fault::Invalid) => on_invalid,
        };
        match policy {
            FaultAction::Skip => nm.dropped(1),
            FaultAction::Error => bail!("map: could not evaluate the expression for a record"),
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
        let mut out = drive(
            "value / 1000",
            FaultAction::Skip,
            FaultAction::Skip,
            None,
            rec(2000.0),
        )
        .await;
        let got = out.recv().await.expect("mapped record");
        assert_eq!(got.value, 2.0);
    }

    #[tokio::test]
    async fn renames_the_output_metric() {
        let mut out = drive(
            "value + 1",
            FaultAction::Skip,
            FaultAction::Skip,
            Some("derived".to_string()),
            rec(1.0),
        )
        .await;
        let got = out.recv().await.expect("mapped record");
        assert_eq!(got.value, 2.0);
        assert_eq!(got.name, "derived");
    }

    #[tokio::test]
    async fn skips_records_it_cannot_evaluate() {
        // `missing` is absent, so the record is dropped and nothing is forwarded.
        let mut out = drive(
            "missing * 2",
            FaultAction::Skip,
            FaultAction::Skip,
            None,
            rec(1.0),
        )
        .await;
        assert!(out.recv().await.is_none());
    }

    #[tokio::test]
    async fn errors_on_invalid_when_configured() {
        // 5 / 0 is non-finite -> on_invalid, here set to error.
        let mut be = InProcess::new(8);
        let feed = be.producer("in");
        let rx = be.consumer("in");
        let tx = be.producer("m");
        let _out = be.consumer("m");
        drop(be);
        let metrics: SharedMetrics = Arc::new(NoopMetrics);
        let nm = NodeMetrics::bind(&metrics, "m", NodeKind::Map);
        let task = tokio::spawn(run(
            "value / 0".to_string(),
            FaultAction::Skip,
            FaultAction::Error,
            None,
            rx,
            tx,
            nm,
        ));
        feed.send(None, rec(5.0)).await.unwrap();
        drop(feed);
        assert!(
            task.await.unwrap().is_err(),
            "non-finite under on_invalid=error must fail"
        );
    }

    /// Run `expr` over a single `rec` and return the output consumer, input closed.
    async fn drive(
        expr: &str,
        on_missing: FaultAction,
        on_invalid: FaultAction,
        name: Option<String>,
        rec: Record,
    ) -> Box<dyn Consumer> {
        let mut be = InProcess::new(8);
        let feed = be.producer("in");
        let rx = be.consumer("in");
        let tx = be.producer("m");
        let out = be.consumer("m");
        drop(be);
        let metrics: SharedMetrics = Arc::new(NoopMetrics);
        let nm = NodeMetrics::bind(&metrics, "m", NodeKind::Map);
        tokio::spawn(run(
            expr.to_string(),
            on_missing,
            on_invalid,
            name,
            rx,
            tx,
            nm,
        ));
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
