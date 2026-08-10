//! Property tests for the two invariants that make windowed aggregation correct once it is
//! partitioned across workers: results don't depend on record order, and partial aggregates
//! merge into the whole. If either breaks, cross-partition rollups and changelog recovery
//! are wrong (DESIGN.md -> Stateful semantics).
//!
//! Samples are integer-valued so f64 sums are exact - otherwise float non-associativity
//! would make "order-independent" false in the last ULP for the wrong reason.

use headrace_core::record::{Attrs, Record};
use headrace_core::transform::{Window, WindowConfig};
use headrace_ir::{Aggregate, AggregateOp, FaultAction};
use proptest::prelude::*;
use std::time::Duration;

proptest! {
    /// Out-of-order arrival within a window must not change the result.
    #[test]
    fn aggregation_is_order_independent(ints in prop::collection::vec(-10_000i64..=10_000, 1..64)) {
        let fwd: Vec<f64> = ints.iter().map(|&i| i as f64).collect();
        let mut rev = fwd.clone();
        rev.reverse();
        for op in OPS {
            prop_assert_eq!(agg_over(op, &fwd), agg_over(op, &rev), "{:?} depends on order", op);
        }
    }

    /// Aggregating the whole equals merging the per-partition partials - the property that
    /// lets group state live on separate workers. Avg is excluded: it is not mergeable from
    /// averages (you'd need count-weighting), which is why the engine keeps sum+count, not avg.
    #[test]
    fn distributive_ops_are_mergeable(
        ints in prop::collection::vec(-10_000i64..=10_000, 1..64),
        k in 0usize..64,
    ) {
        let vs: Vec<f64> = ints.iter().map(|&i| i as f64).collect();
        let (l, r) = vs.split_at(k.min(vs.len()));
        for op in [AggregateOp::Count, AggregateOp::Sum, AggregateOp::Min, AggregateOp::Max] {
            let whole = agg_over(op, &vs);
            let merged = merge(op, agg_over(op, l), agg_over(op, r));
            prop_assert_eq!(whole, merged, "{:?} is not mergeable across partitions", op);
        }
    }
}

const OPS: [AggregateOp; 5] = [
    AggregateOp::Count,
    AggregateOp::Sum,
    AggregateOp::Min,
    AggregateOp::Max,
    AggregateOp::Avg,
];

fn rec(v: f64) -> Record {
    Record {
        ts_nanos: 1,
        start_ts_nanos: None,
        resource: Attrs::new(),
        scope: None,
        name: "m".into(),
        value: v,
        attrs: Attrs::new(),
    }
}

/// Single-group aggregate over `vs`. `None` when `vs` is empty (no group forms).
fn agg_over(op: AggregateOp, vs: &[f64]) -> Option<f64> {
    // All samples share ts 1, so they land in one window; drain it in full.
    let mut w = Window::from(WindowConfig::tumbling(
        Duration::from_nanos(1000),
        Aggregate {
            op,
            field: None,
            on_missing: FaultAction::Skip,
            on_invalid: FaultAction::Skip,
        },
    ));
    for &v in vs {
        w.on_record(&rec(v)).unwrap();
    }
    w.drain_all().into_iter().next().map(|r| r.value)
}

/// The monoid combine for a distributive op - how two partitions' partials become the whole.
fn merge(op: AggregateOp, a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(match op {
            AggregateOp::Count | AggregateOp::Sum => a + b,
            AggregateOp::Min => a.min(b),
            AggregateOp::Max => a.max(b),
            AggregateOp::Avg => unreachable!("avg is not mergeable from avgs alone"),
        }),
        (some, None) | (None, some) => some,
    }
}
