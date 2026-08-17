//! Cumulative-to-delta normalization for OTLP sums.
//!
//! OTel counters default to *cumulative* temporality: each datapoint is the running
//! total since the series' start. Aggregating those directly is wrong - a window `sum`
//! would add running totals. [`Normalizer`] remembers the last value per series and
//! yields the per-interval increment instead, so downstream transforms see deltas.
//!
//! State grows with the number of live series; time/size-based eviction is a follow-up.

use std::collections::HashMap;

/// Per-series last-value state for cumulative-to-delta conversion. Keyed by the typed
/// series identity from [`series_key`](super::convert), not a formatted string.
#[derive(Default)]
pub struct Normalizer {
    last: HashMap<Vec<u8>, f64>,
}

impl Normalizer {
    /// The delta for one cumulative reading of `series`, or `None` for the first sample
    /// (which only establishes the baseline). A monotonic counter that drops below its
    /// last reading has reset - e.g. a process restart - so the new value is itself the
    /// delta; a non-monotonic sum's decrease is a genuine signed change.
    pub(super) fn delta(&mut self, series: Vec<u8>, value: f64, monotonic: bool) -> Option<f64> {
        match self.last.insert(series, value) {
            None => None,
            Some(prev) if monotonic && value < prev => Some(value),
            Some(prev) => Some(value - prev),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A series identity for tests; the bytes are opaque, only their equality matters.
    fn s(name: &str) -> Vec<u8> {
        name.as_bytes().to_vec()
    }

    #[test]
    fn first_sample_only_sets_a_baseline() {
        let mut n = Normalizer::default();
        assert_eq!(n.delta(s("s"), 100.0, true), None);
    }

    #[test]
    fn monotonic_counter_yields_increments() {
        let mut n = Normalizer::default();
        assert_eq!(n.delta(s("s"), 10.0, true), None); // baseline
        assert_eq!(n.delta(s("s"), 30.0, true), Some(20.0));
        assert_eq!(n.delta(s("s"), 60.0, true), Some(30.0));
    }

    #[test]
    fn reset_reports_the_new_value() {
        let mut n = Normalizer::default();
        n.delta(s("s"), 200.0, true); // baseline
        assert_eq!(n.delta(s("s"), 250.0, true), Some(50.0));
        // The process restarts; the counter drops below its last reading.
        assert_eq!(n.delta(s("s"), 5.0, true), Some(5.0));
        assert_eq!(n.delta(s("s"), 12.0, true), Some(7.0));
    }

    #[test]
    fn non_monotonic_decrease_is_a_real_delta() {
        let mut n = Normalizer::default();
        n.delta(s("s"), 10.0, false); // baseline
        assert_eq!(n.delta(s("s"), 7.0, false), Some(-3.0));
    }

    #[test]
    fn series_are_tracked_independently() {
        let mut n = Normalizer::default();
        assert_eq!(n.delta(s("a"), 10.0, true), None);
        assert_eq!(n.delta(s("b"), 100.0, true), None);
        assert_eq!(n.delta(s("a"), 15.0, true), Some(5.0));
        assert_eq!(n.delta(s("b"), 130.0, true), Some(30.0));
    }
}
