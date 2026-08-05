//! Cumulative-to-delta normalization for OTLP sums.
//!
//! OTel counters default to *cumulative* temporality: each datapoint is the running
//! total since the series' start. Aggregating those directly is wrong - a window `sum`
//! would add running totals. [`Normalizer`] remembers the last value per series and
//! yields the per-interval increment instead, so downstream transforms see deltas.
//!
//! State grows with the number of live series; time/size-based eviction is a follow-up.

use std::collections::HashMap;

/// Per-series last-value state for cumulative-to-delta conversion.
#[derive(Default)]
pub struct Normalizer {
    last: HashMap<String, f64>,
}

impl Normalizer {
    /// The delta for one cumulative reading of `series`, or `None` for the first sample
    /// (which only establishes the baseline). A monotonic counter that drops below its
    /// last reading has reset - e.g. a process restart - so the new value is itself the
    /// delta; a non-monotonic sum's decrease is a genuine signed change.
    pub(super) fn delta(&mut self, series: String, value: f64, monotonic: bool) -> Option<f64> {
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

    #[test]
    fn first_sample_only_sets_a_baseline() {
        let mut n = Normalizer::default();
        assert_eq!(n.delta("s".into(), 100.0, true), None);
    }

    #[test]
    fn monotonic_counter_yields_increments() {
        let mut n = Normalizer::default();
        assert_eq!(n.delta("s".into(), 10.0, true), None); // baseline
        assert_eq!(n.delta("s".into(), 30.0, true), Some(20.0));
        assert_eq!(n.delta("s".into(), 60.0, true), Some(30.0));
    }

    #[test]
    fn reset_reports_the_new_value() {
        let mut n = Normalizer::default();
        n.delta("s".into(), 200.0, true); // baseline
        assert_eq!(n.delta("s".into(), 250.0, true), Some(50.0));
        // The process restarts; the counter drops below its last reading.
        assert_eq!(n.delta("s".into(), 5.0, true), Some(5.0));
        assert_eq!(n.delta("s".into(), 12.0, true), Some(7.0));
    }

    #[test]
    fn non_monotonic_decrease_is_a_real_delta() {
        let mut n = Normalizer::default();
        n.delta("s".into(), 10.0, false); // baseline
        assert_eq!(n.delta("s".into(), 7.0, false), Some(-3.0));
    }

    #[test]
    fn series_are_tracked_independently() {
        let mut n = Normalizer::default();
        assert_eq!(n.delta("a".into(), 10.0, true), None);
        assert_eq!(n.delta("b".into(), 100.0, true), None);
        assert_eq!(n.delta("a".into(), 15.0, true), Some(5.0));
        assert_eq!(n.delta("b".into(), 130.0, true), Some(30.0));
    }
}
