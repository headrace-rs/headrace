use crate::backend::Consumer;
use crate::metrics::NodeMetrics;
use crate::record::Record;
use anyhow::{Result, bail};
use headrace_ir::{Format, Sink};

pub async fn run(sink: Sink, rx: Box<dyn Consumer>, nm: NodeMetrics) -> Result<()> {
    match sink {
        Sink::Stdout { format, .. } => stdout(format, rx, nm).await,
        #[cfg(feature = "otlp")]
        Sink::Otlp { endpoint, .. } => crate::otlp::exporter::run(endpoint, rx, nm).await,
        // Forward-compat: an IR sink type this build does not implement.
        other => bail!("unsupported sink `{}`", other.id()),
    }
}

async fn stdout(format: Format, mut rx: Box<dyn Consumer>, nm: NodeMetrics) -> Result<()> {
    while let Some(rec) = rx.recv().await {
        match format {
            Format::Json => println!("{}", json(&rec)?),
            Format::Text => println!("{}", text(&rec)),
        }
        nm.out();
    }
    Ok(())
}

fn text(rec: &Record) -> String {
    let attrs: Vec<String> = rec.attrs.iter().map(|(k, v)| format!("{k}={v}")).collect();
    let window = match rec.start_ts_nanos {
        Some(start) => format!(" [{start}..{}]", rec.ts_nanos),
        None => String::new(),
    };
    format!(
        "{}{} {}={:.2} {{{}}}",
        rec.ts_nanos,
        window,
        rec.name,
        rec.value,
        attrs.join(", ")
    )
}

/// One-line JSON encoding of a record (the `json` stdout format).
fn json(rec: &Record) -> Result<String> {
    Ok(serde_json::to_string(rec)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{AttrValue, Attrs};

    #[test]
    fn text_point_sample_has_no_window() {
        assert_eq!(
            text(&rec(None)),
            "100 http.server.duration=42.00 {service.name=checkout}"
        );
    }

    #[test]
    fn text_rollup_shows_window_bounds() {
        assert_eq!(
            text(&rec(Some(50))),
            "100 [50..100] http.server.duration=42.00 {service.name=checkout}"
        );
    }

    #[test]
    fn json_roundtrips_record() {
        let s = json(&rec(Some(50))).unwrap();
        let back: Record = serde_json::from_str(&s).unwrap();
        assert_eq!(back.name, "http.server.duration");
        assert_eq!(back.value, 42.0);
        assert_eq!(back.start_ts_nanos, Some(50));
        assert_eq!(
            back.attrs.get("service.name"),
            Some(&AttrValue::Str("checkout".into()))
        );
    }

    fn rec(start: Option<u64>) -> Record {
        let mut attrs = Attrs::new();
        attrs.insert("service.name".into(), AttrValue::Str("checkout".into()));
        Record {
            ts_nanos: 100,
            start_ts_nanos: start,
            resource: Attrs::new(),
            scope: None,
            name: "http.server.duration".into(),
            value: 42.0,
            attrs,
        }
    }
}
