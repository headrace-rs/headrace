use crate::backend::Producer;
use crate::metrics::NodeMetrics;
use crate::record::{AttrValue, Attrs, Record, now_nanos};
use anyhow::{Result, bail};
use headrace_ir::Source;
use tokio::io::{AsyncBufReadExt, BufReader};

pub async fn run(src: Source, tx: Box<dyn Producer>, nm: NodeMetrics) -> Result<()> {
    match src {
        Source::Generator {
            metric,
            interval,
            services,
            routes,
            ..
        } => generator(&metric, &interval, services, routes, tx, &nm).await,
        Source::Stdin { .. } => stdin(tx, &nm).await,
        #[cfg(feature = "otlp")]
        Source::Otlp {
            listen,
            max_recv_bytes,
            max_concurrent_streams,
            ..
        } => {
            crate::otlp::receiver::run(listen, max_recv_bytes, max_concurrent_streams, tx, nm).await
        }
        // Forward-compat: an IR source type this build does not implement.
        other => bail!("unsupported source `{}`", other.id()),
    }
}

/// Synthetic source for demos and tests: emit one metric record per `interval`, cycling
/// `service.name` over `services` and `http.route` over `routes` with a varying `value`.
/// Stops when the downstream closes.
async fn generator(
    metric: &str,
    interval: &str,
    services: Vec<String>,
    routes: Vec<String>,
    tx: Box<dyn Producer>,
    nm: &NodeMetrics,
) -> Result<()> {
    let period = humantime::parse_duration(interval)?;
    let services = or_default(services, &["checkout", "cart", "search"]);
    let routes = or_default(routes, &["/", "/api/order", "/api/items"]);
    let mut ticker = tokio::time::interval(period);
    let mut i = 0usize;
    loop {
        ticker.tick().await;
        let mut attrs = Attrs::new();
        attrs.insert(
            "service.name".into(),
            AttrValue::Str(services[i % services.len()].clone()),
        );
        attrs.insert(
            "http.route".into(),
            AttrValue::Str(routes[(i / services.len()) % routes.len()].clone()),
        );
        let rec = Record {
            ts_nanos: now_nanos(),
            start_ts_nanos: None,
            resource: Attrs::new(),
            scope: None,
            name: metric.to_string(),
            value: 50.0 + ((i * 7) % 100) as f64,
            attrs,
        };
        if tx.send(None, rec).await.is_err() {
            return Ok(());
        }
        nm.out();
        i += 1;
    }
}

/// Read one JSON-encoded `Record` per line from stdin; blank lines are ignored and malformed
/// lines are logged and skipped. Stops at EOF or when the downstream closes.
async fn stdin(tx: Box<dyn Producer>, nm: &NodeMetrics) -> Result<()> {
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Record>(&line) {
            Ok(rec) => {
                if tx.send(None, rec).await.is_err() {
                    break;
                }
                nm.out();
            }
            Err(e) => tracing::warn!("skipping bad record: {e}"),
        }
    }
    Ok(())
}

fn or_default(v: Vec<String>, d: &[&str]) -> Vec<String> {
    if v.is_empty() {
        d.iter().map(|s| s.to_string()).collect()
    } else {
        v
    }
}
