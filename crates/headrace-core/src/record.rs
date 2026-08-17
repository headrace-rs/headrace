use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub type Attrs = BTreeMap<String, AttrValue>;

/// Mirrors OTel `AnyValue` (the subset we process today).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
#[non_exhaustive]
pub enum AttrValue {
    Bool(bool),
    Int(i64),
    Double(f64),
    Str(String),
}

impl std::fmt::Display for AttrValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AttrValue::Bool(b) => write!(f, "{b}"),
            AttrValue::Int(i) => write!(f, "{i}"),
            AttrValue::Double(d) => write!(f, "{d}"),
            AttrValue::Str(s) => write!(f, "{s}"),
        }
    }
}

impl AttrValue {
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            AttrValue::Int(i) => Some(*i as f64),
            AttrValue::Double(d) => Some(*d),
            _ => None,
        }
    }

    /// Append a canonical, type-tagged byte encoding of this value to `buf`, for building
    /// composite keys. Types stay distinct (`Int(1)` never encodes like `Str("1")`) and
    /// strings are length-prefixed, so concatenated fields can't run together. This is the
    /// same discipline the window group key ([`KeyPart`]) and NATS partition key use, so a
    /// key can never merge two genuinely different series.
    pub(crate) fn write_key_bytes(&self, buf: &mut Vec<u8>) {
        match self {
            AttrValue::Bool(b) => {
                buf.push(1);
                buf.push(u8::from(*b));
            }
            AttrValue::Int(i) => {
                buf.push(2);
                buf.extend_from_slice(&i.to_le_bytes());
            }
            AttrValue::Double(d) => {
                buf.push(3);
                buf.extend_from_slice(&d.to_bits().to_le_bytes());
            }
            AttrValue::Str(s) => {
                buf.push(4);
                buf.extend_from_slice(&(s.len() as u64).to_le_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
        }
    }
}

/// The unit in flight. OTel data model, flattened to what the nodes need.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    /// Sample time - for a window rollup, the window end (OTel `TimeUnixNano`).
    pub ts_nanos: u64,
    /// Window start (OTel `StartTimeUnixNano`); set by windowing, `None` for point samples.
    #[serde(default)]
    pub start_ts_nanos: Option<u64>,
    #[serde(default)]
    pub resource: Attrs,
    #[serde(default)]
    pub scope: Option<String>,
    pub name: String,
    pub value: f64,
    #[serde(default)]
    pub attrs: Attrs,
}

/// Why a field could not be read as a number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// The field is absent.
    Missing,
    /// The field is present but not numeric.
    Invalid,
}

impl Record {
    /// Attribute lookup, falling back to resource-level attributes.
    pub fn lookup(&self, key: &str) -> Option<&AttrValue> {
        self.attrs.get(key).or_else(|| self.resource.get(key))
    }

    /// Resolve a numeric field: `None` or `"value"` is the record's `value`; any other
    /// name is a numeric attribute. `Err` distinguishes an absent field ([`Fault::Missing`])
    /// from a present, non-numeric one ([`Fault::Invalid`]).
    pub fn numeric(&self, field: Option<&str>) -> Result<f64, Fault> {
        match field {
            None | Some("value") => Ok(self.value),
            Some(name) => match self.lookup(name) {
                None => Err(Fault::Missing),
                Some(v) => v.as_f64().ok_or(Fault::Invalid),
            },
        }
    }
}

pub fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_prefers_attrs_then_falls_back_to_resource() {
        let r = rec_with(
            &[("a", AttrValue::Str("attr".into()))],
            &[
                ("a", AttrValue::Str("res".into())),
                ("b", AttrValue::Int(2)),
            ],
        );
        assert_eq!(r.lookup("a"), Some(&AttrValue::Str("attr".into())));
        assert_eq!(r.lookup("b"), Some(&AttrValue::Int(2)));
        assert_eq!(r.lookup("missing"), None);
    }

    #[test]
    fn as_f64_only_for_numeric() {
        assert_eq!(AttrValue::Int(3).as_f64(), Some(3.0));
        assert_eq!(AttrValue::Double(2.5).as_f64(), Some(2.5));
        assert_eq!(AttrValue::Str("3".into()).as_f64(), None);
        assert_eq!(AttrValue::Bool(true).as_f64(), None);
    }

    #[test]
    fn attrvalue_untagged_roundtrips_and_keeps_types() {
        // Untagged serde must not conflate 1 (Int) with 1.0 (Double) or "1" (Str).
        for v in [
            AttrValue::Bool(true),
            AttrValue::Int(1),
            AttrValue::Double(1.5),
            AttrValue::Str("1".into()),
        ] {
            let json = serde_json::to_string(&v).unwrap();
            let back: AttrValue = serde_json::from_str(&json).unwrap();
            assert_eq!(v, back, "roundtrip changed {v:?} via {json}");
        }
    }

    #[test]
    fn record_start_ts_defaults_to_none() {
        let r: Record = serde_json::from_str(r#"{"ts_nanos":1,"name":"m","value":2.0}"#).unwrap();
        assert_eq!(r.start_ts_nanos, None);
        assert!(r.attrs.is_empty());
    }

    fn rec_with(attrs: &[(&str, AttrValue)], resource: &[(&str, AttrValue)]) -> Record {
        Record {
            ts_nanos: 1,
            start_ts_nanos: None,
            resource: resource
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
            scope: None,
            name: "m".into(),
            value: 1.0,
            attrs: attrs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        }
    }
}
