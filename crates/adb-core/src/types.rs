use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

use arrow::datatypes::{DataType as ArrowType, TimeUnit};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{AdbError, Result};

/// The v0.1 type system. Deliberately small: every type has an unambiguous
/// Arrow representation, a JSON representation, and total ordering, which is
/// what the executor and the segment statistics both need.
///
/// `Decimal` is deliberately absent: money is stored as `Float64` with
/// `semantic_type: currency` in v0.1, and exact decimals are a follow-up
/// (see ARCHITECTURE.md).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    Bool,
    Int64,
    Float64,
    /// UTF-8 string.
    Utf8,
    /// Microseconds since the Unix epoch, UTC.
    Timestamp,
    /// Days since the Unix epoch.
    Date,
    /// Stored as text, but semantically a UUID (validated on ingest).
    Uuid,
    /// Stored as text, validated to be well-formed JSON on ingest.
    Json,
}

impl DataType {
    pub fn arrow_type(self) -> ArrowType {
        match self {
            Self::Bool => ArrowType::Boolean,
            Self::Int64 => ArrowType::Int64,
            Self::Float64 => ArrowType::Float64,
            Self::Utf8 | Self::Uuid | Self::Json => ArrowType::Utf8,
            Self::Timestamp => ArrowType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            Self::Date => ArrowType::Date32,
        }
    }

    pub fn is_numeric(self) -> bool {
        matches!(self, Self::Int64 | Self::Float64)
    }

    /// Types that can be summed / averaged.
    pub fn is_additive(self) -> bool {
        self.is_numeric()
    }

    /// Types that can be ordered and compared with `<`, `>`, min/max.
    pub fn is_ordered(self) -> bool {
        !matches!(self, Self::Json)
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::Int64 => "int64",
            Self::Float64 => "float64",
            Self::Utf8 => "utf8",
            Self::Timestamp => "timestamp",
            Self::Date => "date",
            Self::Uuid => "uuid",
            Self::Json => "json",
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A single scalar.
///
/// Serde derives the *tagged* representation on purpose: values travel through
/// bincode in the WAL, which is not self-describing, so `untagged` would be
/// undecodable. JSON conversion is explicit and type-directed instead
/// (`from_json` / `to_json`), which is also how we coerce agent-supplied
/// timestamps into microseconds exactly once, at the edge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    /// Microseconds since epoch, UTC.
    Timestamp(i64),
    /// Days since epoch.
    Date(i32),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Bool(_) => "bool",
            Self::Int(_) => "int64",
            Self::Float(_) => "float64",
            Self::Str(_) => "utf8",
            Self::Timestamp(_) => "timestamp",
            Self::Date(_) => "date",
        }
    }

    /// The logical type this value can be used as without coercion.
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Self::Null => None,
            Self::Bool(_) => Some(DataType::Bool),
            Self::Int(_) => Some(DataType::Int64),
            Self::Float(_) => Some(DataType::Float64),
            Self::Str(_) => Some(DataType::Utf8),
            Self::Timestamp(_) => Some(DataType::Timestamp),
            Self::Date(_) => Some(DataType::Date),
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int(v) => Some(*v),
            Self::Timestamp(v) => Some(*v),
            Self::Date(v) => Some(*v as i64),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Int(v) => Some(*v as f64),
            Self::Float(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Str(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(v) => Some(*v),
            _ => None,
        }
    }

    /// Coerce an agent-supplied JSON scalar into the column's declared type.
    ///
    /// Timestamps accept RFC 3339 (`2026-01-01T00:00:00Z`), `YYYY-MM-DD
    /// HH:MM:SS` (interpreted as UTC), `YYYY-MM-DD`, or an integer. Integers are
    /// disambiguated by magnitude: `|v| < 1e11` seconds, `< 1e14` milliseconds,
    /// otherwise microseconds. This resolution happens once, here, before the
    /// value reaches the WAL, so replay stays deterministic.
    pub fn from_json(json: &serde_json::Value, ty: DataType) -> Result<Self> {
        use serde_json::Value as J;
        if json.is_null() {
            return Ok(Self::Null);
        }
        let mismatch = |want: &str| {
            Err(AdbError::TypeMismatch {
                expected: want.to_string(),
                actual: json.to_string(),
            })
        };
        match ty {
            DataType::Bool => match json {
                J::Bool(b) => Ok(Self::Bool(*b)),
                _ => mismatch("bool"),
            },
            DataType::Int64 => match json {
                J::Number(n) => n
                    .as_i64()
                    .map(Self::Int)
                    .ok_or(())
                    .or_else(|_| mismatch("int64")),
                _ => mismatch("int64"),
            },
            DataType::Float64 => match json {
                J::Number(n) => n
                    .as_f64()
                    .map(Self::Float)
                    .ok_or(())
                    .or_else(|_| mismatch("float64")),
                _ => mismatch("float64"),
            },
            DataType::Utf8 => match json {
                J::String(s) => Ok(Self::Str(s.clone())),
                _ => mismatch("utf8"),
            },
            DataType::Uuid => match json {
                J::String(s) => {
                    uuid::Uuid::parse_str(s).map_err(|e| AdbError::TypeMismatch {
                        expected: "uuid".to_string(),
                        actual: format!("{s:?} ({e})"),
                    })?;
                    Ok(Self::Str(s.clone()))
                }
                _ => mismatch("uuid"),
            },
            DataType::Json => Ok(Self::Str(json.to_string())),
            DataType::Timestamp => match json {
                J::String(s) => parse_timestamp_micros(s).map(Self::Timestamp),
                J::Number(n) => {
                    let v = n.as_i64().ok_or_else(|| AdbError::TypeMismatch {
                        expected: "timestamp".to_string(),
                        actual: json.to_string(),
                    })?;
                    Ok(Self::Timestamp(normalize_epoch_to_micros(v)))
                }
                _ => mismatch("timestamp"),
            },
            DataType::Date => match json {
                J::String(s) => parse_date_days(s).map(Self::Date),
                J::Number(n) => {
                    let v = n.as_i64().ok_or_else(|| AdbError::TypeMismatch {
                        expected: "date".to_string(),
                        actual: json.to_string(),
                    })?;
                    Ok(Self::Date(v as i32))
                }
                _ => mismatch("date"),
            },
        }
    }

    /// JSON representation returned to agents. Timestamps and dates render as
    /// ISO-8601 strings so an LLM can read them without a schema lookup.
    pub fn to_json(&self) -> serde_json::Value {
        use serde_json::Value as J;
        match self {
            Self::Null => J::Null,
            Self::Bool(b) => J::Bool(*b),
            Self::Int(v) => J::Number((*v).into()),
            Self::Float(v) => serde_json::Number::from_f64(*v)
                .map(J::Number)
                .unwrap_or(J::Null),
            Self::Str(s) => J::String(s.clone()),
            Self::Timestamp(micros) => match DateTime::<Utc>::from_timestamp_micros(*micros) {
                Some(dt) => J::String(dt.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)),
                None => J::Number((*micros).into()),
            },
            Self::Date(days) => {
                match NaiveDate::from_num_days_from_ce_opt(*days + UNIX_EPOCH_CE_DAYS) {
                    Some(d) => J::String(d.to_string()),
                    None => J::Number((*days as i64).into()),
                }
            }
        }
    }
}

const UNIX_EPOCH_CE_DAYS: i32 = 719_163;

/// Parse a timestamp literal into microseconds since epoch (UTC).
pub fn parse_timestamp_micros(s: &str) -> Result<i64> {
    let err = || AdbError::TypeMismatch {
        expected: "timestamp (RFC3339, 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD')".to_string(),
        actual: format!("{s:?}"),
    };
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.timestamp_micros());
    }
    for fmt in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M",
    ] {
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
            return Ok(dt.and_utc().timestamp_micros());
        }
    }
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Ok(d
            .and_hms_opt(0, 0, 0)
            .expect("midnight is always valid")
            .and_utc()
            .timestamp_micros());
    }
    Err(err())
}

/// Parse a date literal into days since epoch.
pub fn parse_date_days(s: &str) -> Result<i32> {
    let date = NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .or_else(|_| DateTime::parse_from_rfc3339(s).map(|dt| dt.naive_utc().date()))
        .map_err(|_| AdbError::TypeMismatch {
            expected: "date ('YYYY-MM-DD')".to_string(),
            actual: format!("{s:?}"),
        })?;
    Ok(date.num_days_from_ce() - UNIX_EPOCH_CE_DAYS)
}

/// Interpret an integer epoch value as microseconds, guessing the input unit by
/// magnitude (seconds / milliseconds / microseconds).
pub fn normalize_epoch_to_micros(v: i64) -> i64 {
    const SEC_BOUND: i64 = 100_000_000_000; // ~year 5138 in seconds
    const MILLI_BOUND: i64 = 100_000_000_000_000;
    let abs = v.abs();
    if abs < SEC_BOUND {
        v.saturating_mul(1_000_000)
    } else if abs < MILLI_BOUND {
        v.saturating_mul(1_000)
    } else {
        v
    }
}

// --- Total ordering -------------------------------------------------------
//
// Segment min/max statistics, sorts and group keys all need a total order,
// including over floats and across NULLs. NULL sorts first; floats use
// `total_cmp` so NaN has a defined position instead of poisoning comparisons.

impl Value {
    fn discriminant(&self) -> u8 {
        match self {
            Self::Null => 0,
            Self::Bool(_) => 1,
            Self::Int(_) | Self::Float(_) => 2,
            Self::Str(_) => 3,
            Self::Timestamp(_) => 4,
            Self::Date(_) => 5,
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Null, Self::Null) => Ordering::Equal,
            (Self::Bool(a), Self::Bool(b)) => a.cmp(b),
            (Self::Int(a), Self::Int(b)) => a.cmp(b),
            (Self::Timestamp(a), Self::Timestamp(b)) => a.cmp(b),
            (Self::Date(a), Self::Date(b)) => a.cmp(b),
            (Self::Str(a), Self::Str(b)) => a.cmp(b),
            // Int and Float compare numerically so mixed-literal predicates work.
            (Self::Float(_) | Self::Int(_), Self::Float(_) | Self::Int(_)) => {
                let a = self.as_f64().unwrap_or(f64::NAN);
                let b = other.as_f64().unwrap_or(f64::NAN);
                a.total_cmp(&b)
            }
            _ => self.discriminant().cmp(&other.discriminant()),
        }
    }
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.discriminant().hash(state);
        match self {
            Self::Null => {}
            Self::Bool(b) => b.hash(state),
            Self::Int(v) => v.hash(state),
            Self::Timestamp(v) => v.hash(state),
            Self::Date(v) => v.hash(state),
            Self::Str(s) => s.hash(state),
            // Must agree with `Ord`: an Int and a numerically equal Float hash alike.
            Self::Float(v) => {
                if v.fract() == 0.0
                    && v.is_finite()
                    && *v >= i64::MIN as f64
                    && *v <= i64::MAX as f64
                {
                    (*v as i64).hash(state)
                } else {
                    v.to_bits().hash(state)
                }
            }
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => f.write_str("NULL"),
            Self::Bool(b) => write!(f, "{b}"),
            Self::Int(v) => write!(f, "{v}"),
            Self::Float(v) => write!(f, "{v}"),
            Self::Str(s) => write!(f, "{s:?}"),
            Self::Timestamp(_) | Self::Date(_) => write!(f, "{}", self.to_json()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn timestamp_coercion_is_normalized_to_micros() {
        let expect = 1_767_225_600_000_000i64; // 2026-01-01T00:00:00Z
        for input in [
            json!("2026-01-01T00:00:00Z"),
            json!("2026-01-01 00:00:00"),
            json!("2026-01-01"),
            json!(1_767_225_600i64),
            json!(1_767_225_600_000i64),
            json!(1_767_225_600_000_000i64),
        ] {
            let v = Value::from_json(&input, DataType::Timestamp).unwrap();
            assert_eq!(v, Value::Timestamp(expect), "input {input}");
        }
    }

    #[test]
    fn timestamp_round_trips_through_json() {
        let v = Value::from_json(&json!("2026-01-01T00:00:00Z"), DataType::Timestamp).unwrap();
        let back = Value::from_json(&v.to_json(), DataType::Timestamp).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn date_round_trips() {
        let v = Value::from_json(&json!("2026-09-09"), DataType::Date).unwrap();
        assert_eq!(v.to_json(), json!("2026-09-09"));
    }

    #[test]
    fn uuid_and_json_are_validated_and_normalized() {
        assert!(Value::from_json(&json!("not-a-uuid"), DataType::Uuid).is_err());
        let id = uuid::Uuid::new_v4().to_string();
        assert_eq!(
            Value::from_json(&json!(id), DataType::Uuid).unwrap(),
            Value::Str(id.clone())
        );
        let v = Value::from_json(&json!({"a": 1}), DataType::Json).unwrap();
        assert_eq!(v, Value::Str("{\"a\":1}".to_string()));
    }

    #[test]
    fn wrong_type_is_rejected_rather_than_coerced() {
        assert!(Value::from_json(&json!("12"), DataType::Int64).is_err());
        assert!(Value::from_json(&json!(1.5), DataType::Int64).is_err());
        assert!(Value::from_json(&json!(1), DataType::Bool).is_err());
    }

    #[test]
    fn ordering_places_null_first_and_compares_numbers_numerically() {
        let mut vs = [
            Value::Int(3),
            Value::Null,
            Value::Float(1.5),
            Value::Int(-2),
            Value::Float(f64::NAN),
        ];
        vs.sort();
        assert_eq!(vs[0], Value::Null);
        assert_eq!(vs[1], Value::Int(-2));
        assert_eq!(vs[2], Value::Float(1.5));
        assert_eq!(vs[3], Value::Int(3));
        assert!(matches!(vs[4], Value::Float(f) if f.is_nan()));
    }

    #[test]
    fn eq_and_hash_agree_for_int_float() {
        use std::collections::HashSet;
        assert_eq!(Value::Int(2), Value::Float(2.0));
        let mut set = HashSet::new();
        set.insert(Value::Int(2));
        assert!(set.contains(&Value::Float(2.0)));
    }

    #[test]
    fn wal_encoding_is_self_describing_enough_for_bincode() {
        // Regression guard: `Value` must not become `#[serde(untagged)]`, which
        // would make WAL records undecodable.
        let encoded = serde_json::to_string(&Value::Int(7)).unwrap();
        assert_eq!(encoded, "{\"Int\":7}");
    }
}
