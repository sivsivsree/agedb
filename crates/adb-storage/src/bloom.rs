//! Per-segment bloom filters for equality pruning (see "Segments and pruning" in ARCHITECTURE.md).
//!
//! Kept in the manifest rather than the Parquet footer on purpose: the point of
//! a filter is to skip the segment *without opening the file*.

use adb_core::{AdbError, Result, Value};
use serde::{Deserialize, Serialize};

use crate::b64;
use crate::hash::hash_value;

/// Split-block-free classic bloom filter with `k` FNV probes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BloomFilter {
    bits: Vec<u64>,
    k: u32,
}

impl BloomFilter {
    /// Size a filter for `expected` distinct values at roughly `fp_rate`.
    pub fn new(expected: usize, fp_rate: f64) -> Self {
        let expected = expected.max(1) as f64;
        let fp_rate = fp_rate.clamp(1e-6, 0.5);
        let m_bits = (-(expected * fp_rate.ln()) / (std::f64::consts::LN_2.powi(2))).ceil();
        let m_bits = (m_bits as usize).clamp(64, 1 << 24);
        let words = m_bits.div_ceil(64);
        let k = ((m_bits as f64 / expected) * std::f64::consts::LN_2)
            .round()
            .clamp(1.0, 16.0) as u32;
        Self {
            bits: vec![0; words],
            k,
        }
    }

    fn probes(&self, value: &Value) -> impl Iterator<Item = usize> + '_ {
        let h1 = hash_value(value, 0x9E37_79B9_7F4A_7C15);
        let h2 = hash_value(value, 0xC2B2_AE3D_27D4_EB4F) | 1;
        let m = (self.bits.len() * 64) as u64;
        (0..self.k).map(move |i| (h1.wrapping_add(h2.wrapping_mul(i as u64)) % m) as usize)
    }

    pub fn insert(&mut self, value: &Value) {
        if value.is_null() {
            return; // NULLs are tracked by null_count, not the filter
        }
        for bit in self.probes(value).collect::<Vec<_>>() {
            self.bits[bit / 64] |= 1 << (bit % 64);
        }
    }

    /// `false` means the value is definitely absent; `true` means "maybe".
    pub fn contains(&self, value: &Value) -> bool {
        if value.is_null() {
            return true;
        }
        self.probes(value)
            .all(|bit| self.bits[bit / 64] & (1 << (bit % 64)) != 0)
    }

    pub fn size_bytes(&self) -> usize {
        self.bits.len() * 8
    }
}

/// JSON-friendly wire form: `{"k": 6, "bits": "<base64 of little-endian u64s>"}`.
#[derive(Serialize, Deserialize)]
struct BloomWire {
    k: u32,
    bits: String,
}

impl Serialize for BloomFilter {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        let mut raw = Vec::with_capacity(self.bits.len() * 8);
        for word in &self.bits {
            raw.extend_from_slice(&word.to_le_bytes());
        }
        BloomWire {
            k: self.k,
            bits: b64::encode(&raw),
        }
        .serialize(s)
    }
}

impl<'de> Deserialize<'de> for BloomFilter {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let wire = BloomWire::deserialize(d)?;
        let raw = b64::decode(&wire.bits).map_err(serde::de::Error::custom)?;
        if raw.len() % 8 != 0 || raw.is_empty() {
            return Err(serde::de::Error::custom(AdbError::Corruption(
                "bloom filter bit length is not a multiple of 8 bytes".into(),
            )));
        }
        if wire.k == 0 || wire.k > 16 {
            return Err(serde::de::Error::custom(AdbError::Corruption(format!(
                "bloom filter probe count {} out of range",
                wire.k
            ))));
        }
        let bits = raw
            .chunks_exact(8)
            .map(|c| u64::from_le_bytes(c.try_into().expect("chunk is 8 bytes")))
            .collect();
        Ok(Self { bits, k: wire.k })
    }
}

/// Build a filter over an iterator of values, or `None` if there is nothing
/// worth filtering.
pub fn build(values: impl ExactSizeIterator<Item = Value>) -> Option<BloomFilter> {
    let len = values.len();
    if len == 0 {
        return None;
    }
    let mut filter = BloomFilter::new(len, 0.01);
    for v in values {
        filter.insert(&v);
    }
    Some(filter)
}

/// Bloom filters are only sound for types whose canonical encoding is stable and
/// agrees with equality. Floats are excluded: a non-integral float literal can be
/// written many ways and we would rather scan than answer wrongly.
pub fn is_filterable(ty: adb_core::DataType) -> bool {
    use adb_core::DataType as T;
    matches!(
        ty,
        T::Int64 | T::Utf8 | T::Uuid | T::Timestamp | T::Date | T::Bool
    )
}

/// Serialize/deserialize check used by the manifest tests.
pub fn round_trip(filter: &BloomFilter) -> Result<BloomFilter> {
    let json = serde_json::to_string(filter).map_err(AdbError::from)?;
    serde_json::from_str(&json).map_err(AdbError::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives() {
        let values: Vec<Value> = (0..2_000).map(Value::Int).collect();
        let filter = build(values.clone().into_iter()).unwrap();
        for v in &values {
            assert!(filter.contains(v), "{v} must be reported present");
        }
    }

    #[test]
    fn false_positive_rate_is_near_target() {
        let values: Vec<Value> = (0..5_000).map(Value::Int).collect();
        let filter = build(values.into_iter()).unwrap();
        let misses = (1_000_000..1_010_000)
            .filter(|i| filter.contains(&Value::Int(*i)))
            .count();
        assert!(misses < 400, "false positive rate too high: {misses}/10000");
    }

    #[test]
    fn survives_serialization() {
        let values: Vec<Value> = ["a", "b", "c"]
            .iter()
            .map(|s| Value::Str(s.to_string()))
            .collect();
        let filter = build(values.clone().into_iter()).unwrap();
        let decoded = round_trip(&filter).unwrap();
        assert_eq!(filter, decoded);
        for v in &values {
            assert!(decoded.contains(v));
        }
        assert!(!decoded.contains(&Value::Str("zzz".into())));
    }

    #[test]
    fn int_and_float_literals_agree() {
        let filter = build(vec![Value::Float(2.0)].into_iter()).unwrap();
        assert!(filter.contains(&Value::Int(2)));
    }

    #[test]
    fn floats_are_not_filterable() {
        assert!(!is_filterable(adb_core::DataType::Float64));
        assert!(!is_filterable(adb_core::DataType::Json));
        assert!(is_filterable(adb_core::DataType::Utf8));
    }

    #[test]
    fn rejects_corrupt_encodings() {
        let bad = "{\"k\":0,\"bits\":\"AAAAAAAAAAA=\"}";
        assert!(serde_json::from_str::<BloomFilter>(bad).is_err());
        let bad = "{\"k\":4,\"bits\":\"AAA=\"}";
        assert!(serde_json::from_str::<BloomFilter>(bad).is_err());
    }
}
