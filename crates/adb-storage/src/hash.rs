//! Stable hashing for persisted structures.
//!
//! Bloom filters live inside manifests, so their hash function must be identical
//! across processes, machines and releases. `ahash`/`DefaultHasher` explicitly do
//! not promise that (they vary by seed, version and CPU features), and a hash
//! change would turn into *false negatives*, silently wrong query results,
//! rather than a load error. So this is a fixed FNV-1a over a canonical byte
//! encoding of each value.

use adb_core::Value;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn fnv1a(seed: u64, bytes: &[u8]) -> u64 {
    let mut h = FNV_OFFSET ^ seed.wrapping_mul(FNV_PRIME);
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

/// Canonical bytes for a value.
///
/// Integers, timestamps and dates share one tag, and floats with a zero
/// fractional part canonicalize as integers. That is required for agreement with
/// [`adb_core::Value`]'s `Eq`/`Hash`, where `Int(2) == Float(2.0)`: if the two
/// hashed differently, a predicate written as `= 2` could miss rows stored as
/// `2.0` when consulting a bloom filter.
pub fn canonical_bytes(value: &Value) -> Vec<u8> {
    let mut out = Vec::with_capacity(9);
    match value {
        Value::Null => out.push(0),
        Value::Bool(b) => {
            out.push(1);
            out.push(*b as u8);
        }
        Value::Int(v) => {
            out.push(2);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Value::Timestamp(v) => {
            out.push(2);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Value::Date(v) => {
            out.push(2);
            out.extend_from_slice(&(*v as i64).to_le_bytes());
        }
        Value::Float(f) => {
            if f.is_finite() && f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                out.push(2);
                out.extend_from_slice(&(*f as i64).to_le_bytes());
            } else {
                out.push(4);
                out.extend_from_slice(&f.to_bits().to_le_bytes());
            }
        }
        Value::Str(s) => {
            out.push(3);
            out.extend_from_slice(s.as_bytes());
        }
    }
    out
}

/// Stable 64-bit hash of a value under `seed`.
pub fn hash_value(value: &Value, seed: u64) -> u64 {
    fnv1a(seed, &canonical_bytes(value))
}

/// Stable hash of a composite key, used to route rows to partitions.
///
/// Partition assignment is persisted implicitly (a row lives in the partition it
/// was routed to), so this must never change for existing tables.
pub fn hash_key(values: &[Value]) -> u64 {
    let mut h = FNV_OFFSET;
    for v in values {
        h = fnv1a(h, &canonical_bytes(v));
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vectors_pin_the_hash_function() {
        // These constants are part of the on-disk contract: if a change to the
        // hash makes this test fail, persisted bloom filters and partition
        // routing are both invalidated and need a format version bump.
        assert_eq!(hash_value(&Value::Int(1), 0), 17_140_249_297_226_746_820);
        assert_eq!(
            hash_value(&Value::Str("orders".into()), 0),
            13_265_335_333_527_250_651
        );
        assert_eq!(
            hash_key(&[Value::Int(1), Value::Str("a".into())]),
            9_713_408_020_293_303_458
        );
    }

    #[test]
    fn numerically_equal_values_hash_alike() {
        assert_eq!(
            hash_value(&Value::Int(2), 7),
            hash_value(&Value::Float(2.0), 7)
        );
        assert_eq!(
            canonical_bytes(&Value::Int(5)),
            canonical_bytes(&Value::Timestamp(5))
        );
    }

    #[test]
    fn different_values_mostly_differ() {
        let a = hash_value(&Value::Str("alpha".into()), 1);
        let b = hash_value(&Value::Str("beta".into()), 1);
        let c = hash_value(&Value::Str("alpha".into()), 2);
        assert_ne!(a, b);
        assert_ne!(a, c);
    }
}
