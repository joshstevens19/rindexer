use std::fmt;
use std::str::FromStr;

use alloy::primitives::Address;
use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

/// Wrapper for a ClickHouse `FixedString(N)` column whose contents are ASCII text.
///
/// The clickhouse 0.15 driver requires `FixedString(N)` to be (de)serialized as a
/// fixed-size byte tuple `[u8; N]`. This wrapper provides that compatibility while
/// exposing a string-like API to the rest of the codebase.
#[derive(Clone, Copy)]
pub struct ClickhouseFixedString<const N: usize>([u8; N]);

impl<const N: usize> ClickhouseFixedString<N> {
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).expect("ClickHouse FixedString contains non-UTF8 bytes")
    }

    pub fn into_string(self) -> String {
        self.as_str().to_owned()
    }
}

impl<const N: usize> fmt::Debug for ClickhouseFixedString<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}

impl<const N: usize> fmt::Display for ClickhouseFixedString<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<const N: usize> From<ClickhouseFixedString<N>> for String {
    fn from(v: ClickhouseFixedString<N>) -> Self {
        v.into_string()
    }
}

impl<'de, const N: usize> Deserialize<'de> for ClickhouseFixedString<N> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct FixedStringVisitor<const N: usize>;

        impl<'de, const N: usize> Visitor<'de> for FixedStringVisitor<N> {
            type Value = ClickhouseFixedString<N>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "a ClickHouse FixedString({}) of {} bytes", N, N)
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut buf = [0u8; N];
                for (i, slot) in buf.iter_mut().enumerate() {
                    *slot = seq
                        .next_element::<u8>()?
                        .ok_or_else(|| de::Error::invalid_length(i, &self))?;
                }
                Ok(ClickhouseFixedString(buf))
            }
        }

        deserializer.deserialize_tuple(N, FixedStringVisitor::<N>)
    }
}

/// `FixedString(66)` — `0x` + 64 hex chars. Used for `tx_hash`, `block_hash`, `parent_hash`.
pub type ClickhouseHash = ClickhouseFixedString<66>;

/// `FixedString(42)` — `0x` + 40 hex chars. Used for address columns.
pub type ClickhouseAddress = ClickhouseFixedString<42>;

impl From<ClickhouseAddress> for Address {
    fn from(v: ClickhouseAddress) -> Self {
        Address::from_str(v.as_str())
            .expect("ClickHouse FixedString(42) address column is not a valid address")
    }
}
