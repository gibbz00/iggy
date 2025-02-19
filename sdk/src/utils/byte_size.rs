use crate::error::Error;
use byte_unit::{Byte, UnitType};
use core::fmt;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use super::duration::IggyDuration;

/// A struct for representing byte sizes with various utility functions.
///
/// This struct uses `Byte` from `byte_unit` crate.
/// It also implements serialization and deserialization via the `serde` crate.
///
/// # Example
///
/// ```
/// use iggy::utils::byte_size::IggyByteSize;
/// use std::str::FromStr;
///
/// let size = IggyByteSize::from(568_000_000_u64);
/// assert_eq!(568_000_000, size.as_bytes_u64());
/// assert_eq!("568 MB", size.as_human_string());
/// assert_eq!("568 MB", format!("{}", size));
///
/// let size = IggyByteSize::from(0_u64);
/// assert_eq!("unlimited", size.as_human_string_with_zero_as_unlimited());
/// assert_eq!("0 B", size.as_human_string());
/// assert_eq!(0, size.as_bytes_u64());
///
/// let size = IggyByteSize::from_str("1 GB").unwrap();
/// assert_eq!(1_000_000_000, size.as_bytes_u64());
/// assert_eq!("1 GB", size.as_human_string());
/// assert_eq!("1 GB", format!("{}", size));
/// ```
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub struct IggyByteSize(Byte);

impl Default for IggyByteSize {
    fn default() -> Self {
        Self(Byte::from_u64(0))
    }
}

impl IggyByteSize {
    /// Returns the byte size as a `u64`.
    pub fn as_bytes_u64(&self) -> u64 {
        self.0.as_u64()
    }

    /// Returns a human-readable string representation of the byte size using decimal units.
    pub fn as_human_string(&self) -> String {
        self.0.get_appropriate_unit(UnitType::Decimal).to_string()
    }

    /// Returns a human-readable string representation of the byte size.
    /// Returns "unlimited" if the size is zero.
    pub fn as_human_string_with_zero_as_unlimited(&self) -> String {
        if self.as_bytes_u64() == 0 {
            return "unlimited".to_string();
        }
        self.0.get_appropriate_unit(UnitType::Decimal).to_string()
    }

    /// Calculates the throughput based on the provided duration and returns a human-readable string.
    pub(crate) fn _as_human_throughput_string(&self, duration: &IggyDuration) -> String {
        if duration.is_zero() {
            return "0 B/s".to_string();
        }
        let seconds = duration.as_secs_f64();
        let normalized_bytes_per_second = Self::from((self.as_bytes_u64() as f64 / seconds) as u64);
        format!("{}/s", normalized_bytes_per_second)
    }
}

/// Converts a `u64` bytes to `IggyByteSize`.
impl From<u64> for IggyByteSize {
    fn from(byte_size: u64) -> Self {
        IggyByteSize(Byte::from_u64(byte_size))
    }
}

/// Converts an `Option<u64>` bytes to `IggyByteSize`.
impl From<Option<u64>> for IggyByteSize {
    fn from(byte_size: Option<u64>) -> Self {
        match byte_size {
            Some(value) => IggyByteSize(Byte::from_u64(value)),
            None => IggyByteSize(Byte::from_u64(0)),
        }
    }
}

impl FromStr for IggyByteSize {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if matches!(s, "0" | "unlimited" | "Unlimited" | "none" | "None") {
            Ok(IggyByteSize(Byte::from_u64(0)))
        } else {
            Ok(IggyByteSize(Byte::from_str(s)?))
        }
    }
}

impl PartialEq<u64> for IggyByteSize {
    fn eq(&self, other: &u64) -> bool {
        self.as_bytes_u64() == *other
    }
}

impl PartialOrd<u64> for IggyByteSize {
    fn partial_cmp(&self, other: &u64) -> Option<std::cmp::Ordering> {
        self.as_bytes_u64().partial_cmp(other)
    }
}

impl fmt::Display for IggyByteSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_human_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_u64_ok() {
        let byte_size = IggyByteSize::from(123456789);
        assert_eq!(byte_size.as_bytes_u64(), 123456789);
    }

    #[test]
    fn test_from_u64_zero() {
        let byte_size = IggyByteSize::from(0);
        assert_eq!(byte_size.as_bytes_u64(), 0);
    }

    #[test]
    fn test_from_str_ok() {
        let byte_size = IggyByteSize::from_str("123456789").unwrap();
        assert_eq!(byte_size.as_bytes_u64(), 123456789);
    }

    #[test]
    fn test_from_str_zero() {
        let byte_size = IggyByteSize::from_str("0").unwrap();
        assert_eq!(byte_size.as_bytes_u64(), 0);
    }

    #[test]
    fn test_from_str_invalid() {
        let byte_size = IggyByteSize::from_str("invalid");
        assert!(byte_size.is_err());
    }

    #[test]
    fn test_from_str_gigabyte() {
        let byte_size = IggyByteSize::from_str("1 GiB").unwrap();
        assert_eq!(byte_size.as_bytes_u64(), 1024 * 1024 * 1024);

        let byte_size = IggyByteSize::from_str("1 GB").unwrap();
        assert_eq!(byte_size.as_bytes_u64(), 1000 * 1000 * 1000);
    }

    #[test]
    fn test_from_str_megabyte() {
        let byte_size = IggyByteSize::from_str("1 MiB").unwrap();
        assert_eq!(byte_size.as_bytes_u64(), 1024 * 1024);

        let byte_size = IggyByteSize::from_str("1 MB").unwrap();
        assert_eq!(byte_size.as_bytes_u64(), 1000 * 1000);
    }

    #[test]
    fn test_to_human_string_ok() {
        let byte_size = IggyByteSize::from(1_073_000_000);
        assert_eq!(byte_size.as_human_string(), "1.073 GB");
    }

    #[test]
    fn test_to_human_string_zero() {
        let byte_size = IggyByteSize::from(0);
        assert_eq!(byte_size.as_human_string(), "0 B");
    }

    #[test]
    fn test_to_human_string_special_zero() {
        let byte_size = IggyByteSize::from(0);
        assert_eq!(
            byte_size.as_human_string_with_zero_as_unlimited(),
            "unlimited"
        );
    }

    #[test]
    fn test_throughput_ok() {
        let byte_size = IggyByteSize::from(1_073_000_000);
        let duration = IggyDuration::from_str("10s").unwrap();
        assert_eq!(
            byte_size._as_human_throughput_string(&duration),
            "107.3 MB/s"
        );
    }

    #[test]
    fn test_throughput_zero_size() {
        let byte_size = IggyByteSize::from(0);
        let duration = IggyDuration::from_str("10s").unwrap();
        assert_eq!(byte_size._as_human_throughput_string(&duration), "0 B/s");
    }

    #[test]
    fn test_throughput_zero_duration() {
        let byte_size = IggyByteSize::from(1_073_000_000);
        let duration = IggyDuration::from_str("0s").unwrap();
        assert_eq!(byte_size._as_human_throughput_string(&duration), "0 B/s");
    }

    #[test]
    fn test_throughput_very_low() {
        let byte_size = IggyByteSize::from(8);
        let duration = IggyDuration::from_str("1s").unwrap();
        assert_eq!(byte_size._as_human_throughput_string(&duration), "8 B/s");
    }

    #[test]
    fn test_throughput_very_high() {
        let byte_size = IggyByteSize::from(u64::MAX);
        let duration = IggyDuration::from_str("1s").unwrap();
        assert_eq!(
            byte_size._as_human_throughput_string(&duration),
            "18.446744073709553 EB/s"
        );
    }
}
