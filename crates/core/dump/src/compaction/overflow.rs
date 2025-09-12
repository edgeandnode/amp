use std::{
    fmt::{Debug, Display, Formatter},
    num::NonZero,
    str::FromStr,
};

use common::arrow::array::ArrowNativeTypeOp;
use serde::{Deserialize, Serialize};

/// Represents a ratio of two integers, for example `3/2` or `1.5`. Simplifies
/// floating point math into integer math for more precise calculations.
///
/// This is used while making a best-effort decision on whether to compact segments
/// based on their [`SegmentSize`].
///
/// For example, an Overflow of `4/3` means
/// that two segments should be compacted
/// if their combined size is at least `4/3`
/// of the upper bound segment size.
/// This means that if the upper bound is `300MB`,
/// two segments should be compacted if their combined size
/// is at least `400MB`.
///
/// This helps to avoid situations where a tiny segment
/// (e.g. `2MB`) is attempting to merge with a large
/// segment (e.g. `299MB`), which would result in a
/// segment of `301MB` and thus exceed the upper bound
/// by a small margin.
///
/// Overflows may be specified as either a `u64`
/// or a `f64`. When specified as a `u64`, the denominator
/// is assumed to be `1`. When specified as a `f64`,
/// it is converted to a fraction with a denominator
/// of `PRECISION` and then reduced to its simplest form.
///
/// # Generics
/// - `PRECISION`: The precision to use when converting
///  a `f64` to a fraction. Defaults to `10000`. This means
/// that a `f64` value of `1.2345` would be converted
/// to `12345/10000` and then reduced to `2469/2000` which
/// is represented as `Overflow(2469, 2000)`.
///
/// # Member Variables
/// - `0`: The numerator of the overflow.
/// - `1`: The denominator of the overflow.
/// ```
#[derive(Clone, Copy)]
pub struct OverflowFactory<const PRECISION: u64 = 10000>(pub(super) u64, pub(super) u64);
pub type Overflow = OverflowFactory<10000>;

impl Overflow {
    #[cfg(test)]
    fn new(numerator: u64, denominator: u64) -> Self {
        if let Some(denominator) = NonZero::new(denominator)
            && let Some(numerator) = NonZero::new(numerator)
        {
            Self(numerator.get(), denominator.get())
        } else {
            Self::default()
        }
    }
}

impl<const PRECISION: u64> OverflowFactory<PRECISION> {
    #[inline]
    pub fn soft_limit<T: TryInto<u64> + TryFrom<u64> + Copy>(&self, value: T) -> T {
        if let Ok(value) = value.try_into()
            && let Some(value) = value.checked_mul(self.0)
            && let Some(value) = value.checked_div(self.1)
            && let Ok(value) = value.try_into()
        {
            value
        } else {
            value
        }
    }

    /// Reduces the overflow to its simplest form.
    /// For example, `Overflow(4, 2)` would be reduced to `Overflow(2, 1)`.
    /// If the precision is `0`, it is treated as `1`.
    /// If the numerator is `0`, the overflow is not reduced.
    #[inline]
    pub fn reduce(&mut self) {
        if self.0 == 0 {
            return;
        }

        let mut gcd = self.0;
        let mut b = self.1.max(1);

        if b > gcd {
            std::mem::swap(&mut gcd, &mut b);
        }

        while b != 0 {
            let temp = gcd;
            gcd = b;
            b = temp % b;
        }

        self.0 /= gcd.max(1);
        self.1 /= gcd.max(1);
    }
}

impl<const N: u64> Debug for OverflowFactory<N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Overflow( {self}, precision: {N} )")
    }
}

impl<const N: u64> Display for OverflowFactory<N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.1 == 1 {
            write!(f, "{}", self.0)
        } else {
            write!(f, "{}/{}", self.0, self.1)
        }
    }
}

impl<const N: u64> Default for OverflowFactory<N> {
    fn default() -> Self {
        Self(1, 1)
    }
}

impl<const N: u64> From<u64> for OverflowFactory<N> {
    fn from(value: u64) -> Self {
        if let Some(value) = NonZero::new(value) {
            Self(value.get(), 1)
        } else {
            Self::default()
        }
    }
}

impl<const PRECISION: u64> From<f64> for OverflowFactory<PRECISION> {
    /// Converts a floating point number to an `Overflow` ratio.
    /// The float is multiplied by the `PRECISION` constant to
    /// convert it to a fraction, then reduced to its simplest form.
    /// If the float is infinite, NaN, or non-positive, or if
    /// the `PRECISION` is `0`, the default `Overflow(1, 1)` is returned.
    ///
    /// # Panics
    /// This function does not panic. If the float is invalid,
    /// it returns the default `Overflow(1, 1)`.
    fn from(value: f64) -> Self {
        if value.is_infinite() || value.is_nan() || value <= 0.0 || PRECISION == 0 {
            return Self::default();
        } else if value > 1.0 && value.round() == value {
            return Self(value as u64, 1);
        } else if let Ok(value) = value.mul_checked(PRECISION as f64)
            && let Some(a) = NonZero::new(value as u64)
        {
            let mut overflow = Self(a.get(), PRECISION);
            overflow.reduce();
            overflow
        } else {
            tracing::warn!("Overflow value {value} is too large or invalid, using default");
            Self::default()
        }
    }
}

impl<const PRECISION: u64> FromStr for OverflowFactory<PRECISION> {
    type Err = String;

    fn from_str(v: &str) -> Result<Self, Self::Err> {
        let is_fractional =
            v.matches('/').count() == 1 && v.split('/').all(|part| !part.trim().is_empty());
        let is_decimal = v.contains('.') && v.parse::<f64>().is_ok();
        if is_fractional && is_decimal {
            Err("Overflow cannot be both a decimal and a fraction".into())
        } else if is_fractional {
            let parts = v.trim().split('/').map(str::trim).collect::<Vec<&str>>();
            let numerator = parts[0]
                .trim()
                .parse::<u64>()
                .map_err(|_| String::from("Invalid numerator in overflow fraction"))?;
            let denominator = parts[1]
                .trim()
                .parse::<u64>()
                .ok()
                .and_then(NonZero::new)
                .ok_or_else(|| String::from("Invalid denominator in overflow fraction"))?;
            let mut overflow = OverflowFactory(numerator, denominator.get());
            overflow.reduce();
            Ok(overflow)
        } else if is_decimal {
            let parts = v.trim().split('.').map(str::trim).collect::<Vec<&str>>();
            let integer_part = parts[0];
            let fractional_part = parts[1];
            let mut fractional_length = parts[1].len().min(PRECISION as usize);

            while fractional_length > 0 {
                if let Some(precision) = 10u64.checked_pow(fractional_length as u32) {
                    let numerator_part =
                        integer_part.to_string() + &fractional_part[..fractional_length];
                    let numerator = numerator_part
                        .parse::<u64>()
                        .map_err(|_| format!("Invalid overflow decimal: {numerator_part}"))?;

                    let mut overflow = OverflowFactory(numerator, precision);
                    overflow.reduce();

                    return Ok(overflow);
                } else {
                    fractional_length -= 1;
                }
            }
            Err(String::from("Invalid overflow decimal"))
        } else {
            let value = v
                .trim()
                .parse::<u64>()
                .map_err(|_| String::from("Invalid overflow value"))?;
            Ok(OverflowFactory::<PRECISION>::from(value))
        }
    }
}

impl<'de, const PRECISION: u64> Deserialize<'de> for OverflowFactory<PRECISION> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct OverflowVisitor<const N: u64>;
        impl<'de, const N: u64> serde::de::Visitor<'de> for OverflowVisitor<N> {
            type Value = OverflowFactory<N>;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("a positive integer or float representing overflow")
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(OverflowFactory::<N>::from(value))
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if let Ok(v) = u64::try_from(v)
                    && let Some(v) = NonZero::new(v)
                {
                    Ok(OverflowFactory::<N>::from(v.get()))
                } else {
                    Err(E::custom("Overflow must be a positive integer"))
                }
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(OverflowFactory::<N>::from(value))
            }
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                OverflowFactory::<N>::from_str(v).map_err(E::custom)
            }
        }
        deserializer.deserialize_any(OverflowVisitor::<PRECISION>)
    }
}

impl<const N: u64> Serialize for OverflowFactory<N> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if self.1 == 1 {
            serializer.serialize_u64(self.0)
        } else {
            serializer.serialize_str(&self.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::Overflow;

    #[test]
    fn overflow_from_f64() {
        let overflow: Overflow = 1.5f64.into();
        assert_eq!(overflow.0, 3);
        assert_eq!(overflow.1, 2);

        let overflow: Overflow = 2.0f64.into();
        assert_eq!(overflow.0, 2);
        assert_eq!(overflow.1, 1);

        let overflow: Overflow = 0.0f64.into();
        assert_eq!(overflow.0, 1);
        assert_eq!(overflow.1, 1);

        let overflow: Overflow = (-1.0f64).into();
        assert_eq!(overflow.0, 1);
        assert_eq!(overflow.1, 1);

        let overflow: Overflow = f64::INFINITY.into();
        assert_eq!(overflow.0, 1);
        assert_eq!(overflow.1, 1);

        let overflow: Overflow = f64::NAN.into();
        assert_eq!(overflow.0, 1);
        assert_eq!(overflow.1, 1);

        let overflow: Overflow = 1.2345f64.into();
        assert_eq!(overflow.0, 2469);
        assert_eq!(overflow.1, 2000);

        let overflow: super::OverflowFactory<10> = (1.0 / 3.0).into();
        assert_eq!(overflow.0, 3);
        assert_eq!(overflow.1, 10);
    }

    #[test]
    fn overflow_from_u64() {
        let overflow: Overflow = 3u64.into();
        assert_eq!(overflow.0, 3);
        assert_eq!(overflow.1, 1);
        let overflow: Overflow = 0u64.into();
        assert_eq!(overflow.0, 1);
        assert_eq!(overflow.1, 1);
    }

    #[test]
    fn overflow_reduce() {
        let mut overflow = Overflow::new(12345, 10000);
        overflow.reduce();
        assert_eq!(overflow.0, 2469);
        assert_eq!(overflow.1, 2000);
        let mut overflow = Overflow::new(2, 4);
        overflow.reduce();
        assert_eq!(overflow.0, 1);
        assert_eq!(overflow.1, 2);
        let mut overflow = Overflow::new(0, 4);
        overflow.reduce();
        assert_eq!(overflow.0, 1);
        assert_eq!(overflow.1, 1);
        const PRECISION: u64 = 100;
        let mut overflow = super::OverflowFactory::<PRECISION>(250, 100);
        overflow.reduce();
        assert_eq!(overflow.0, 5);
        assert_eq!(overflow.1, 2);
    }

    #[test]
    fn serde_overflow() {
        #[derive(Deserialize, Serialize, Debug)]
        struct Config {
            overflow: super::Overflow,
        }

        let json = r#"{"overflow": 1.5}"#;
        let config: Config = serde_json::from_str(json).unwrap();
        assert_eq!(config.overflow.0, 3);
        assert_eq!(config.overflow.1, 2);

        let config = Config {
            overflow: "12345/10000".parse().unwrap(),
        };
        let json = serde_json::to_string(&config).unwrap();
        assert_eq!(json, r#"{"overflow":"2469/2000"}"#);

        let config: Config = serde_json::from_str(&json).unwrap();
        assert_eq!(config.overflow.0, 2469);
        assert_eq!(config.overflow.1, 2000);

        let config = Config {
            overflow: 3u64.into(),
        };
        let json = serde_json::to_string(&config).unwrap();
        assert_eq!(json, r#"{"overflow":3}"#);

        let config: Config = serde_json::from_str(&json).unwrap();
        assert_eq!(config.overflow.0, 3);
        assert_eq!(config.overflow.1, 1);

        let config = Config {
            overflow: "1.2345".parse().unwrap(),
        };
        let json = serde_json::to_string(&config).unwrap();
        assert_eq!(json, r#"{"overflow":"2469/2000"}"#);
        let config: Config = serde_json::from_str(&json).unwrap();
        assert_eq!(config.overflow.0, 2469);
        assert_eq!(config.overflow.1, 2000);
    }
}
