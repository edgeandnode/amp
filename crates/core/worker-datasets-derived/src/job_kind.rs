/// The `kind` discriminator value for derived dataset materialization jobs.
pub const JOB_KIND: &str = "materialize-derived";

/// Type-safe representation of the derived dataset materialization job kind.
///
/// Serializes as the string [`JOB_KIND`] and only deserializes successfully
/// when the input matches that exact value.
#[derive(Debug)]
pub struct MaterializeDerivedJobKind;

impl std::fmt::Display for MaterializeDerivedJobKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(JOB_KIND)
    }
}

impl std::str::FromStr for MaterializeDerivedJobKind {
    type Err = JobKindMismatchError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s != JOB_KIND {
            return Err(JobKindMismatchError(s.to_owned()));
        }
        Ok(MaterializeDerivedJobKind)
    }
}

impl serde::Serialize for MaterializeDerivedJobKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(JOB_KIND)
    }
}

impl<'de> serde::Deserialize<'de> for MaterializeDerivedJobKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// Error returned when a string does not match the expected [`JOB_KIND`].
#[derive(Debug, thiserror::Error)]
#[error("job kind mismatch: expected `{JOB_KIND}`, got `{0}`")]
pub struct JobKindMismatchError(String);
