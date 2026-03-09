//! Job notification types for worker coordination

use amp_worker_core::jobs::job_id::JobId;

/// The payload of a job notification (without `node_id`, which is in the wrapper)
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Notification {
    pub job_id: JobId,
    pub action: Action,
}

impl Notification {
    /// Create a new start action
    #[must_use]
    pub fn start(job_id: JobId) -> Self {
        Self {
            job_id,
            action: Action::Start,
        }
    }

    /// Create a new stop action
    #[must_use]
    pub fn stop(job_id: JobId) -> Self {
        Self {
            job_id,
            action: Action::Stop,
        }
    }

    /// Create a new rematerialize action
    #[must_use]
    pub fn rematerialize(job_id: JobId, start_block: u64, end_block: u64) -> Self {
        Self {
            job_id,
            action: Action::Rematerialize {
                start_block,
                end_block,
            },
        }
    }
}

/// Job notification actions
///
/// These actions coordinate the jobs state and the write lock on the output table locations.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Action {
    /// Start the job
    ///
    /// Fetch the job descriptor from the Metadata DB job queue and start the job.
    Start,

    /// Stop the job
    ///
    /// Stop the job: mark the job as stopped and release the locations by deleting
    /// the row from the `jobs` table.
    Stop,

    /// Rematerialize a block range
    ///
    /// Re-extract the specified block range for the dataset. New files will have
    /// newer timestamps and automatically become the canonical segments.
    Rematerialize {
        /// Start block of the range (inclusive)
        start_block: u64,
        /// End block of the range (inclusive)
        end_block: u64,
    },
}

impl std::fmt::Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Start => write!(f, "START"),
            Self::Stop => write!(f, "STOP"),
            Self::Rematerialize {
                start_block,
                end_block,
            } => write!(f, "REMATERIALIZE({start_block}..{end_block})"),
        }
    }
}

impl serde::Serialize for Action {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;

        match self {
            Self::Start => serializer.serialize_str("START"),
            Self::Stop => serializer.serialize_str("STOP"),
            Self::Rematerialize {
                start_block,
                end_block,
            } => {
                // Serialize as: {"REMATERIALIZE": {"start_block": N, "end_block": M}}
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry(
                    "REMATERIALIZE",
                    &RematerializePayload {
                        start_block: *start_block,
                        end_block: *end_block,
                    },
                )?;
                map.end()
            }
        }
    }
}

impl<'de> serde::Deserialize<'de> for Action {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, MapAccess, Visitor};

        struct ActionVisitor;

        impl<'de> Visitor<'de> for ActionVisitor {
            type Value = Action;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str(r#""START", "STOP", or {"REMATERIALIZE": {...}}"#)
            }

            fn visit_str<E>(self, value: &str) -> Result<Action, E>
            where
                E: de::Error,
            {
                match value {
                    s if s.eq_ignore_ascii_case("START") => Ok(Action::Start),
                    s if s.eq_ignore_ascii_case("STOP") => Ok(Action::Stop),
                    _ => Err(de::Error::unknown_variant(
                        value,
                        &["START", "STOP", "REMATERIALIZE"],
                    )),
                }
            }

            fn visit_map<M>(self, mut map: M) -> Result<Action, M::Error>
            where
                M: MapAccess<'de>,
            {
                let Some(key) = map.next_key::<String>()? else {
                    return Err(de::Error::custom("expected action type"));
                };

                if key.eq_ignore_ascii_case("REMATERIALIZE") {
                    let payload: RematerializePayload = map.next_value()?;
                    Ok(Action::Rematerialize {
                        start_block: payload.start_block,
                        end_block: payload.end_block,
                    })
                } else {
                    Err(de::Error::unknown_variant(
                        &key,
                        &["START", "STOP", "REMATERIALIZE"],
                    ))
                }
            }
        }

        deserializer.deserialize_any(ActionVisitor)
    }
}

/// Payload for the rematerialize action
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct RematerializePayload {
    start_block: u64,
    end_block: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn action_serialization_start() {
        let action = Action::Start;
        let json = serde_json::to_string(&action).unwrap();
        assert_eq!(json, r#""START""#);
    }

    #[test]
    fn action_serialization_stop() {
        let action = Action::Stop;
        let json = serde_json::to_string(&action).unwrap();
        assert_eq!(json, r#""STOP""#);
    }

    #[test]
    fn action_serialization_rematerialize() {
        let action = Action::Rematerialize {
            start_block: 1000000,
            end_block: 2000000,
        };
        let json = serde_json::to_string(&action).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(
            parsed,
            serde_json::json!({
                "REMATERIALIZE": {
                    "start_block": 1000000,
                    "end_block": 2000000
                }
            })
        );
    }

    #[test]
    fn action_deserialization_start() {
        let action: Action = serde_json::from_str(r#""START""#).unwrap();
        assert!(matches!(action, Action::Start));

        // Case-insensitive
        let action: Action = serde_json::from_str(r#""start""#).unwrap();
        assert!(matches!(action, Action::Start));
    }

    #[test]
    fn action_deserialization_stop() {
        let action: Action = serde_json::from_str(r#""STOP""#).unwrap();
        assert!(matches!(action, Action::Stop));

        // Case-insensitive
        let action: Action = serde_json::from_str(r#""stop""#).unwrap();
        assert!(matches!(action, Action::Stop));
    }

    #[test]
    fn action_deserialization_rematerialize() {
        let json = r#"{"REMATERIALIZE": {"start_block": 1000000, "end_block": 2000000}}"#;
        let action: Action = serde_json::from_str(json).unwrap();
        assert!(matches!(
            action,
            Action::Rematerialize {
                start_block: 1000000,
                end_block: 2000000
            }
        ));

        // Case-insensitive
        let json = r#"{"rematerialize": {"start_block": 500, "end_block": 1000}}"#;
        let action: Action = serde_json::from_str(json).unwrap();
        assert!(matches!(
            action,
            Action::Rematerialize {
                start_block: 500,
                end_block: 1000
            }
        ));
    }

    #[test]
    fn action_deserialization_invalid() {
        let result: Result<Action, _> = serde_json::from_str(r#""INVALID""#);
        assert!(result.is_err());

        let result: Result<Action, _> = serde_json::from_str(r#"{"UNKNOWN": {}}"#);
        assert!(result.is_err());
    }

    #[test]
    fn notification_serialization_roundtrip() {
        let notif = Notification::start(JobId::from(metadata_db::jobs::JobId::from_i64_unchecked(
            123,
        )));
        let json = serde_json::to_string(&notif).unwrap();
        let parsed: Notification = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, notif);

        let notif = Notification::stop(JobId::from(metadata_db::jobs::JobId::from_i64_unchecked(
            456,
        )));
        let json = serde_json::to_string(&notif).unwrap();
        let parsed: Notification = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, notif);

        let notif = Notification::rematerialize(
            JobId::from(metadata_db::jobs::JobId::from_i64_unchecked(789)),
            1000,
            2000,
        );
        let json = serde_json::to_string(&notif).unwrap();
        let parsed: Notification = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, notif);
    }
}
