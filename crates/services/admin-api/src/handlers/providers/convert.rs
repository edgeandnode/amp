//! JSON to TOML conversion utilities for provider configuration

use serde::de::Error as _;

/// Convert a JSON Map to a TOML Table
///
/// This helper function converts `serde_json::Map` to `toml::Table` directly
/// using `serde` serialization/deserialization.
///
/// ## Failure Cases
/// This conversion can fail in the following scenarios:
/// - **Null values**: TOML doesn't support null/None values (JSON nulls are incompatible)
/// - **Mixed-type arrays**: TOML requires arrays to contain homogeneous types
/// - **Invalid TOML keys**: Keys that contain characters invalid for TOML identifiers
/// - **Deeply nested structures**: Complex nesting that doesn't map well to TOML format
/// - **Deserialization errors**: Internal serde deserialization failures
pub fn json_map_to_toml_table(
    json_map: serde_json::Map<String, serde_json::Value>,
) -> Result<toml::Table, serde_json::Error> {
    serde_json::from_value(serde_json::Value::Object(json_map))
}

/// Convert a TOML Table to a JSON Map
///
/// This helper function converts `toml::Table` to `serde_json::Map` directly
/// using `serde` serialization/deserialization.
///
/// ## Failure Cases
/// This conversion can fail in the following scenarios:
/// - **Serialization errors**: Internal serde_json serialization failures
/// - **Non-object result**: If TOML somehow doesn't serialize to a JSON object (shouldn't happen in practice)
/// - **Memory limitations**: Very large TOML tables that exceed available memory during serialization
///
/// ## Notes
/// - TOML datetime values serialize to JSON strings successfully
/// - Large integers are preserved within JSON's safe integer range
pub fn from_toml_table_to_json_map(
    toml_table: toml::Table,
) -> Result<serde_json::Map<String, serde_json::Value>, serde_json::Error> {
    let json_value = serde_json::to_value(toml_table)?;
    let map = match json_value {
        serde_json::Value::Object(map) => map,
        _ => {
            return Err(serde_json::Error::custom(
                "TOML table must convert to JSON object",
            ));
        }
    };
    Ok(map)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    mod from_json_conversion {
        use super::*;

        #[test]
        fn json_map_to_toml_table_with_basic_types_succeeds() {
            //* Given
            let map = serde_json::Map::from_iter([
                (
                    "url".to_string(),
                    json!("https://ethereum-mainnet.example.com"),
                ),
                ("timeout".to_string(), json!(30)),
                ("enabled".to_string(), json!(true)),
            ]);

            //* When
            let result = json_map_to_toml_table(map);

            //* Then
            assert!(result.is_ok(), "conversion should succeed with basic types");
            let table = result.expect("should return valid TOML table");
            assert_eq!(
                table
                    .get("url")
                    .expect("should have url")
                    .as_str()
                    .expect("should be string"),
                "https://ethereum-mainnet.example.com"
            );
            assert_eq!(
                table
                    .get("timeout")
                    .expect("should have timeout")
                    .as_integer()
                    .expect("should be integer"),
                30
            );
            assert_eq!(
                table
                    .get("enabled")
                    .expect("should have enabled")
                    .as_bool()
                    .expect("should be bool"),
                true
            );
        }

        #[test]
        fn json_map_to_toml_table_with_empty_map_succeeds() {
            //* Given
            let map = serde_json::Map::new();

            //* When
            let result = json_map_to_toml_table(map);

            //* Then
            assert!(result.is_ok(), "conversion should succeed with empty map");
            let table = result.expect("should return valid TOML table");
            assert!(table.is_empty(), "table should be empty");
        }

        #[test]
        fn json_map_to_toml_table_with_nested_object_and_array_succeeds() {
            //* Given
            let map = serde_json::Map::from_iter([
                (
                    "config".to_string(),
                    json!({
                        "retry_count": 3,
                        "backoff": "exponential"
                    }),
                ),
                (
                    "endpoints".to_string(),
                    json!(["http://node1", "http://node2"]),
                ),
            ]);

            //* When
            let result = json_map_to_toml_table(map);

            //* Then
            assert!(
                result.is_ok(),
                "conversion should succeed with nested objects and arrays"
            );
            let table = result.expect("should return valid TOML table");

            let config = table
                .get("config")
                .expect("should have config")
                .as_table()
                .expect("should be table");
            assert_eq!(
                config
                    .get("retry_count")
                    .expect("should have retry_count")
                    .as_integer()
                    .expect("should be integer"),
                3
            );
            assert_eq!(
                config
                    .get("backoff")
                    .expect("should have backoff")
                    .as_str()
                    .expect("should be string"),
                "exponential"
            );

            let endpoints = table
                .get("endpoints")
                .expect("should have endpoints")
                .as_array()
                .expect("should be array");
            assert_eq!(endpoints.len(), 2, "should have 2 endpoints");
            assert_eq!(
                endpoints[0].as_str().expect("should be string"),
                "http://node1"
            );
            assert_eq!(
                endpoints[1].as_str().expect("should be string"),
                "http://node2"
            );
        }

        #[test]
        fn json_map_to_toml_table_with_null_value_fails() {
            //* Given
            let map = serde_json::Map::from_iter([("optional_field".to_string(), json!(null))]);

            //* When
            let result = json_map_to_toml_table(map);

            //* Then
            assert!(
                result.is_err(),
                "conversion should fail with null values as TOML doesn't support null"
            );
        }
    }

    mod from_toml_conversion {
        use super::*;

        #[test]
        fn from_toml_table_to_json_map_with_basic_types_succeeds() {
            //* Given
            let table = toml::Table::from_iter([
                (
                    "url".to_string(),
                    toml::Value::String("https://ethereum-mainnet.example.com".to_string()),
                ),
                ("timeout".to_string(), toml::Value::Integer(30)),
                ("enabled".to_string(), toml::Value::Boolean(true)),
            ]);

            //* When
            let result = from_toml_table_to_json_map(table);

            //* Then
            assert!(result.is_ok(), "conversion should succeed with basic types");
            let map = result.expect("should return valid JSON map");
            assert_eq!(
                map.get("url")
                    .expect("should have url")
                    .as_str()
                    .expect("should be string"),
                "https://ethereum-mainnet.example.com"
            );
            assert_eq!(
                map.get("timeout")
                    .expect("should have timeout")
                    .as_i64()
                    .expect("should be integer"),
                30
            );
            assert_eq!(
                map.get("enabled")
                    .expect("should have enabled")
                    .as_bool()
                    .expect("should be bool"),
                true
            );
        }

        #[test]
        fn from_toml_table_to_json_map_with_empty_table_succeeds() {
            //* Given
            let table = toml::Table::new();

            //* When
            let result = from_toml_table_to_json_map(table);

            //* Then
            assert!(result.is_ok(), "conversion should succeed with empty table");
            let map = result.expect("should return valid JSON map");
            assert!(map.is_empty(), "map should be empty");
        }

        #[test]
        fn from_toml_table_to_json_map_with_nested_table_succeeds() {
            //* Given
            let nested_table = toml::Table::from_iter([
                ("retry_count".to_string(), toml::Value::Integer(3)),
                (
                    "backoff".to_string(),
                    toml::Value::String("exponential".to_string()),
                ),
            ]);

            let table =
                toml::Table::from_iter([("config".to_string(), toml::Value::Table(nested_table))]);

            //* When
            let result = from_toml_table_to_json_map(table);

            //* Then
            assert!(
                result.is_ok(),
                "conversion should succeed with nested tables"
            );
            let map = result.expect("should return valid JSON map");
            let config = map
                .get("config")
                .expect("should have config")
                .as_object()
                .expect("should be object");
            assert_eq!(
                config
                    .get("retry_count")
                    .expect("should have retry_count")
                    .as_i64()
                    .expect("should be integer"),
                3
            );
            assert_eq!(
                config
                    .get("backoff")
                    .expect("should have backoff")
                    .as_str()
                    .expect("should be string"),
                "exponential"
            );
        }

        #[test]
        fn from_toml_table_to_json_map_with_large_integer_succeeds() {
            //* Given
            // Use a large but still JSON-safe integer
            let large_int = 9007199254740991i64; // 2^53 - 1, max safe JSON integer
            let table = toml::Table::from_iter([(
                "large_number".to_string(),
                toml::Value::Integer(large_int),
            )]);

            //* When
            let result = from_toml_table_to_json_map(table);

            //* Then
            assert!(
                result.is_ok(),
                "conversion should succeed with JSON-safe large integers"
            );
            let map = result.expect("should return valid JSON map");
            assert_eq!(
                map.get("large_number")
                    .expect("should have large_number")
                    .as_i64()
                    .expect("should be integer"),
                large_int
            );
        }
    }
}
