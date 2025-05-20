//! Common utilities for HTTP handlers

use common::BoxResult;

/// Validates that a dataset name follows the required format:
/// - Must be lowercase
/// - Can only contain letters, underscores, and numbers
pub fn validate_dataset_name(name: &str) -> BoxResult<()> {
    if name.is_empty() {
        return Err("name must not be empty".into());
    }

    if let Some(c) = name
        .chars()
        .find(|&c| !(c.is_ascii_lowercase() || c == '_' || c.is_numeric()))
    {
        return Err(format!("invalid character '{c}' in name '{name}'").into());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_dataset_name;

    #[test]
    fn accept_valid_dataset_names() {
        assert!(validate_dataset_name("my_dataset").is_ok());
        assert!(validate_dataset_name("my_dataset_123").is_ok());
        assert!(validate_dataset_name("__my_dataset_123").is_ok());
    }

    #[test]
    fn reject_invalid_dataset_names() {
        // Empty name is not allowed
        assert!(
            validate_dataset_name("").is_err(),
            "empty name is not allowed"
        );

        // Hyphens are not allowed
        assert!(
            validate_dataset_name("my-dataset").is_err(),
            "hyphens are not allowed"
        );
        assert!(
            validate_dataset_name("my_dataset-123").is_err(),
            "hyphens are not allowed"
        );

        // Spaces are not allowed
        assert!(
            validate_dataset_name("my dataset").is_err(),
            "spaces are not allowed"
        );
        assert!(
            validate_dataset_name("my dataset_123").is_err(),
            "spaces are not allowed"
        );

        // Uppercase letters are not allowed
        assert!(
            validate_dataset_name("MyDataset").is_err(),
            "uppercase letters are not allowed"
        );
        assert!(
            validate_dataset_name("MY_DATASET").is_err(),
            "uppercase letters are not allowed"
        );

        // Special characters are not allowed
        assert!(
            validate_dataset_name("my@dataset").is_err(),
            "special character '@' is not allowed"
        );
        assert!(
            validate_dataset_name("my.dataset").is_err(),
            "special character '.' is not allowed"
        );
        assert!(
            validate_dataset_name("my#dataset").is_err(),
            "special character '#' is not allowed"
        );
        assert!(
            validate_dataset_name("my$dataset").is_err(),
            "special character '$' is not allowed"
        );
        assert!(
            validate_dataset_name("my&dataset").is_err(),
            "special character '&' is not allowed"
        );
        assert!(
            validate_dataset_name("my*dataset").is_err(),
            "special character '*' is not allowed"
        );
        assert!(
            validate_dataset_name("my/dataset").is_err(),
            "special character '/' is not allowed"
        );
        assert!(
            validate_dataset_name("my\\dataset").is_err(),
            "special character '\\' is not allowed"
        );
    }
}
