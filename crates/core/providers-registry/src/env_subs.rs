use std::env;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Variable not found: {0}")]
    MissingVar(String),
    #[error("Invalid variable syntax at position {position}: {message}")]
    InvalidSyntax { position: usize, message: String },
}

/// Substitutes ${VAR} patterns in a TOML value using a lookup function
pub fn substitute_vars<F>(value: &mut toml::Value, lookup: &F) -> Result<(), Error>
where
    F: Fn(&str) -> Option<String>,
{
    match value {
        toml::Value::String(s) => {
            *s = substitute_string(s, lookup)?;
        }
        toml::Value::Table(table) => {
            for (_, v) in table.iter_mut() {
                substitute_vars(v, lookup)?;
            }
        }
        toml::Value::Array(array) => {
            for v in array.iter_mut() {
                substitute_vars(v, lookup)?;
            }
        }
        _ => {} // Numbers, booleans, dates unchanged
    }
    Ok(())
}

/// Convenience function that uses environment variables as the lookup source
pub fn substitute_env_vars(value: &mut toml::Value) -> Result<(), Error> {
    substitute_vars(value, &|var_name| env::var(var_name).ok())
}

/// Substitutes ${VAR} patterns in a string using a lookup function
fn substitute_string<F>(template: &str, lookup: &F) -> Result<String, Error>
where
    F: Fn(&str) -> Option<String>,
{
    let mut result = String::new();
    let mut chars = template.chars().enumerate().peekable();

    while let Some((i, c)) = chars.next() {
        if c == '$' && chars.peek().map(|(_, c)| *c) == Some('{') {
            chars.next(); // consume '{'
            let start_pos = i;
            let mut var_name = String::new();
            let mut found_closing = false;

            // Collect characters until we hit '}'
            for (_, c) in chars.by_ref() {
                if c == '}' {
                    found_closing = true;
                    break;
                }
                var_name.push(c);
            }

            if !found_closing {
                return Err(Error::InvalidSyntax {
                    position: start_pos,
                    message: "Unclosed variable: missing '}'".to_string(),
                });
            }

            if var_name.is_empty() {
                return Err(Error::InvalidSyntax {
                    position: start_pos,
                    message: "Empty variable name: ${}".to_string(),
                });
            }

            // Look up the variable value
            match lookup(&var_name) {
                Some(value) => result.push_str(&value),
                None => return Err(Error::MissingVar(var_name)),
            }
        } else {
            result.push(c);
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn test_lookup() -> impl Fn(&str) -> Option<String> {
        let mut vars = HashMap::new();
        vars.insert("TEST_VAR".to_string(), "test_value".to_string());
        vars.insert("TEST_URL".to_string(), "http://localhost:8545".to_string());
        vars.insert("TEST_KIND".to_string(), "evm-rpc".to_string());
        vars.insert("TEST_HOST".to_string(), "localhost".to_string());

        move |name: &str| vars.get(name).cloned()
    }

    #[test]
    fn test_basic_substitution() {
        let lookup = test_lookup();
        let result = substitute_string("prefix_${TEST_VAR}_suffix", &lookup).unwrap();
        assert_eq!(result, "prefix_test_value_suffix");
    }

    #[test]
    fn test_no_substitution_needed() {
        let lookup = test_lookup();
        let result = substitute_string("no variables here", &lookup).unwrap();
        assert_eq!(result, "no variables here");
    }

    #[test]
    fn test_multiple_substitutions() {
        let lookup = test_lookup();
        let result = substitute_string("${TEST_KIND} at ${TEST_URL}", &lookup).unwrap();
        assert_eq!(result, "evm-rpc at http://localhost:8545");
    }

    #[test]
    fn test_toml_string_substitution() {
        let lookup = test_lookup();
        let mut value = toml::Value::String("${TEST_URL}".to_string());
        substitute_vars(&mut value, &lookup).unwrap();
        assert_eq!(value.as_str().unwrap(), "http://localhost:8545");
    }

    #[test]
    fn test_toml_table_substitution() {
        let lookup = test_lookup();

        let mut table = toml::map::Map::new();
        table.insert(
            "kind".to_string(),
            toml::Value::String("${TEST_KIND}".to_string()),
        );
        table.insert(
            "url".to_string(),
            toml::Value::String("${TEST_URL}".to_string()),
        );
        table.insert("port".to_string(), toml::Value::Integer(8545));

        let mut value = toml::Value::Table(table);
        substitute_vars(&mut value, &lookup).unwrap();

        let table = value.as_table().unwrap();
        assert_eq!(table.get("kind").unwrap().as_str().unwrap(), "evm-rpc");
        assert_eq!(
            table.get("url").unwrap().as_str().unwrap(),
            "http://localhost:8545"
        );
        assert_eq!(table.get("port").unwrap().as_integer().unwrap(), 8545);
    }

    #[test]
    fn test_toml_array_substitution() {
        let lookup = test_lookup();

        let array = vec![
            toml::Value::String("${TEST_HOST}".to_string()),
            toml::Value::String("127.0.0.1".to_string()),
        ];
        let mut value = toml::Value::Array(array);
        substitute_vars(&mut value, &lookup).unwrap();

        let array = value.as_array().unwrap();
        assert_eq!(array[0].as_str().unwrap(), "localhost");
        assert_eq!(array[1].as_str().unwrap(), "127.0.0.1");
    }

    #[test]
    fn test_missing_var() {
        let lookup = |_: &str| None;

        let result = substitute_string("${MISSING_VAR}", &lookup);
        assert!(result.is_err());

        match result.unwrap_err() {
            Error::MissingVar(var_name) => {
                assert_eq!(var_name, "MISSING_VAR");
            }
            other => panic!("Expected MissingVar error, got: {:?}", other),
        }
    }

    #[test]
    fn test_invalid_syntax_unclosed() {
        let lookup = test_lookup();
        let result = substitute_string("${UNCLOSED", &lookup);

        match result.unwrap_err() {
            Error::InvalidSyntax { position, message } => {
                assert_eq!(position, 0);
                assert!(message.contains("Unclosed variable"));
            }
            other => panic!("Expected InvalidSyntax error, got: {:?}", other),
        }
    }

    #[test]
    fn test_invalid_syntax_empty_var() {
        let lookup = test_lookup();
        let result = substitute_string("${}", &lookup);

        match result.unwrap_err() {
            Error::InvalidSyntax { position, message } => {
                assert_eq!(position, 0);
                assert!(message.contains("Empty variable name"));
            }
            other => panic!("Expected InvalidSyntax error, got: {:?}", other),
        }
    }

    #[test]
    fn test_env_vars_convenience_function() {
        // Test the convenience function that uses actual environment variables
        // We'll use a unique variable name to avoid conflicts
        let test_var = "AMP_TEST_UNIQUE_VAR_12345";

        unsafe {
            env::set_var(test_var, "test_value");
        }

        let mut value = toml::Value::String(format!("${{{}}}", test_var));
        let result = substitute_env_vars(&mut value);
        assert!(result.is_ok());
        assert_eq!(value.as_str().unwrap(), "test_value");

        unsafe {
            env::remove_var(test_var);
        }
    }

    #[test]
    fn test_escaped_dollar_not_supported() {
        // Our implementation doesn't support escaping, which is fine for our use case
        // Just document the behavior
        let lookup = |name: &str| {
            if name == "VAR" {
                Some("value".to_string())
            } else {
                None
            }
        };

        // This will try to substitute ${VAR}, not treat $$ as escaped
        let result = substitute_string("$$${VAR}", &lookup).unwrap();
        assert_eq!(result, "$$value"); // Both $ characters are preserved literally
    }
}
