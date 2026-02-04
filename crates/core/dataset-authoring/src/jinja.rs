//! Jinja2 SQL templating.
//!
//! Provides sandboxed Jinja rendering with `ref`, `source`, `var`, `env_var`,
//! and `this` helpers for deterministic SQL template expansion.
//!
//! # Determinism
//!
//! The Jinja environment is strictly sandboxed:
//! - No `include` or `import` statements
//! - No filesystem or network access
//! - Variable resolution is fully deterministic given the same context
//!
//! # Functions
//!
//! - `ref(alias, table)` / `source(alias, table)` - Returns `"<alias>.<table>"` for referencing
//!   dependency tables. Validates that the alias exists in the declared dependencies.
//! - `var(name, default=None)` - Returns variable value. Precedence: CLI `--var` > amp.yaml `vars` > default.
//!   Errors if missing without default.
//! - `env_var(name, default=None)` - Returns environment variable value. Precedence: env > default.
//!   Errors if missing without default.
//! - `this` - Global variable containing the current table name.
//!
//! # Example
//!
//! ```ignore
//! use dataset_authoring::jinja::{JinjaContext, render_sql};
//!
//! let ctx = JinjaContext::builder()
//!     .with_cli_var("network", "mainnet")
//!     .with_yaml_var("api_key", "default_key")
//!     .with_dep_alias("eth")
//!     .build();
//!
//! let sql = r#"
//! SELECT * FROM {{ ref("eth", "blocks") }}
//! WHERE network = '{{ var("network") }}'
//! "#;
//!
//! let rendered = render_sql(sql, "my_table", &ctx)?;
//! // SELECT * FROM eth.blocks WHERE network = 'mainnet'
//! ```

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use datasets_derived::deps::DepAlias;
use minijinja::{Environment, Error as JinjaError, ErrorKind, State};

/// Context for Jinja template rendering.
///
/// Contains all variables and dependency information needed to render SQL templates.
/// Construct using [`JinjaContextBuilder`].
#[derive(Debug, Clone)]
pub struct JinjaContext {
    /// Variables passed via CLI `--var` arguments (highest priority).
    cli_vars: BTreeMap<String, String>,
    /// Variables defined in amp.yaml `vars` section.
    yaml_vars: BTreeMap<String, String>,
    /// Set of valid dependency aliases from amp.yaml `dependencies`.
    dep_aliases: BTreeSet<DepAlias>,
}

impl JinjaContext {
    /// Create a new builder for constructing a `JinjaContext`.
    pub fn builder() -> JinjaContextBuilder {
        JinjaContextBuilder::default()
    }

    /// Get a variable value by precedence: CLI > YAML > default.
    ///
    /// Returns `None` if the variable is not found in either CLI or YAML vars.
    fn get_var(&self, name: &str) -> Option<&str> {
        self.cli_vars
            .get(name)
            .or_else(|| self.yaml_vars.get(name))
            .map(String::as_str)
    }

    /// Check if a dependency alias is valid.
    fn has_dep_alias(&self, alias: &str) -> bool {
        // Try to parse as DepAlias and check if it exists
        alias
            .parse::<DepAlias>()
            .is_ok_and(|a| self.dep_aliases.contains(&a))
    }
}

/// Builder for [`JinjaContext`].
#[derive(Debug, Default)]
pub struct JinjaContextBuilder {
    cli_vars: BTreeMap<String, String>,
    yaml_vars: BTreeMap<String, String>,
    dep_aliases: BTreeSet<DepAlias>,
}

impl JinjaContextBuilder {
    /// Add a CLI variable (highest priority).
    pub fn with_cli_var(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.cli_vars.insert(name.into(), value.into());
        self
    }

    /// Add multiple CLI variables.
    pub fn with_cli_vars(mut self, vars: impl IntoIterator<Item = (String, String)>) -> Self {
        self.cli_vars.extend(vars);
        self
    }

    /// Add a YAML variable (from amp.yaml `vars` section).
    pub fn with_yaml_var(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.yaml_vars.insert(name.into(), value.into());
        self
    }

    /// Add multiple YAML variables.
    pub fn with_yaml_vars(mut self, vars: impl IntoIterator<Item = (String, String)>) -> Self {
        self.yaml_vars.extend(vars);
        self
    }

    /// Add a valid dependency alias.
    pub fn with_dep_alias(mut self, alias: DepAlias) -> Self {
        self.dep_aliases.insert(alias);
        self
    }

    /// Add multiple dependency aliases.
    pub fn with_dep_aliases(mut self, aliases: impl IntoIterator<Item = DepAlias>) -> Self {
        self.dep_aliases.extend(aliases);
        self
    }

    /// Build the context.
    pub fn build(self) -> JinjaContext {
        JinjaContext {
            cli_vars: self.cli_vars,
            yaml_vars: self.yaml_vars,
            dep_aliases: self.dep_aliases,
        }
    }
}

/// Errors that occur during Jinja template rendering.
#[derive(Debug, thiserror::Error)]
pub enum RenderError {
    /// Template rendering failed.
    #[error("failed to render template")]
    Render {
        /// The underlying minijinja error.
        #[source]
        source: JinjaError,
    },

    /// Template compilation failed.
    #[error("invalid template syntax")]
    Template {
        /// The underlying minijinja error.
        #[source]
        source: JinjaError,
    },
}

/// Render a SQL template with the given context and table name.
///
/// The template has access to:
/// - `ref(alias, table)` - Reference a dependency table
/// - `source(alias, table)` - Alias for `ref`
/// - `var(name, default=None)` - Get a variable value
/// - `env_var(name, default=None)` - Get an environment variable
/// - `this` - The current table name
///
/// # Errors
///
/// Returns [`RenderError::Template`] if the template syntax is invalid.
/// Returns [`RenderError::Render`] if rendering fails (e.g., missing required variable).
pub fn render_sql(
    template: &str,
    table_name: &str,
    context: &JinjaContext,
) -> Result<String, RenderError> {
    let env = create_environment(context);

    // Add the template
    let compiled = env
        .template_from_str(template)
        .map_err(|source| RenderError::Template { source })?;

    // Create render context with `this` set to current table name
    let ctx = minijinja::context! {
        this => table_name,
    };

    compiled
        .render(ctx)
        .map_err(|source| RenderError::Render { source })
}

/// Create a sandboxed minijinja environment with custom functions.
///
/// The environment has auto-escape disabled (SQL doesn't need HTML escaping)
/// and only allows our deterministic helper functions.
fn create_environment(context: &JinjaContext) -> Environment<'static> {
    let mut env = Environment::new();

    // Disable auto-escaping for SQL templates
    env.set_auto_escape_callback(|_| minijinja::AutoEscape::None);

    // Register the `ref` function
    let ctx = Arc::new(context.clone());
    let ref_ctx = ctx.clone();
    env.add_function("ref", move |alias: &str, table: &str| -> Result<String, JinjaError> {
        if !ref_ctx.has_dep_alias(alias) {
            return Err(JinjaError::new(
                ErrorKind::InvalidOperation,
                format!("unknown dependency alias '{alias}'; must be declared in amp.yaml dependencies"),
            ));
        }
        Ok(format!("{alias}.{table}"))
    });

    // Register `source` as an alias for `ref`
    let source_ctx = ctx.clone();
    env.add_function(
        "source",
        move |alias: &str, table: &str| -> Result<String, JinjaError> {
            if !source_ctx.has_dep_alias(alias) {
                return Err(JinjaError::new(
                    ErrorKind::InvalidOperation,
                    format!(
                        "unknown dependency alias '{alias}'; must be declared in amp.yaml dependencies"
                    ),
                ));
            }
            Ok(format!("{alias}.{table}"))
        },
    );

    // Register the `var` function
    let var_ctx = ctx.clone();
    env.add_function(
        "var",
        move |_state: &State, name: &str, default: Option<&str>| -> Result<String, JinjaError> {
            match var_ctx.get_var(name) {
                Some(value) => Ok(value.to_string()),
                None => match default {
                    Some(d) => Ok(d.to_string()),
                    None => Err(JinjaError::new(
                        ErrorKind::UndefinedError,
                        format!("variable '{name}' is not defined and no default was provided"),
                    )),
                },
            }
        },
    );

    // Register the `env_var` function
    env.add_function(
        "env_var",
        |_state: &State, name: &str, default: Option<&str>| -> Result<String, JinjaError> {
            match std::env::var(name) {
                Ok(value) => Ok(value),
                Err(_) => match default {
                    Some(d) => Ok(d.to_string()),
                    None => Err(JinjaError::new(
                        ErrorKind::UndefinedError,
                        format!(
                            "environment variable '{name}' is not set and no default was provided"
                        ),
                    )),
                },
            }
        },
    );

    env
}

#[cfg(test)]
mod tests {
    use super::*;

    mod ref_function {
        use super::*;

        #[test]
        fn ref_with_valid_alias_returns_qualified_name() {
            //* Given
            let ctx = JinjaContext::builder()
                .with_dep_alias("eth".parse().expect("valid alias"))
                .build();
            let template = r#"{{ ref("eth", "blocks") }}"#;

            //* When
            let result = render_sql(template, "test_table", &ctx);

            //* Then
            let rendered = result.expect("should render successfully");
            assert_eq!(rendered, "eth.blocks");
        }

        #[test]
        fn ref_with_invalid_alias_fails() {
            //* Given
            let ctx = JinjaContext::builder().build();
            let template = r#"{{ ref("unknown", "blocks") }}"#;

            //* When
            let result = render_sql(template, "test_table", &ctx);

            //* Then
            let err = result.expect_err("should fail with unknown alias");
            assert!(matches!(err, RenderError::Render { .. }));
            let message = err.to_string();
            assert!(
                message.contains("render"),
                "error should be about rendering: {message}"
            );
        }

        #[test]
        fn source_is_alias_for_ref() {
            //* Given
            let ctx = JinjaContext::builder()
                .with_dep_alias("eth".parse().expect("valid alias"))
                .build();
            let template = r#"{{ source("eth", "transactions") }}"#;

            //* When
            let result = render_sql(template, "test_table", &ctx);

            //* Then
            let rendered = result.expect("should render successfully");
            assert_eq!(rendered, "eth.transactions");
        }

        #[test]
        fn source_with_invalid_alias_fails() {
            //* Given
            let ctx = JinjaContext::builder().build();
            let template = r#"{{ source("missing", "table") }}"#;

            //* When
            let result = render_sql(template, "test_table", &ctx);

            //* Then
            let err = result.expect_err("should fail with unknown alias");
            assert!(matches!(err, RenderError::Render { .. }));
        }
    }

    mod var_function {
        use super::*;

        #[test]
        fn var_with_cli_value_returns_cli_value() {
            //* Given
            let ctx = JinjaContext::builder()
                .with_cli_var("network", "mainnet")
                .with_yaml_var("network", "testnet")
                .build();
            let template = r#"{{ var("network") }}"#;

            //* When
            let result = render_sql(template, "test_table", &ctx);

            //* Then
            let rendered = result.expect("should render successfully");
            assert_eq!(rendered, "mainnet", "CLI should take precedence over YAML");
        }

        #[test]
        fn var_with_yaml_value_returns_yaml_value() {
            //* Given
            let ctx = JinjaContext::builder()
                .with_yaml_var("api_key", "default_key")
                .build();
            let template = r#"{{ var("api_key") }}"#;

            //* When
            let result = render_sql(template, "test_table", &ctx);

            //* Then
            let rendered = result.expect("should render successfully");
            assert_eq!(rendered, "default_key");
        }

        #[test]
        fn var_with_default_returns_default_when_missing() {
            //* Given
            let ctx = JinjaContext::builder().build();
            let template = r#"{{ var("missing", "fallback") }}"#;

            //* When
            let result = render_sql(template, "test_table", &ctx);

            //* Then
            let rendered = result.expect("should render successfully");
            assert_eq!(rendered, "fallback");
        }

        #[test]
        fn var_without_default_fails_when_missing() {
            //* Given
            let ctx = JinjaContext::builder().build();
            let template = r#"{{ var("missing") }}"#;

            //* When
            let result = render_sql(template, "test_table", &ctx);

            //* Then
            let err = result.expect_err("should fail with missing var");
            assert!(matches!(err, RenderError::Render { .. }));
        }

        #[test]
        fn var_precedence_cli_over_yaml_over_default() {
            //* Given - test that CLI wins over YAML
            let ctx = JinjaContext::builder()
                .with_cli_var("priority_test", "cli_wins")
                .with_yaml_var("priority_test", "yaml_value")
                .build();
            let template = r#"{{ var("priority_test", "default_value") }}"#;

            //* When
            let result = render_sql(template, "test_table", &ctx);

            //* Then
            let rendered = result.expect("should render successfully");
            assert_eq!(rendered, "cli_wins");
        }

        #[test]
        fn var_precedence_yaml_over_default() {
            //* Given - YAML present, no CLI
            let ctx = JinjaContext::builder()
                .with_yaml_var("yaml_only", "yaml_value")
                .build();
            let template = r#"{{ var("yaml_only", "default_value") }}"#;

            //* When
            let result = render_sql(template, "test_table", &ctx);

            //* Then
            let rendered = result.expect("should render successfully");
            assert_eq!(rendered, "yaml_value");
        }
    }

    mod env_var_function {
        use super::*;

        #[test]
        fn env_var_with_set_value_returns_value() {
            //* Given
            // Use a var that should always be set
            let template = r#"{{ env_var("PATH", "not_set") }}"#;
            let ctx = JinjaContext::builder().build();

            //* When
            let result = render_sql(template, "test_table", &ctx);

            //* Then
            let rendered = result.expect("should render successfully");
            assert_ne!(rendered, "not_set", "PATH should be set in environment");
        }

        #[test]
        fn env_var_with_default_returns_default_when_unset() {
            //* Given
            let template =
                r#"{{ env_var("AMP_NONEXISTENT_VAR_FOR_TESTING_12345", "fallback_default") }}"#;
            let ctx = JinjaContext::builder().build();

            //* When
            let result = render_sql(template, "test_table", &ctx);

            //* Then
            let rendered = result.expect("should render successfully");
            assert_eq!(rendered, "fallback_default");
        }

        #[test]
        fn env_var_without_default_fails_when_unset() {
            //* Given
            let template = r#"{{ env_var("AMP_NONEXISTENT_VAR_FOR_TESTING_12345") }}"#;
            let ctx = JinjaContext::builder().build();

            //* When
            let result = render_sql(template, "test_table", &ctx);

            //* Then
            let err = result.expect_err("should fail with missing env var");
            assert!(matches!(err, RenderError::Render { .. }));
        }
    }

    mod this_variable {
        use super::*;

        #[test]
        fn this_returns_current_table_name() {
            //* Given
            let ctx = JinjaContext::builder().build();
            let template = r#"CREATE TABLE {{ this }} AS SELECT 1"#;

            //* When
            let result = render_sql(template, "my_table", &ctx);

            //* Then
            let rendered = result.expect("should render successfully");
            assert_eq!(rendered, "CREATE TABLE my_table AS SELECT 1");
        }

        #[test]
        fn this_works_in_complex_template() {
            //* Given
            let ctx = JinjaContext::builder()
                .with_dep_alias("eth".parse().expect("valid alias"))
                .build();
            let template = r#"CREATE TABLE {{ this }} AS SELECT * FROM {{ ref("eth", "blocks") }} WHERE table_name = '{{ this }}'"#;

            //* When
            let result = render_sql(template, "block_stats", &ctx);

            //* Then
            let rendered = result.expect("should render successfully");
            assert_eq!(
                rendered,
                "CREATE TABLE block_stats AS SELECT * FROM eth.blocks WHERE table_name = 'block_stats'"
            );
        }
    }

    mod template_syntax {
        use super::*;

        #[test]
        fn invalid_template_syntax_fails() {
            //* Given
            let ctx = JinjaContext::builder().build();
            let template = r#"{{ unclosed"#;

            //* When
            let result = render_sql(template, "test_table", &ctx);

            //* Then
            let err = result.expect_err("should fail with invalid syntax");
            assert!(matches!(err, RenderError::Template { .. }));
        }

        #[test]
        fn plain_sql_without_jinja_passes_through() {
            //* Given
            let ctx = JinjaContext::builder().build();
            let template = "SELECT * FROM some_table WHERE id = 1";

            //* When
            let result = render_sql(template, "test_table", &ctx);

            //* Then
            let rendered = result.expect("should render successfully");
            assert_eq!(rendered, template);
        }

        #[test]
        fn multiline_template_renders_correctly() {
            //* Given
            let ctx = JinjaContext::builder()
                .with_dep_alias("eth".parse().expect("valid alias"))
                .with_cli_var("min_value", "1000")
                .build();
            let template = r#"SELECT
    block_number,
    tx_hash
FROM {{ ref("eth", "transactions") }}
WHERE value > {{ var("min_value") }}"#;

            //* When
            let result = render_sql(template, "test_table", &ctx);

            //* Then
            let rendered = result.expect("should render successfully");
            let expected = r#"SELECT
    block_number,
    tx_hash
FROM eth.transactions
WHERE value > 1000"#;
            assert_eq!(rendered, expected);
        }
    }

    mod context_builder {
        use super::*;

        #[test]
        fn builder_accepts_multiple_deps() {
            //* Given
            let aliases = ["eth", "polygon", "base"]
                .iter()
                .map(|s| s.parse::<DepAlias>().expect("valid alias"));

            //* When
            let ctx = JinjaContext::builder().with_dep_aliases(aliases).build();

            //* Then
            assert!(ctx.has_dep_alias("eth"));
            assert!(ctx.has_dep_alias("polygon"));
            assert!(ctx.has_dep_alias("base"));
            assert!(!ctx.has_dep_alias("unknown"));
        }

        #[test]
        fn builder_accepts_multiple_cli_vars() {
            //* Given
            let vars = vec![
                ("var1".to_string(), "value1".to_string()),
                ("var2".to_string(), "value2".to_string()),
            ];

            //* When
            let ctx = JinjaContext::builder().with_cli_vars(vars).build();
            let template = r#"{{ var("var1") }}-{{ var("var2") }}"#;
            let result = render_sql(template, "test", &ctx);

            //* Then
            let rendered = result.expect("should render successfully");
            assert_eq!(rendered, "value1-value2");
        }

        #[test]
        fn builder_accepts_multiple_yaml_vars() {
            //* Given
            let vars = vec![
                ("yaml1".to_string(), "yval1".to_string()),
                ("yaml2".to_string(), "yval2".to_string()),
            ];

            //* When
            let ctx = JinjaContext::builder().with_yaml_vars(vars).build();
            let template = r#"{{ var("yaml1") }}-{{ var("yaml2") }}"#;
            let result = render_sql(template, "test", &ctx);

            //* Then
            let rendered = result.expect("should render successfully");
            assert_eq!(rendered, "yval1-yval2");
        }
    }

    mod determinism {
        use super::*;

        #[test]
        fn rendering_is_deterministic() {
            //* Given
            let ctx = JinjaContext::builder()
                .with_dep_alias("eth".parse().expect("valid alias"))
                .with_cli_var("network", "mainnet")
                .with_yaml_var("default_limit", "100")
                .build();
            let template = r#"SELECT * FROM {{ ref("eth", "blocks") }} WHERE network = '{{ var("network") }}' LIMIT {{ var("default_limit") }}"#;

            //* When - render multiple times
            let results: Vec<_> = (0..5)
                .map(|_| render_sql(template, "test_table", &ctx))
                .collect();

            //* Then
            let first = results[0]
                .as_ref()
                .expect("should render successfully")
                .clone();
            for result in &results[1..] {
                let rendered = result.as_ref().expect("should render successfully");
                assert_eq!(&first, rendered, "all renders should be identical");
            }
        }
    }
}
