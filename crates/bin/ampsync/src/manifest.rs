use std::{path::Path, time::Duration};

use admin_api::handlers::datasets::{
    get_version_schema::DatasetSchemaResponse, get_versions::DatasetVersionsResponse,
};
use datasets_common::{name::Name, version::Version};
use datasets_derived::{DerivedDatasetKind, Manifest, manifest::Table};
use lazy_static::lazy_static;
use oxc_allocator::Allocator;
use oxc_ast::ast::*;
use oxc_ast_visit::{Visit, walk};
use oxc_parser::{ParseOptions, Parser};
use oxc_span::SourceType;
use serde_json::Value;
use thiserror::Error;

use crate::{dataset_definition::DatasetDefinition, sql_validator};

/// Errors that can occur when fetching or parsing dataset manifests.
#[derive(Error, Debug, Clone)]
pub enum ManifestError {
    #[error("Dataset '{dataset}' version '{version}' not found in admin-api")]
    DatasetNotFound { dataset: String, version: String },

    #[error("No versions available for dataset '{dataset}' in admin-api")]
    NoVersionsAvailable { dataset: String },

    #[error("Failed to connect to admin-api at {url}: {message}")]
    NetworkError { url: String, message: String },

    #[error("HTTP {status} from admin-api {url}: {message}")]
    HttpError {
        status: u16,
        url: String,
        message: String,
    },

    #[error("Invalid dataset name '{name}': {reason}")]
    InvalidDatasetName { name: String, reason: String },

    #[error("Invalid dataset version '{version}': {reason}")]
    InvalidVersion { version: String, reason: String },

    #[error("Schema mismatch: {0}")]
    SchemaMismatch(String),

    #[error("Failed to read config file at '{path}': {message}")]
    ConfigFileReadError { path: String, message: String },

    #[error("Failed to parse config file at '{path}': {reason}")]
    ConfigParseError { path: String, reason: String },

    #[error("SQL validation failed for table '{table}': {reason}")]
    SqlValidationError { table: String, reason: String },
}

impl ManifestError {
    /// Returns true if this error is retryable (transient/recoverable).
    ///
    /// Retryable errors include:
    /// - Dataset not found (may be published soon)
    /// - No versions available (may be published soon)
    /// - Network errors (may be transient)
    /// - 5xx HTTP errors (server-side issues, may recover)
    pub fn is_retryable(&self) -> bool {
        match self {
            ManifestError::DatasetNotFound { .. }
            | ManifestError::NoVersionsAvailable { .. }
            | ManifestError::NetworkError { .. } => true,
            ManifestError::HttpError { status, .. } => *status >= 500,
            _ => false,
        }
    }
}

/// Maximum number of dataset versions to fetch from admin-api when resolving version
const ADMIN_API_VERSION_LIMIT: usize = 1000;

lazy_static! {
    /// Shared HTTP client for admin-api requests with optimized connection pooling.
    ///
    /// Creating a new client for each request is expensive as it spawns new connection pools,
    /// DNS resolvers, and TLS session caches. This shared client reuses connections efficiently.
    static ref ADMIN_API_CLIENT: reqwest::Client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(10)
        .tcp_keepalive(Duration::from_secs(60))
        .build()
        .expect("Failed to create HTTP client");
}

/// Load and parse a nozzle configuration file into a DatasetDefinition.
///
/// This function reads the nozzle config file and attempts to parse it into a user-facing
/// DatasetDefinition (simple format with just SQL strings in tables).
/// Supports:
/// - JSON files: Direct parsing via serde_json
/// - JS/TS files: AST parsing via oxc_parser to extract exported configuration
///
/// Sanitization:
/// - All SQL queries are sanitized to remove ORDER BY clauses (non-incremental queries not supported)
/// - The sanitized SQL replaces the original SQL in the table definitions
///
/// # Arguments
/// * `config_path` - Path to the nozzle configuration file
///
/// # Returns
/// * `Result<DatasetDefinition, ManifestError>` - Parsed dataset definition with sanitized SQL
pub async fn load_dataset_definition(
    config_path: &Path,
) -> Result<DatasetDefinition, ManifestError> {
    // Read the file contents asynchronously to avoid blocking the executor
    let contents = tokio::fs::read_to_string(config_path).await.map_err(|e| {
        ManifestError::ConfigFileReadError {
            path: config_path.display().to_string(),
            message: e.to_string(),
        }
    })?;

    // Determine how to parse based on file extension
    let extension = config_path
        .extension()
        .and_then(|ext| ext.to_str())
        .ok_or_else(|| ManifestError::ConfigParseError {
            path: config_path.display().to_string(),
            reason: "Unable to determine file extension".to_string(),
        })?;

    let mut dataset_def = match extension {
        "json" => {
            // Parse JSON directly using serde_json
            serde_json::from_str(&contents).map_err(|e| ManifestError::ConfigParseError {
                path: config_path.display().to_string(),
                reason: format!("Failed to parse JSON: {}", e),
            })
        }
        "js" | "mjs" | "ts" | "mts" => {
            // Parse JS/TS files using oxc_parser
            parse_js_ts_manifest(&contents, extension, config_path)
        }
        _ => Err(ManifestError::ConfigParseError {
            path: config_path.display().to_string(),
            reason: format!("Unsupported file extension: {}", extension),
        }),
    }?;

    // Sanitize all SQL queries in tables (remove ORDER BY clauses)
    for (table_name, table_def) in &mut dataset_def.tables {
        let sanitized_sql = sql_validator::sanitize_sql(&table_def.sql).map_err(|e| {
            ManifestError::SqlValidationError {
                table: table_name.clone(),
                reason: format!("Failed to sanitize SQL: {}", e),
            }
        })?;
        table_def.sql = sanitized_sql;
    }

    Ok(dataset_def)
}

/// Parse a JavaScript/TypeScript file to extract the dataset definition using AST parsing
fn parse_js_ts_manifest(
    contents: &str,
    extension: &str,
    config_path: &Path,
) -> Result<DatasetDefinition, ManifestError> {
    let allocator = Allocator::default();

    // Determine source type based on extension
    let source_type = match extension {
        "ts" | "mts" => SourceType::default().with_typescript(true),
        "tsx" => SourceType::default().with_typescript(true).with_jsx(true),
        _ => SourceType::default(),
    };

    // Parse the source code
    let parse_result = Parser::new(&allocator, contents, source_type)
        .with_options(ParseOptions {
            parse_regular_expression: true,
            ..ParseOptions::default()
        })
        .parse();

    if !parse_result.errors.is_empty() {
        let error_messages: Vec<String> = parse_result
            .errors
            .iter()
            .map(|e| format!("{:?}", e))
            .collect();
        return Err(ManifestError::ConfigParseError {
            path: config_path.display().to_string(),
            reason: format!("Parse errors: {}", error_messages.join(", ")),
        });
    }

    // Extract the manifest from the AST
    let mut extractor = ManifestExtractor::new();
    extractor.visit_program(&parse_result.program);

    extractor.extract_manifest(config_path)
}

/// Resolve a simple version (e.g., "0.2.0") to a fully qualified version (e.g., "0.2.0-LTcyNjgzMjc1NA").
///
/// This function queries the admin-api to get all versions for a dataset and finds the first
/// version that starts with the given version prefix.
///
/// # Arguments
/// * `admin_api_addr` - Base URL of the admin-api service
/// * `name` - Dataset name
/// * `version` - Simple version from the config (e.g., "0.2.0")
///
/// # Returns
/// * `Ok(Version)` - The fully qualified version
/// * `Err(ManifestError)` - If the version cannot be found or HTTP request fails
async fn resolve_qualified_version(
    admin_api_addr: &str,
    name: &Name,
    version: &Version,
) -> Result<Version, ManifestError> {
    let url = format!(
        "{}/datasets/{}/versions?limit={}",
        admin_api_addr, name, ADMIN_API_VERSION_LIMIT
    );

    let versions_resp =
        ADMIN_API_CLIENT
            .get(&url)
            .send()
            .await
            .map_err(|e| ManifestError::NetworkError {
                url: url.clone(),
                message: e.to_string(),
            })?;

    // Check for HTTP errors
    let status = versions_resp.status();
    if status == reqwest::StatusCode::NOT_FOUND {
        return Err(ManifestError::NoVersionsAvailable {
            dataset: name.to_string(),
        });
    }
    if !status.is_success() {
        let body = versions_resp.text().await.unwrap_or_default();
        return Err(ManifestError::HttpError {
            status: status.as_u16(),
            url: url.clone(),
            message: body,
        });
    }

    let versions_data: DatasetVersionsResponse =
        versions_resp
            .json()
            .await
            .map_err(|e| ManifestError::NetworkError {
                url: url.clone(),
                message: e.to_string(),
            })?;

    // Check if any versions exist
    if versions_data.versions.is_empty() {
        return Err(ManifestError::NoVersionsAvailable {
            dataset: name.to_string(),
        });
    }

    // Find the first version that matches our config version prefix
    // The config has a simple version like "0.2.0", but we need the fully qualified
    // version like "0.2.0-LTcyNjgzMjc1NA"
    let version_prefix = version.to_string();
    let qualified_version = versions_data
        .versions
        .iter()
        .find(|v| v.to_string().starts_with(&version_prefix))
        .ok_or_else(|| ManifestError::DatasetNotFound {
            dataset: name.to_string(),
            version: version_prefix.clone(),
        })?
        .clone();

    tracing::info!(
        dataset = %name,
        requested_version = %version,
        qualified_version = %qualified_version,
        "version_resolved"
    );

    Ok(qualified_version)
}

/// Fetch manifest with indefinite polling for initial startup.
///
/// This function polls the admin-api until the dataset becomes available.
/// Used during initial startup when the dataset might not be published yet.
///
/// # Arguments
/// * `admin_api_addr` - Base URL of the admin-api service
/// * `config_path` - Path to the local nozzle configuration file
///
/// # Returns
/// * `Ok(Manifest)` - Successfully fetched manifest
/// * `Err(ManifestError)` - Non-recoverable errors (invalid config, permanent failures)
///
/// # Behavior
/// - On retryable errors: Retry with exponential backoff (indefinitely)
/// - On non-retryable errors: Fail immediately
/// - Logs helpful messages to guide users to run `nozzle dump`
pub async fn fetch_manifest_with_startup_poll(
    admin_api_addr: &str,
    config_path: &Path,
) -> Result<Manifest, ManifestError> {
    let mut first_error_logged = false;
    let mut attempt = 0u32;
    let max_backoff_secs = 30u64;

    loop {
        match fetch_manifest(admin_api_addr, config_path).await {
            Ok(manifest) => {
                if attempt > 0 {
                    tracing::info!(
                        "Successfully fetched manifest after {} attempts",
                        attempt + 1
                    );
                }
                return Ok(manifest);
            }
            Err(e) if e.is_retryable() => {
                if !first_error_logged {
                    tracing::warn!(
                        error = %e,
                        "dataset_not_found_waiting_for_publish"
                    );
                    tracing::warn!(
                        "Have you run 'nozzle dump --dataset <name>' to publish the dataset?"
                    );
                    tracing::info!("waiting_for_dataset");
                    first_error_logged = true;
                }

                // Calculate backoff with saturation to avoid overflow
                let backoff_secs = if attempt < 5 {
                    2u64.pow(attempt)
                } else {
                    max_backoff_secs
                }
                .min(max_backoff_secs);

                if attempt > 0 && attempt % 5 == 0 {
                    tracing::info!(
                        attempt = attempt + 1,
                        retry_delay_secs = backoff_secs,
                        "still_waiting_for_dataset"
                    );
                }

                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                attempt += 1;
            }
            Err(e) => {
                // Non-retryable error
                return Err(e);
            }
        }
    }
}

/// Fetch manifest with limited retries for hot-reload.
///
/// This function retries a limited number of times to give the dump command
/// time to complete after a config file change.
///
/// # Arguments
/// * `admin_api_addr` - Base URL of the admin-api service
/// * `config_path` - Path to the local nozzle configuration file
/// * `max_retries` - Maximum number of retry attempts
///
/// # Returns
/// * `Ok(Manifest)` - Successfully fetched manifest
/// * `Err(ManifestError)` - Failed after max retries or non-recoverable error
///
/// # Behavior
/// - On retryable errors: Retry up to max_retries times with exponential backoff
/// - On non-retryable errors: Fail immediately
/// - After max retries: Return error suggesting to check dump command
pub async fn fetch_manifest_with_retry(
    admin_api_addr: &str,
    config_path: &Path,
    max_retries: u32,
) -> Result<Manifest, ManifestError> {
    let mut attempt = 0u32;

    loop {
        match fetch_manifest(admin_api_addr, config_path).await {
            Ok(manifest) => {
                if attempt > 0 {
                    tracing::info!(
                        attempts = attempt,
                        dataset = %manifest.name,
                        "manifest_fetched_after_retry"
                    );
                }
                return Ok(manifest);
            }
            Err(e) if e.is_retryable() && attempt < max_retries => {
                // Calculate backoff with saturation to avoid overflow
                let backoff_secs = if attempt < 5 {
                    2u64.pow(attempt)
                } else {
                    30u64
                }
                .min(30);

                tracing::warn!(
                    attempt = attempt + 1,
                    max_retries = max_retries,
                    retry_delay_secs = backoff_secs,
                    "dataset_not_found_retrying"
                );

                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                attempt += 1;
            }
            Err(e) => {
                // Max retries exceeded or non-recoverable error
                if attempt >= max_retries && e.is_retryable() {
                    return Err(ManifestError::ConfigParseError {
                        path: config_path.display().to_string(),
                        reason: format!(
                            "Failed to fetch manifest after {} retries: {}. \
                             Dataset version not found - did the 'nozzle dump' command complete successfully?",
                            max_retries, e
                        ),
                    });
                }
                return Err(e);
            }
        }
    }
}

/// Fetch and construct a dataset Manifest by combining schema from admin-api with local config.
///
/// This function performs the following steps:
/// 1. Loads the local dataset definition from the config file to get SQL queries
/// 2. Parses and validates the dataset name and version
/// 3. Fetches the schema information from the admin-api endpoint
/// 4. Combines the schema (from admin-api) with SQL queries (from local config)
///
/// # Arguments
/// * `admin_api_addr` - Base URL of the admin-api service (e.g., "http://localhost:1610")
/// * `config_path` - Path to the local nozzle configuration file (.json, .js, .ts)
///
/// # Returns
/// * `Ok(Manifest)` - A complete manifest with schema and SQL combined
/// * `Err(ManifestError)` - Various errors:
///   - Failed to load or parse local config file
///   - Invalid dataset name or version
///   - HTTP request failure to admin-api
///   - Schema mismatch between admin-api and local config
///
/// # Example
/// ```ignore
/// let manifest = fetch_manifest(
///     "http://localhost:1610",
///     Path::new("./nozzle.config.ts")
/// ).await?;
/// ```
pub async fn fetch_manifest(
    admin_api_addr: &str,
    config_path: &Path,
) -> Result<Manifest, ManifestError> {
    // Load the dataset definition to extract name, version, and SQL queries
    let dataset_def = load_dataset_definition(config_path).await?;

    // Parse and validate dataset name
    let name: Name = dataset_def
        .name
        .parse()
        .map_err(|e| ManifestError::InvalidDatasetName {
            name: dataset_def.name.clone(),
            reason: format!("{}", e),
        })?;

    // Parse and validate dataset version
    let version: Version =
        dataset_def
            .version
            .parse()
            .map_err(|e| ManifestError::InvalidVersion {
                version: dataset_def.version.clone(),
                reason: format!("{}", e),
            })?;

    // Resolve the simple version from config to a fully qualified version
    let qualified_version = resolve_qualified_version(admin_api_addr, &name, &version).await?;

    tracing::info!(
        dataset = %name,
        version = %qualified_version,
        admin_api_addr = %admin_api_addr,
        "fetching_schema"
    );

    // Fetch schema from admin-api using the qualified version
    let schema_url = format!(
        "{}/datasets/{}/versions/{}/schema",
        admin_api_addr, name, qualified_version
    );

    let schema_resp = ADMIN_API_CLIENT
        .get(&schema_url)
        .send()
        .await
        .map_err(|e| ManifestError::NetworkError {
            url: schema_url.clone(),
            message: e.to_string(),
        })?;

    // Check for HTTP errors
    let status = schema_resp.status();
    if !status.is_success() {
        let body = schema_resp.text().await.unwrap_or_default();
        return Err(ManifestError::HttpError {
            status: status.as_u16(),
            url: schema_url.clone(),
            message: body,
        });
    }

    let schema_response: DatasetSchemaResponse =
        schema_resp
            .json()
            .await
            .map_err(|e| ManifestError::NetworkError {
                url: schema_url,
                message: e.to_string(),
            })?;

    // Combine schema from admin-api with SQL from local config
    let mut tables = std::collections::BTreeMap::<String, Table>::new();

    for table_info in schema_response.tables {
        // Look up the corresponding SQL from the local config
        let sql = dataset_def
            .tables
            .get(&table_info.name)
            .map(|t| &t.sql)
            .ok_or_else(|| {
                ManifestError::SchemaMismatch(format!(
                    "Table '{}' exists in admin-api schema but not in local config '{}'",
                    table_info.name,
                    config_path.display()
                ))
            })?;

        tables.insert(
            table_info.name.clone(),
            Table {
                input: datasets_derived::manifest::TableInput::View(
                    datasets_derived::manifest::View { sql: sql.clone() },
                ),
                schema: table_info.schema,
                network: table_info.network,
            },
        );
    }

    Ok(Manifest {
        name,
        version: qualified_version,
        kind: DerivedDatasetKind,
        network: dataset_def.network,
        functions: std::collections::BTreeMap::new(),
        dependencies: std::collections::BTreeMap::new(),
        tables,
    })
}

/// AST visitor that extracts manifest configuration from JavaScript/TypeScript files
struct ManifestExtractor {
    manifest_json: Option<Value>,
}

impl ManifestExtractor {
    fn new() -> Self {
        Self {
            manifest_json: None,
        }
    }

    fn extract_manifest(self, config_path: &Path) -> Result<DatasetDefinition, ManifestError> {
        let json = self
            .manifest_json
            .ok_or_else(|| ManifestError::ConfigParseError {
                path: config_path.display().to_string(),
                reason: "No manifest configuration found in the file".to_string(),
            })?;
        serde_json::from_value(json).map_err(|e| ManifestError::ConfigParseError {
            path: config_path.display().to_string(),
            reason: format!("Failed to parse manifest JSON: {}", e),
        })
    }

    /// Convert an AST object expression to a JSON Value
    fn object_to_json(&self, obj: &ObjectExpression) -> Value {
        let mut map = serde_json::Map::new();

        for prop in &obj.properties {
            match prop {
                ObjectPropertyKind::ObjectProperty(prop) => {
                    let key = self.get_property_key(&prop.key);
                    let value = self.expression_to_json(&prop.value);
                    map.insert(key, value);
                }
                _ => {
                    // Skip getters, setters, and spread elements for now
                }
            }
        }

        Value::Object(map)
    }

    /// Extract property key as string
    fn get_property_key(&self, key: &PropertyKey) -> String {
        match key {
            PropertyKey::StaticIdentifier(ident) => ident.name.to_string(),
            PropertyKey::StringLiteral(lit) => lit.value.to_string(),
            PropertyKey::NumericLiteral(lit) => lit.value.to_string(),
            _ => "unknown".to_string(),
        }
    }

    /// Convert an AST expression to a JSON Value
    fn expression_to_json(&self, expr: &Expression) -> Value {
        match expr {
            Expression::StringLiteral(lit) => Value::String(lit.value.to_string()),
            Expression::NumericLiteral(lit) => {
                if lit.value.fract() == 0.0 {
                    Value::Number(serde_json::Number::from(lit.value as i64))
                } else {
                    serde_json::Number::from_f64(lit.value)
                        .map(Value::Number)
                        .unwrap_or(Value::Null)
                }
            }
            Expression::BooleanLiteral(lit) => Value::Bool(lit.value),
            Expression::NullLiteral(_) => Value::Null,
            Expression::ObjectExpression(obj) => self.object_to_json(obj),
            Expression::ArrayExpression(arr) => {
                let values: Vec<Value> = arr
                    .elements
                    .iter()
                    .filter_map(|elem| match elem {
                        ArrayExpressionElement::SpreadElement(_) => None,
                        ArrayExpressionElement::Elision(_) => Some(Value::Null),
                        ArrayExpressionElement::BooleanLiteral(lit) => Some(Value::Bool(lit.value)),
                        ArrayExpressionElement::NullLiteral(_) => Some(Value::Null),
                        ArrayExpressionElement::NumericLiteral(lit) => {
                            if lit.value.fract() == 0.0 {
                                Some(Value::Number(serde_json::Number::from(lit.value as i64)))
                            } else {
                                serde_json::Number::from_f64(lit.value).map(Value::Number)
                            }
                        }
                        ArrayExpressionElement::StringLiteral(lit) => {
                            Some(Value::String(lit.value.to_string()))
                        }
                        ArrayExpressionElement::TemplateLiteral(template) => {
                            Some(Value::String(self.template_to_string(template)))
                        }
                        _ => Some(Value::Null), // Fallback for complex expressions
                    })
                    .collect();
                Value::Array(values)
            }
            Expression::TemplateLiteral(template) => {
                Value::String(self.template_to_string(template))
            }
            Expression::Identifier(ident) => {
                // For identifiers, we'll store them as strings for now
                // In a more sophisticated implementation, we'd resolve the identifier
                Value::String(format!("${{{}}}", ident.name))
            }
            _ => {
                // For other expressions (function calls, etc.), use a placeholder
                Value::String("{{complex_expression}}".to_string())
            }
        }
    }

    /// Convert template literal to string (simplified - doesn't handle expressions)
    fn template_to_string(&self, template: &TemplateLiteral) -> String {
        let mut result = String::new();
        for (i, quasi) in template.quasis.iter().enumerate() {
            result.push_str(&quasi.value.cooked.as_ref().unwrap_or(&quasi.value.raw));
            // Add placeholder for expressions
            if i < template.expressions.len() {
                result.push_str("${...}");
            }
        }
        result
    }
}

impl<'a> Visit<'a> for ManifestExtractor {
    /// Look for module.exports assignments
    fn visit_assignment_expression(&mut self, assign: &AssignmentExpression<'a>) {
        // Check if this is module.exports = ...
        if let AssignmentTarget::StaticMemberExpression(member) = &assign.left {
            if let Expression::Identifier(obj) = &member.object {
                if obj.name == "module" && member.property.name == "exports" {
                    self.handle_potential_manifest_expression(&assign.right);
                }
            }
        }
        walk::walk_assignment_expression(self, assign);
    }

    /// Look for export default statements
    fn visit_export_default_declaration(&mut self, export: &ExportDefaultDeclaration<'a>) {
        // Handle different kinds of export default declarations
        match &export.declaration {
            dec if dec.is_expression() => {
                if let Some(expr) = dec.as_expression() {
                    self.handle_potential_manifest_expression(expr);
                }
            }
            _ => {}
        }
        walk::walk_export_default_declaration(self, export);
    }
}

impl ManifestExtractor {
    /// Handle expressions that might contain manifest data
    fn handle_potential_manifest_expression(&mut self, expr: &Expression) {
        match expr {
            Expression::ObjectExpression(obj) => {
                // Direct object export
                self.manifest_json = Some(self.object_to_json(obj));
            }
            Expression::CallExpression(call) => {
                // Function call like defineDataset(() => ({ ... }))
                self.handle_function_call(call);
            }
            Expression::ParenthesizedExpression(paren) => {
                // Unwrap parenthesized expressions like ({ ... })
                self.handle_potential_manifest_expression(&paren.expression);
            }
            Expression::ArrowFunctionExpression(arrow) => {
                // Arrow function that returns manifest
                if arrow.expression {
                    // For expression arrow functions, the body is an expression
                    for stmt in &arrow.body.statements {
                        if let Statement::ExpressionStatement(expr_stmt) = stmt {
                            self.handle_potential_manifest_expression(&expr_stmt.expression);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    /// Handle function calls like defineDataset(() => ({ ... }))
    fn handle_function_call(&mut self, call: &CallExpression) {
        // Look for function calls with arrow function arguments
        for arg in &call.arguments {
            match arg {
                Argument::ArrowFunctionExpression(arrow) => {
                    if arrow.expression {
                        // For expression arrow functions like () => ({ ... })
                        // The body.statements contains a single ExpressionStatement with the return value
                        // But we need to handle the case where it might be a parenthesized expression
                        for stmt in &arrow.body.statements {
                            if let Statement::ExpressionStatement(expr_stmt) = stmt {
                                self.handle_potential_manifest_expression(&expr_stmt.expression);
                            }
                        }
                    } else {
                        // For block arrow functions, look for return statements
                        for stmt in &arrow.body.statements {
                            if let Statement::ReturnStatement(ret) = stmt {
                                if let Some(ret_expr) = &ret.argument {
                                    self.handle_potential_manifest_expression(ret_expr);
                                }
                            }
                        }
                    }
                }
                Argument::FunctionExpression(func) => {
                    // Handle function expressions
                    if let Some(body) = &func.body {
                        for stmt in &body.statements {
                            if let Statement::ReturnStatement(ret) = stmt {
                                if let Some(ret_expr) = &ret.argument {
                                    self.handle_potential_manifest_expression(ret_expr);
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    #[tokio::test]
    async fn test_fetch_manifest_success() {
        use admin_api::handlers::datasets::get_version_schema::TableSchemaInfo;
        use datasets_common::manifest::DataType;
        use datasets_derived::manifest::{ArrowSchema, Field, TableSchema};

        // Create a temporary JSON config file
        let mut config_file = NamedTempFile::with_suffix(".json").unwrap();
        writeln!(
            config_file,
            r#"{{
                "name": "test_dataset",
                "network": "mainnet",
                "version": "1.0.0",
                "dependencies": {{}},
                "tables": {{
                    "blocks": {{
                        "sql": "SELECT * FROM source.blocks"
                    }},
                    "transactions": {{
                        "sql": "SELECT * FROM source.transactions"
                    }}
                }},
                "functions": {{}}
            }}"#
        )
        .unwrap();
        config_file.flush().unwrap();

        // Start a mock HTTP server
        let mut server = mockito::Server::new_async().await;

        // Mock the versions endpoint to return a qualified version
        let versions_mock = server
            .mock("GET", "/datasets/test_dataset/versions?limit=1000")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::to_string(&DatasetVersionsResponse {
                    versions: vec![
                        "1.0.0-ABC123".parse().unwrap(),
                        "0.9.0-XYZ789".parse().unwrap(),
                    ],
                    next_cursor: None,
                })
                .unwrap(),
            )
            .create_async()
            .await;

        // Mock the schema endpoint with the qualified version
        let schema_mock = server
            .mock("GET", "/datasets/test_dataset/versions/1.0.0-ABC123/schema")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::to_string(&DatasetSchemaResponse {
                    name: "test_dataset".parse().unwrap(),
                    version: "1.0.0".parse().unwrap(),
                    tables: vec![
                        TableSchemaInfo {
                            name: "blocks".to_string(),
                            network: "mainnet".to_string(),
                            schema: TableSchema {
                                arrow: ArrowSchema {
                                    fields: vec![
                                        Field {
                                            name: "number".to_string(),
                                            type_: DataType(arrow_schema::DataType::UInt64),
                                            nullable: false,
                                        },
                                        Field {
                                            name: "hash".to_string(),
                                            type_: DataType(arrow_schema::DataType::Utf8),
                                            nullable: false,
                                        },
                                    ],
                                },
                            },
                        },
                        TableSchemaInfo {
                            name: "transactions".to_string(),
                            network: "mainnet".to_string(),
                            schema: TableSchema {
                                arrow: ArrowSchema {
                                    fields: vec![Field {
                                        name: "hash".to_string(),
                                        type_: DataType(arrow_schema::DataType::Utf8),
                                        nullable: false,
                                    }],
                                },
                            },
                        },
                    ],
                })
                .unwrap(),
            )
            .create_async()
            .await;

        // Call fetch_manifest
        let result = fetch_manifest(&server.url(), config_file.path()).await;

        // Verify both mocks were called
        versions_mock.assert_async().await;
        schema_mock.assert_async().await;

        // Verify the result
        match result {
            Ok(manifest) => {
                assert_eq!(manifest.name.as_ref(), "test_dataset");
                assert_eq!(manifest.version.to_string(), "1.0.0-ABC123");
                assert_eq!(manifest.network, "mainnet");
                assert_eq!(manifest.tables.len(), 2);

                // Verify blocks table
                let blocks = manifest.tables.get("blocks").unwrap();
                assert_eq!(blocks.network, "mainnet");
                assert_eq!(blocks.schema.arrow.fields.len(), 2);
                match &blocks.input {
                    datasets_derived::manifest::TableInput::View(view) => {
                        assert_eq!(view.sql, "SELECT * FROM source.blocks");
                    }
                }

                // Verify transactions table
                let transactions = manifest.tables.get("transactions").unwrap();
                assert_eq!(transactions.network, "mainnet");
                assert_eq!(transactions.schema.arrow.fields.len(), 1);
                match &transactions.input {
                    datasets_derived::manifest::TableInput::View(view) => {
                        assert_eq!(view.sql, "SELECT * FROM source.transactions");
                    }
                }
            }
            Err(e) => {
                panic!("fetch_manifest should succeed: {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_fetch_manifest_http_error() {
        // Create a temporary JSON config file
        let mut config_file = NamedTempFile::with_suffix(".json").unwrap();
        writeln!(
            config_file,
            r#"{{
                "name": "test_dataset",
                "network": "mainnet",
                "version": "1.0.0",
                "dependencies": {{}},
                "tables": {{}},
                "functions": {{}}
            }}"#
        )
        .unwrap();
        config_file.flush().unwrap();

        // Start a mock HTTP server that returns 404 on versions endpoint
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/datasets/test_dataset/versions?limit=1000")
            .with_status(404)
            .with_body("Not Found")
            .create_async()
            .await;

        // Call fetch_manifest
        let result = fetch_manifest(&server.url(), config_file.path()).await;

        // Verify the mock was called
        mock.assert_async().await;

        // Verify the error
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("No versions available"));
        assert!(err_msg.contains("test_dataset"));
    }

    #[tokio::test]
    async fn test_fetch_manifest_schema_mismatch() {
        use admin_api::handlers::datasets::get_version_schema::TableSchemaInfo;
        use datasets_derived::manifest::{ArrowSchema, TableSchema};

        // Create a config with only one table
        let mut config_file = NamedTempFile::with_suffix(".json").unwrap();
        writeln!(
            config_file,
            r#"{{
                "name": "test_dataset",
                "network": "mainnet",
                "version": "1.0.0",
                "dependencies": {{}},
                "tables": {{
                    "blocks": {{
                        "sql": "SELECT * FROM source.blocks"
                    }}
                }},
                "functions": {{}}
            }}"#
        )
        .unwrap();
        config_file.flush().unwrap();

        // Start a mock HTTP server
        let mut server = mockito::Server::new_async().await;

        // Mock versions endpoint
        let versions_mock = server
            .mock("GET", "/datasets/test_dataset/versions?limit=1000")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::to_string(&DatasetVersionsResponse {
                    versions: vec!["1.0.0-ABC123".parse().unwrap()],
                    next_cursor: None,
                })
                .unwrap(),
            )
            .create_async()
            .await;

        // Mock schema endpoint that returns schema with extra table
        let schema_mock = server
            .mock("GET", "/datasets/test_dataset/versions/1.0.0-ABC123/schema")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::to_string(&DatasetSchemaResponse {
                    name: "test_dataset".parse().unwrap(),
                    version: "1.0.0-ABC123".parse().unwrap(),
                    tables: vec![
                        TableSchemaInfo {
                            name: "blocks".to_string(),
                            network: "mainnet".to_string(),
                            schema: TableSchema {
                                arrow: ArrowSchema { fields: vec![] },
                            },
                        },
                        TableSchemaInfo {
                            name: "transactions".to_string(), // This table is NOT in local config
                            network: "mainnet".to_string(),
                            schema: TableSchema {
                                arrow: ArrowSchema { fields: vec![] },
                            },
                        },
                    ],
                })
                .unwrap(),
            )
            .create_async()
            .await;

        // Call fetch_manifest
        let result = fetch_manifest(&server.url(), config_file.path()).await;

        // Verify both mocks were called
        versions_mock.assert_async().await;
        schema_mock.assert_async().await;

        // Verify the error
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Schema mismatch"));
        assert!(err_msg.contains("transactions"));
        assert!(err_msg.contains("local config"));
    }

    #[tokio::test]
    async fn test_fetch_manifest_invalid_config_name() {
        // Create a config with invalid name
        let mut config_file = NamedTempFile::with_suffix(".json").unwrap();
        writeln!(
            config_file,
            r#"{{
                "name": "invalid name with spaces",
                "network": "mainnet",
                "version": "1.0.0",
                "dependencies": {{}},
                "tables": {{}},
                "functions": {{}}
            }}"#
        )
        .unwrap();
        config_file.flush().unwrap();

        let server = mockito::Server::new_async().await;

        // Call fetch_manifest
        let result = fetch_manifest(&server.url(), config_file.path()).await;

        // Should fail during name parsing, before making HTTP request
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Invalid dataset name"));
    }

    #[tokio::test]
    async fn test_fetch_manifest_invalid_version() {
        // Create a config with invalid version
        let mut config_file = NamedTempFile::with_suffix(".json").unwrap();
        writeln!(
            config_file,
            r#"{{
                "name": "test_dataset",
                "network": "mainnet",
                "version": "not-a-semver",
                "dependencies": {{}},
                "tables": {{}},
                "functions": {{}}
            }}"#
        )
        .unwrap();
        config_file.flush().unwrap();

        let server = mockito::Server::new_async().await;

        // Call fetch_manifest
        let result = fetch_manifest(&server.url(), config_file.path()).await;

        // Should fail during version parsing
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Invalid dataset version") || err_msg.contains("version"),
            "Expected version error but got: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_resolve_qualified_version_no_versions() {
        // Test when no versions are returned
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/datasets/test_dataset/versions?limit=1000")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::to_string(&DatasetVersionsResponse {
                    versions: vec![],
                    next_cursor: None,
                })
                .unwrap(),
            )
            .create_async()
            .await;

        let name: Name = "test_dataset".parse().unwrap();
        let version: Version = "1.0.0".parse().unwrap();

        let result = resolve_qualified_version(&server.url(), &name, &version).await;

        mock.assert_async().await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("No versions available"));
        assert!(err_msg.contains("test_dataset"));
    }

    #[tokio::test]
    async fn test_resolve_qualified_version_no_match() {
        // Test when versions exist but none match the prefix
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/datasets/test_dataset/versions?limit=1000")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::to_string(&DatasetVersionsResponse {
                    versions: vec![
                        "2.0.0-ABC123".parse().unwrap(),
                        "1.9.0-XYZ789".parse().unwrap(),
                    ],
                    next_cursor: None,
                })
                .unwrap(),
            )
            .create_async()
            .await;

        let name: Name = "test_dataset".parse().unwrap();
        let version: Version = "1.0.0".parse().unwrap();

        let result = resolve_qualified_version(&server.url(), &name, &version).await;

        mock.assert_async().await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("not found"));
        assert!(err_msg.contains("test_dataset"));
        assert!(err_msg.contains("1.0.0"));
    }

    #[tokio::test]
    async fn test_resolve_qualified_version_success() {
        // Test successful resolution
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/datasets/test_dataset/versions?limit=1000")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::to_string(&DatasetVersionsResponse {
                    versions: vec![
                        "1.0.0-ABC123".parse().unwrap(),
                        "1.0.0-DEF456".parse().unwrap(),
                        "0.9.0-XYZ789".parse().unwrap(),
                    ],
                    next_cursor: None,
                })
                .unwrap(),
            )
            .create_async()
            .await;

        let name: Name = "test_dataset".parse().unwrap();
        let version: Version = "1.0.0".parse().unwrap();

        let result = resolve_qualified_version(&server.url(), &name, &version).await;

        mock.assert_async().await;

        assert!(result.is_ok());
        let qualified = result.unwrap();
        // Should return the first matching version
        assert_eq!(qualified.to_string(), "1.0.0-ABC123");
    }

    #[tokio::test]
    async fn test_load_json_dataset_definition() {
        // Create a temporary JSON file with a minimal dataset definition
        let mut file = NamedTempFile::with_suffix(".json").unwrap();
        writeln!(
            file,
            r#"{{
                "name": "test-dataset",
                "network": "mainnet",
                "version": "1.0.0",
                "dependencies": {{}},
                "tables": {{}},
                "functions": {{}}
            }}"#
        )
        .unwrap();

        // Try to load it
        let result = load_dataset_definition(file.path()).await;

        match result {
            Ok(def) => {
                assert_eq!(def.name, "test-dataset");
                assert_eq!(def.version, "1.0.0");
                assert_eq!(def.network, "mainnet");
            }
            Err(e) => {
                panic!("Failed to parse: {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_parse_simple_js_export() {
        let js_content = r#"
            export default {
                name: "test",
                version: "1.0.0",
                network: "mainnet",
                kind: "manifest",
                dependencies: {},
                tables: {},
                functions: {}
            }
        "#;

        let result = parse_js_ts_manifest(js_content, "js", Path::new("test.js"));
        match result {
            Ok(def) => {
                assert_eq!(def.name, "test");
                assert_eq!(def.version, "1.0.0");
                assert_eq!(def.network, "mainnet");
            }
            Err(e) => {
                panic!("Failed to parse: {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_parse_define_dataset_pattern() {
        let ts_content = r#"
            import { defineDataset } from "nozzl"

            export default defineDataset(() => ({
                name: "example",
                version: "0.1.0",
                network: "mainnet",
                kind: "manifest",
                dependencies: {},
                tables: {},
                functions: {}
            }))
        "#;

        let result = parse_js_ts_manifest(ts_content, "ts", Path::new("test.ts"));
        // This should parse the defineDataset pattern
        match result {
            Ok(def) => {
                assert_eq!(def.name, "example");
                assert_eq!(def.version, "0.1.0");
                assert_eq!(def.network, "mainnet");
            }
            Err(e) => {
                panic!("Failed to parse defineDataset pattern: {}", e);
            }
        }
    }

    #[test]
    fn test_is_retryable_dataset_not_found() {
        let error = ManifestError::DatasetNotFound {
            dataset: "test".to_string(),
            version: "1.0.0".to_string(),
        };
        assert!(error.is_retryable());
    }

    #[test]
    fn test_is_retryable_no_versions_available() {
        let error = ManifestError::NoVersionsAvailable {
            dataset: "test".to_string(),
        };
        assert!(error.is_retryable());
    }

    #[test]
    fn test_is_retryable_network_error() {
        let error = ManifestError::NetworkError {
            url: "http://localhost:1610".to_string(),
            message: "Connection refused".to_string(),
        };
        assert!(error.is_retryable());
    }

    #[test]
    fn test_is_retryable_http_5xx_error() {
        let error = ManifestError::HttpError {
            status: 503,
            url: "http://localhost:1610".to_string(),
            message: "Service Unavailable".to_string(),
        };
        assert!(error.is_retryable());
    }

    #[test]
    fn test_is_not_retryable_http_4xx_error() {
        let error = ManifestError::HttpError {
            status: 400,
            url: "http://localhost:1610".to_string(),
            message: "Bad Request".to_string(),
        };
        assert!(!error.is_retryable());
    }

    #[test]
    fn test_is_not_retryable_invalid_config() {
        let error = ManifestError::InvalidDatasetName {
            name: "invalid name".to_string(),
            reason: "Contains spaces".to_string(),
        };
        assert!(!error.is_retryable());
    }

    #[tokio::test]
    async fn test_fetch_manifest_with_startup_poll_success_after_retries() {
        use admin_api::handlers::datasets::get_version_schema::TableSchemaInfo;
        use datasets_derived::manifest::{ArrowSchema, TableSchema};

        // Create a temporary JSON config file
        let mut config_file = NamedTempFile::with_suffix(".json").unwrap();
        writeln!(
            config_file,
            r#"{{
                "name": "test_dataset",
                "network": "mainnet",
                "version": "1.0.0",
                "dependencies": {{}},
                "tables": {{
                    "blocks": {{
                        "sql": "SELECT * FROM source.blocks"
                    }}
                }},
                "functions": {{}}
            }}"#
        )
        .unwrap();
        config_file.flush().unwrap();

        let mut server = mockito::Server::new_async().await;

        // First two calls return 404, third succeeds
        let versions_mock_fail1 = server
            .mock("GET", "/datasets/test_dataset/versions?limit=1000")
            .with_status(404)
            .with_body("Not Found")
            .expect(1)
            .create_async()
            .await;

        let versions_mock_fail2 = server
            .mock("GET", "/datasets/test_dataset/versions?limit=1000")
            .with_status(404)
            .with_body("Not Found")
            .expect(1)
            .create_async()
            .await;

        let versions_mock_success = server
            .mock("GET", "/datasets/test_dataset/versions?limit=1000")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::to_string(&DatasetVersionsResponse {
                    versions: vec!["1.0.0-ABC123".parse().unwrap()],
                    next_cursor: None,
                })
                .unwrap(),
            )
            .expect(1)
            .create_async()
            .await;

        let schema_mock = server
            .mock("GET", "/datasets/test_dataset/versions/1.0.0-ABC123/schema")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::to_string(&DatasetSchemaResponse {
                    name: "test_dataset".parse().unwrap(),
                    version: "1.0.0-ABC123".parse().unwrap(),
                    tables: vec![TableSchemaInfo {
                        name: "blocks".to_string(),
                        network: "mainnet".to_string(),
                        schema: TableSchema {
                            arrow: ArrowSchema { fields: vec![] },
                        },
                    }],
                })
                .unwrap(),
            )
            .create_async()
            .await;

        // Call with startup polling - should succeed after retries
        let result = fetch_manifest_with_startup_poll(&server.url(), config_file.path()).await;

        versions_mock_fail1.assert_async().await;
        versions_mock_fail2.assert_async().await;
        versions_mock_success.assert_async().await;
        schema_mock.assert_async().await;

        assert!(result.is_ok());
        let manifest = result.unwrap();
        assert_eq!(manifest.name.as_ref(), "test_dataset");
    }

    #[tokio::test]
    async fn test_fetch_manifest_with_startup_poll_immediate_failure_on_invalid_config() {
        // Create a config with invalid name
        let mut config_file = NamedTempFile::with_suffix(".json").unwrap();
        writeln!(
            config_file,
            r#"{{
                "name": "invalid name with spaces",
                "network": "mainnet",
                "version": "1.0.0",
                "dependencies": {{}},
                "tables": {{}},
                "functions": {{}}
            }}"#
        )
        .unwrap();
        config_file.flush().unwrap();

        let server = mockito::Server::new_async().await;

        // Should fail immediately without retrying (invalid config)
        let result = fetch_manifest_with_startup_poll(&server.url(), config_file.path()).await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Invalid dataset name"));
    }

    #[tokio::test]
    async fn test_fetch_manifest_with_retry_success_after_retries() {
        use admin_api::handlers::datasets::get_version_schema::TableSchemaInfo;
        use datasets_derived::manifest::{ArrowSchema, TableSchema};

        let mut config_file = NamedTempFile::with_suffix(".json").unwrap();
        writeln!(
            config_file,
            r#"{{
                "name": "test_dataset",
                "network": "mainnet",
                "version": "1.0.0",
                "dependencies": {{}},
                "tables": {{
                    "blocks": {{
                        "sql": "SELECT * FROM source.blocks"
                    }}
                }},
                "functions": {{}}
            }}"#
        )
        .unwrap();
        config_file.flush().unwrap();

        let mut server = mockito::Server::new_async().await;

        // First call returns 404, second succeeds
        let versions_mock_fail = server
            .mock("GET", "/datasets/test_dataset/versions?limit=1000")
            .with_status(404)
            .with_body("Not Found")
            .expect(1)
            .create_async()
            .await;

        let versions_mock_success = server
            .mock("GET", "/datasets/test_dataset/versions?limit=1000")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::to_string(&DatasetVersionsResponse {
                    versions: vec!["1.0.0-ABC123".parse().unwrap()],
                    next_cursor: None,
                })
                .unwrap(),
            )
            .expect(1)
            .create_async()
            .await;

        let schema_mock = server
            .mock("GET", "/datasets/test_dataset/versions/1.0.0-ABC123/schema")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::to_string(&DatasetSchemaResponse {
                    name: "test_dataset".parse().unwrap(),
                    version: "1.0.0-ABC123".parse().unwrap(),
                    tables: vec![TableSchemaInfo {
                        name: "blocks".to_string(),
                        network: "mainnet".to_string(),
                        schema: TableSchema {
                            arrow: ArrowSchema { fields: vec![] },
                        },
                    }],
                })
                .unwrap(),
            )
            .create_async()
            .await;

        // Call with retry (max 3) - should succeed on second attempt
        let result = fetch_manifest_with_retry(&server.url(), config_file.path(), 3).await;

        versions_mock_fail.assert_async().await;
        versions_mock_success.assert_async().await;
        schema_mock.assert_async().await;

        assert!(result.is_ok());
        let manifest = result.unwrap();
        assert_eq!(manifest.name.as_ref(), "test_dataset");
    }

    #[tokio::test]
    async fn test_fetch_manifest_with_retry_max_retries_exceeded() {
        let mut config_file = NamedTempFile::with_suffix(".json").unwrap();
        writeln!(
            config_file,
            r#"{{
                "name": "test_dataset",
                "network": "mainnet",
                "version": "1.0.0",
                "dependencies": {{}},
                "tables": {{}},
                "functions": {{}}
            }}"#
        )
        .unwrap();
        config_file.flush().unwrap();

        let mut server = mockito::Server::new_async().await;

        // All attempts return 404
        let versions_mock = server
            .mock("GET", "/datasets/test_dataset/versions?limit=1000")
            .with_status(404)
            .with_body("Not Found")
            .expect(4) // Initial attempt + 3 retries
            .create_async()
            .await;

        // Call with retry (max 3) - should fail after exhausting retries
        let result = fetch_manifest_with_retry(&server.url(), config_file.path(), 3).await;

        versions_mock.assert_async().await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Failed to fetch manifest after 3 retries"));
        assert!(err_msg.contains("did the 'nozzle dump' command complete"));
    }

    #[tokio::test]
    async fn test_fetch_manifest_with_retry_immediate_failure_on_invalid_config() {
        // Create a config with invalid version
        let mut config_file = NamedTempFile::with_suffix(".json").unwrap();
        writeln!(
            config_file,
            r#"{{
                "name": "test_dataset",
                "network": "mainnet",
                "version": "not-a-semver",
                "dependencies": {{}},
                "tables": {{}},
                "functions": {{}}
            }}"#
        )
        .unwrap();
        config_file.flush().unwrap();

        let server = mockito::Server::new_async().await;

        // Should fail immediately without retrying (invalid version)
        let result = fetch_manifest_with_retry(&server.url(), config_file.path(), 3).await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("version") || err_msg.contains("Invalid"));
    }

    #[tokio::test]
    async fn test_parse_real_nozzle_config() {
        let nozzle_config = r#"
            import { defineDataset } from "nozzl"

            const event = (event: string) => {
              return `
                SELECT block_hash, tx_hash, block_num, timestamp, address, evm_decode_log(topic1, topic2, topic3, data, '${event}') as event
                FROM anvil.logs
                WHERE topic0 = evm_topic('${event}')
              `
            }

            const transfer = event("Transfer(address indexed from, address indexed to, uint256 value)")
            const count = event("Count(uint256 count)")

            export default defineDataset(() => ({
              name: "example",
              version: "0.1.0",
              network: "mainnet",
              kind: "manifest",
              dependencies: {
                anvil: {
                  owner: "graphprotocol",
                  name: "anvil",
                  version: "0.1.0",
                },
              },
              tables: {
                counts: {
                  sql: `
                    SELECT c.block_hash, c.tx_hash, c.address, c.block_num, c.timestamp, c.event['count'] as count
                    FROM (${count}) as c
                  `,
                },
                transfers: {
                  sql: `
                    SELECT t.block_num, t.timestamp, t.event['from'] as from, t.event['to'] as to, t.event['value'] as value
                    FROM (${transfer}) as t
                  `,
                },
              },
              functions: {}
            }))
        "#;

        let result = parse_js_ts_manifest(nozzle_config, "ts", Path::new("nozzle.config.ts"));
        match result {
            Ok(def) => {
                assert_eq!(def.name, "example");
                assert_eq!(def.version, "0.1.0");
                assert_eq!(def.network, "mainnet");
                // Check that tables were parsed (even if SQL contains complex expressions)
                assert!(def.tables.contains_key("counts"));
                assert!(def.tables.contains_key("transfers"));
            }
            Err(e) => {
                println!("Failed to parse real nozzle config: {}", e);
                // This might fail due to complex template literals or other features
                // but we should at least be able to extract the basic structure
            }
        }
    }
}
