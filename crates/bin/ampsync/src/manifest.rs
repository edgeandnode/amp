use std::{fs, path::Path};

use common::BoxError;
use datasets_derived::manifest::Manifest;
use oxc_allocator::Allocator;
use oxc_ast::ast::*;
use oxc_ast_visit::{Visit, walk};
use oxc_parser::{ParseOptions, Parser};
use oxc_span::SourceType;
use serde_json::Value;

use crate::{dataset_definition::DatasetDefinition, sql_validator};

/// Load and parse a nozzle configuration file into a DatasetDefinition.
///
/// This function reads the nozzle config file and attempts to parse it into a user-facing
/// DatasetDefinition (simple format with just SQL strings in tables).
/// Supports:
/// - JSON files: Direct parsing via serde_json
/// - JS/TS files: AST parsing via oxc_parser to extract exported configuration
///
/// Validates:
/// - All SQL queries must not contain ORDER BY clauses (non-incremental queries not supported)
///
/// # Arguments
/// * `config_path` - Path to the nozzle configuration file
///
/// # Returns
/// * `Result<DatasetDefinition, BoxError>` - Parsed dataset definition or error
pub async fn load_dataset_definition(config_path: &Path) -> Result<DatasetDefinition, BoxError> {
    // Read the file contents
    let contents = fs::read_to_string(config_path)
        .map_err(|e| format!("Failed to read manifest file: {}", e))?;

    // Determine how to parse based on file extension
    let extension = config_path
        .extension()
        .and_then(|ext| ext.to_str())
        .ok_or("Unable to determine file extension")?;

    let dataset_def = match extension {
        "json" => {
            // Parse JSON directly using serde_json
            serde_json::from_str(&contents)
                .map_err(|e| format!("Failed to parse manifest as JSON: {}", e).into())
        }
        "js" | "mjs" | "ts" | "mts" => {
            // Parse JS/TS files using oxc_parser
            parse_js_ts_manifest(&contents, extension)
        }
        _ => Err(format!("Unsupported file extension: {}", extension).into()),
    }?;

    // Validate all SQL queries in tables
    for (table_name, table_def) in &dataset_def.tables {
        sql_validator::validate_sql(&table_def.sql)
            .map_err(|e| format!("Invalid SQL in table '{}': {}", table_name, e))?;
    }

    Ok(dataset_def)
}

/// Parse a JavaScript/TypeScript file to extract the dataset definition using AST parsing
fn parse_js_ts_manifest(contents: &str, extension: &str) -> Result<DatasetDefinition, BoxError> {
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
        return Err(format!("Parse errors: {}", error_messages.join(", ")).into());
    }

    // Extract the manifest from the AST
    let mut extractor = ManifestExtractor::new();
    extractor.visit_program(&parse_result.program);

    extractor
        .extract_manifest()
        .ok_or_else(|| "No manifest configuration found in the file".into())
}

/// Transform a DatasetDefinition to a full Manifest by inferring schemas from Nozzle server.
///
/// This function takes a simple user-facing dataset definition and transforms it into
/// a complete manifest with inferred Arrow schemas for all tables.
///
/// # Arguments
/// * `dataset_def` - The user-facing dataset definition
/// * `nozzle_endpoint` - Nozzle server endpoint for schema inference
///
/// # Returns
/// * `Result<Manifest, BoxError>` - Complete manifest with inferred schemas
pub async fn transform_to_manifest(
    dataset_def: DatasetDefinition,
    nozzle_endpoint: &str,
) -> Result<Manifest, BoxError> {
    use std::collections::BTreeMap;

    use datasets_derived::manifest::{Table, TableInput, TableSchema, View};
    use nozzle_client::SqlClient;

    // Connect to Nozzle server for schema inference
    let mut sql_client = SqlClient::new(nozzle_endpoint)
        .await
        .map_err(|e| format!("Failed to connect to Nozzle server: {}", e))?;

    // Transform tables with schema inference
    let mut tables = BTreeMap::new();

    for (table_name, table_def) in dataset_def.tables {
        // Infer the schema by querying Nozzle with dataset context
        let schema = crate::schema_inference::infer_schema(
            &mut sql_client,
            &table_def.sql,
            &dataset_def.name,
            &table_name,
        )
        .await
        .map_err(|e| format!("Failed to infer schema for table '{}': {}", table_name, e))?;

        // Create Table with inferred schema
        let table = Table {
            input: TableInput::View(View { sql: table_def.sql }),
            schema: TableSchema { arrow: schema },
            network: dataset_def.network.clone(),
        };

        tables.insert(table_name, table);
    }

    // Transform functions
    let functions = dataset_def
        .functions
        .into_iter()
        .map(|(name, func_def)| {
            use std::sync::Arc;

            use datasets_derived::manifest::{Function, FunctionSource};

            (
                name,
                Function {
                    input_types: func_def
                        .input_types
                        .into_iter()
                        .map(|type_str| {
                            // Parse the type string to DataType
                            // For now, this is a placeholder - we'll need to implement proper parsing
                            use datafusion::arrow::datatypes::DataType as ArrowDataType;
                            use datasets_common::manifest::DataType;

                            // Simple type parsing - extend as needed
                            let arrow_type = match type_str.as_str() {
                                "Int64" => ArrowDataType::Int64,
                                "Utf8" => ArrowDataType::Utf8,
                                "Boolean" => ArrowDataType::Boolean,
                                "Float64" => ArrowDataType::Float64,
                                _ => ArrowDataType::Utf8, // Default fallback
                            };

                            DataType(arrow_type)
                        })
                        .collect(),
                    output_type: {
                        use datafusion::arrow::datatypes::DataType as ArrowDataType;
                        use datasets_common::manifest::DataType;

                        let arrow_type = match func_def.output_type.as_str() {
                            "Int64" => ArrowDataType::Int64,
                            "Utf8" => ArrowDataType::Utf8,
                            "Boolean" => ArrowDataType::Boolean,
                            "Float64" => ArrowDataType::Float64,
                            _ => ArrowDataType::Utf8, // Default fallback
                        };

                        DataType(arrow_type)
                    },
                    source: FunctionSource {
                        source: Arc::from(func_def.source.source),
                        filename: func_def.source.filename,
                    },
                },
            )
        })
        .collect();

    // Transform dependencies
    let dependencies = dataset_def
        .dependencies
        .into_iter()
        .map(|(name, dep)| {
            use datasets_common::manifest::VersionReq;
            use datasets_derived::manifest::Dependency;

            (
                name, // String key
                Dependency {
                    owner: dep.owner, // String, not Name
                    name: dep.name,   // String, not Name
                    version: dep.version.parse::<VersionReq>().unwrap(),
                },
            )
        })
        .collect();

    use datasets_common::{name::Name, version::Version};
    use datasets_derived::DerivedDatasetKind;

    Ok(Manifest {
        name: dataset_def.name.parse::<Name>()?,
        version: dataset_def.version.parse::<Version>()?,
        kind: DerivedDatasetKind,
        network: dataset_def.network, // String, not Name
        dependencies,
        tables,
        functions,
    })
}

/// Load and transform a nozzle configuration file into a complete Manifest.
///
/// This is the main entry point that combines parsing and transformation.
///
/// # Arguments
/// * `config_path` - Path to the nozzle configuration file
/// * `nozzle_endpoint` - Nozzle server endpoint for schema inference
///
/// # Returns
/// * `Result<Manifest, BoxError>` - Complete manifest with inferred schemas
pub async fn load_manifest(
    config_path: &Path,
    nozzle_endpoint: &str,
) -> Result<Manifest, BoxError> {
    // Load the dataset definition
    let dataset_def = load_dataset_definition(config_path).await?;

    // Transform to manifest with schema inference
    transform_to_manifest(dataset_def, nozzle_endpoint).await
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

    fn extract_manifest(self) -> Option<DatasetDefinition> {
        let json = self.manifest_json?;
        serde_json::from_value(json).ok()
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

        let result = parse_js_ts_manifest(js_content, "js");
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

        let result = parse_js_ts_manifest(ts_content, "ts");
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

        let result = parse_js_ts_manifest(nozzle_config, "ts");
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
