//! Shared validation pipeline for dataset authoring.
//!
//! Performs configuration parsing, model discovery, dependency resolution,
//! SQL rendering, SELECT-only validation, incremental checks, and schema
//! inference. This pipeline is validation-only and does not write artifacts.

use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use common::{
    incrementalizer::NonIncrementalQueryError,
    plan_visitors::is_incremental,
    query_context,
    sql::{
        FunctionReference, ResolveFunctionReferencesError, ResolveTableReferencesError,
        TableReference, parse, resolve_function_references, resolve_table_references,
    },
};
use datafusion::{
    arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields},
    common::utils::quote_identifier,
    error::DataFusionError,
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
    },
};
use datasets_common::{
    manifest::{DataType as ManifestDataType, TableSchema},
    table_name::TableName,
};
use datasets_derived::{
    deps::{DepAlias, DepAliasError, DepAliasOrSelfRef, DepAliasOrSelfRefError, SELF_REF_KEYWORD},
    func_name::{ETH_CALL_FUNCTION_NAME, FuncName},
    sql_str::SqlStr,
};

use crate::{
    config::{self, AmpYaml},
    discovery::{DiscoveryError, discover_tables},
    jinja::{JinjaContext, RenderError, render_sql},
    lockfile::Lockfile,
    manifest,
    query::{SelectValidationError, validate_select},
    resolver::{DependencyGraph, ManifestFetcher, ResolveError, Resolver},
    schema::{DependencyTable, SchemaContext, SchemaInferenceError},
};

/// Options for the validation pipeline.
pub struct ValidationOptions<'a, F> {
    /// Project directory containing amp.yaml/amp.yml.
    pub dir: &'a Path,
    /// CLI variable overrides.
    pub cli_vars: &'a [(String, String)],
    /// Dependency resolver.
    pub resolver: &'a Resolver<F>,
    /// Dependency resolution mode.
    pub resolution: ResolutionMode<'a>,
}

impl<'a, F> ValidationOptions<'a, F> {
    /// Creates validation options with defaults.
    pub fn new(dir: &'a Path, resolver: &'a Resolver<F>) -> Self {
        Self {
            dir,
            resolver,
            cli_vars: &[],
            resolution: ResolutionMode::Resolve,
        }
    }

    /// Provide CLI variable overrides.
    pub fn with_cli_vars(mut self, cli_vars: &'a [(String, String)]) -> Self {
        self.cli_vars = cli_vars;
        self
    }

    /// Configure dependency resolution mode.
    pub fn with_resolution(mut self, resolution: ResolutionMode<'a>) -> Self {
        self.resolution = resolution;
        self
    }
}

/// Dependency resolution mode for validation.
#[derive(Debug, Clone, Copy)]
pub enum ResolutionMode<'a> {
    /// Resolve dependencies from the registry (default).
    Resolve,
    /// Resolve dependencies using hashes from a lockfile.
    Locked { lockfile: &'a Lockfile },
}

/// Output of the validation pipeline.
#[derive(Debug)]
pub struct ValidationOutput {
    /// Resolved config file path.
    pub config_path: PathBuf,
    /// Parsed authoring configuration.
    pub config: AmpYaml,
    /// Discovered tables (table -> file path).
    pub discovered_tables: BTreeMap<TableName, PathBuf>,
    /// Resolved dependency graph.
    pub dependencies: DependencyGraph,
    /// Rendered SQL and inferred schemas for each table.
    pub validated_tables: BTreeMap<TableName, ValidatedTable>,
}

/// Validated table output.
#[derive(Debug, Clone)]
pub struct ValidatedTable {
    /// Path to the source SQL file, relative to the project root.
    pub source_path: PathBuf,
    /// Rendered SQL after Jinja templating.
    pub rendered_sql: String,
    /// Inferred schema from the rendered SQL.
    pub schema: TableSchema,
}

/// Errors that can occur during validation.
#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    /// Failed to locate, read, or parse the authoring config.
    #[error(transparent)]
    ConfigLoad(#[from] config::ConfigLoadError),

    /// CLI vars not declared in amp.yaml.
    #[error(
        "unknown CLI vars: {unknown:?}. Declare them under `vars` in {}",
        config_path.display()
    )]
    UnknownCliVars {
        unknown: Vec<String>,
        config_path: PathBuf,
    },

    /// Model discovery failed.
    #[error("model discovery failed")]
    Discovery(#[source] DiscoveryError),

    /// Dependency resolution failed.
    #[error("dependency resolution failed")]
    Resolution(#[source] ResolveError),

    /// Failed to read SQL template file.
    #[error("failed to read SQL file {}", path.display())]
    ReadSqlFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// Jinja template rendering failed.
    #[error("failed to render SQL for table '{table}'")]
    JinjaRender {
        table: TableName,
        #[source]
        source: RenderError,
    },

    /// Invalid function data type.
    #[error("invalid data type '{type_str}' for function '{func_name}'")]
    InvalidFunctionType {
        func_name: FuncName,
        type_str: String,
        #[source]
        source: serde_json::Error,
    },

    /// Function source file is missing.
    #[error("function source file not found for '{func_name}' at {}", path.display())]
    FunctionSourceMissing { func_name: FuncName, path: PathBuf },

    /// Failed to read function source file metadata.
    #[error("failed to read function source file for '{func_name}' at {}", path.display())]
    FunctionSourceRead {
        func_name: FuncName,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// Invalid SQL string.
    #[error("invalid SQL string for table '{table}'")]
    InvalidSqlString { table: TableName },

    /// SQL parsing failed.
    #[error("failed to parse SQL for table '{table}'")]
    SqlParse {
        table: TableName,
        #[source]
        source: common::sql::ParseSqlError,
    },

    /// Statement is not a SELECT query.
    #[error("invalid SQL for model '{table}'")]
    NotSelectStatement {
        table: TableName,
        #[source]
        source: SelectValidationError,
    },

    /// Failed to resolve table references from SQL.
    #[error("failed to resolve table references for table '{table}'")]
    TableReferenceResolution {
        table: TableName,
        #[source]
        source: ResolveTableReferencesError<DepAliasError>,
    },

    /// Catalog-qualified table reference detected in SQL.
    #[error("catalog-qualified table reference in table '{table}'")]
    CatalogQualifiedTableInSql {
        table: TableName,
        #[source]
        source: ResolveTableReferencesError<DepAliasError>,
    },

    /// Invalid table name in SQL reference.
    #[error("invalid table name in SQL for table '{table}'")]
    InvalidTableName {
        table: TableName,
        #[source]
        source: ResolveTableReferencesError<DepAliasError>,
    },

    /// Unqualified table reference detected.
    #[error("unqualified table reference in table '{table}': {table_ref}")]
    UnqualifiedTableReference { table: TableName, table_ref: String },

    /// Dependency alias referenced in SQL was not declared.
    #[error("dependency alias '{alias}' not found for table '{table}'")]
    DependencyAliasNotFound { table: TableName, alias: DepAlias },

    /// Referenced table not found in dependency manifest.
    #[error("table '{referenced_table}' not found in dependency '{alias}' for table '{table}'")]
    TableNotFoundInDependency {
        table: TableName,
        alias: DepAlias,
        referenced_table: TableName,
    },

    /// Failed to resolve function references from SQL.
    #[error("failed to resolve function references for table '{table}'")]
    FunctionReferenceResolution {
        table: TableName,
        #[source]
        source: ResolveFunctionReferencesError<DepAliasOrSelfRefError>,
    },

    /// Referenced function not found in dependency manifest.
    #[error("function '{function}' not found in dependency '{alias}' for table '{table}'")]
    FunctionNotFoundInDependency {
        table: TableName,
        alias: DepAlias,
        function: FuncName,
    },

    /// Self-referenced function not defined in amp.yaml.
    #[error("self-referenced function '{function}' not found for table '{table}'")]
    SelfFunctionNotFound {
        table: TableName,
        function: FuncName,
    },

    /// eth_call is not available for the referenced dependency.
    #[error("eth_call not available for dependency '{alias}' in table '{table}'")]
    EthCallNotAvailable { table: TableName, alias: DepAlias },

    /// SQL planning failed.
    #[error("failed to plan SQL for table '{table}'")]
    SqlPlanning {
        table: TableName,
        #[source]
        source: query_context::Error,
    },

    /// Schema inference failed.
    #[error("schema inference failed for table '{table}'")]
    SchemaInference {
        table: TableName,
        #[source]
        source: SchemaInferenceError,
    },

    /// Network inference failed.
    #[error("network inference failed for table '{table}'")]
    NetworkInference {
        table: TableName,
        #[source]
        source: crate::manifest::ManifestError,
    },

    /// Query contains non-incremental operations.
    #[error("query for table '{table}' contains non-incremental operations")]
    NonIncremental {
        table: TableName,
        #[source]
        source: NonIncrementalQueryError,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StubUdf {
    name: String,
    signature: Signature,
    return_type: ArrowDataType,
}

impl StubUdf {
    fn new(name: String, signature: Signature, return_type: ArrowDataType) -> Self {
        Self {
            name,
            signature,
            return_type,
        }
    }

    fn from_exact_signature(
        name: String,
        input_types: Vec<ArrowDataType>,
        return_type: ArrowDataType,
    ) -> Self {
        let signature = Signature {
            type_signature: TypeSignature::Exact(input_types),
            volatility: Volatility::Immutable,
            parameter_names: None,
        };
        Self::new(name, signature, return_type)
    }

    fn eth_call(schema: &str) -> Self {
        let name = format!("{}.{}", quote_identifier(schema), ETH_CALL_FUNCTION_NAME);
        let signature = Signature {
            type_signature: TypeSignature::Any(4),
            volatility: Volatility::Volatile,
            parameter_names: Some(vec![
                "from".to_string(),
                "to".to_string(),
                "input_data".to_string(),
                "block".to_string(),
            ]),
        };
        let fields = ArrowFields::from_iter([
            ArrowField::new("data", ArrowDataType::Binary, true),
            ArrowField::new("message", ArrowDataType::Utf8, true),
        ]);
        let return_type = ArrowDataType::Struct(fields);
        Self::new(name, signature, return_type)
    }
}

impl ScalarUDFImpl for StubUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[ArrowDataType]) -> Result<ArrowDataType, DataFusionError> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> Result<ColumnarValue, DataFusionError> {
        Err(DataFusionError::Plan(format!(
            "UDF '{}' is not executable during validation",
            self.name
        )))
    }
}

fn qualified_udf_name(schema: &str, function: &FuncName) -> String {
    format!("{}.{}", quote_identifier(schema), function)
}

/// Run the shared validation pipeline for a dataset project.
///
/// This function performs configuration parsing, model discovery,
/// dependency resolution, SQL rendering, SELECT validation, incremental
/// checks, and schema inference. It does not write any output artifacts.
///
/// # Errors
///
/// Returns [`ValidationError`] for any validation failures.
pub async fn validate_project<F: ManifestFetcher>(
    options: ValidationOptions<'_, F>,
) -> Result<ValidationOutput, ValidationError> {
    let (config_path, config) = config::load_from_dir(options.dir)?;
    validate_project_with_config(options, config_path, config).await
}

/// Run the shared validation pipeline for a dataset project with a pre-loaded config.
///
/// This function performs model discovery, dependency resolution, SQL rendering,
/// SELECT validation, incremental checks, and schema inference. It does not write
/// any output artifacts.
///
/// # Errors
///
/// Returns [`ValidationError`] for any validation failures.
pub async fn validate_project_with_config<F: ManifestFetcher>(
    options: ValidationOptions<'_, F>,
    config_path: PathBuf,
    config: AmpYaml,
) -> Result<ValidationOutput, ValidationError> {
    let ValidationOptions {
        dir,
        cli_vars,
        resolver,
        resolution,
    } = options;

    validate_cli_vars(&config_path, &config, cli_vars)?;
    validate_function_types(&config)?;
    validate_function_sources(dir, &config)?;

    let discovered_tables =
        discover_tables(dir, &config.tables).map_err(ValidationError::Discovery)?;

    let dependencies = match resolution {
        ResolutionMode::Resolve => resolver
            .resolve_all(&config.dependencies)
            .await
            .map_err(ValidationError::Resolution)?,
        ResolutionMode::Locked { lockfile } => resolver
            .resolve_all_locked(lockfile)
            .await
            .map_err(ValidationError::Resolution)?,
    };

    let jinja_ctx = build_jinja_context(&config, cli_vars);

    let mut schema_ctx = SchemaContext::new();
    register_dependency_tables(&mut schema_ctx, &dependencies);
    register_dependency_udfs(&mut schema_ctx, &dependencies)?;
    register_self_udfs(&mut schema_ctx, &config)?;

    let mut validated_tables = BTreeMap::new();
    for (table_name, sql_path) in &discovered_tables {
        let validated = validate_table(
            table_name,
            sql_path,
            dir,
            &jinja_ctx,
            &schema_ctx,
            &config,
            &dependencies,
        )
        .await?;
        validated_tables.insert(table_name.clone(), validated);
    }

    validate_network_inference(&discovered_tables, &dependencies)?;

    Ok(ValidationOutput {
        config_path,
        config,
        discovered_tables,
        dependencies,
        validated_tables,
    })
}

fn validate_cli_vars(
    config_path: &Path,
    config: &AmpYaml,
    cli_vars: &[(String, String)],
) -> Result<(), ValidationError> {
    if cli_vars.is_empty() {
        return Ok(());
    }

    let allowed: BTreeSet<&str> = match &config.vars {
        Some(vars) => vars.keys().map(String::as_str).collect(),
        None => BTreeSet::new(),
    };

    let mut unknown = BTreeSet::new();
    for (name, _) in cli_vars {
        if !allowed.contains(name.as_str()) {
            unknown.insert(name.clone());
        }
    }

    if unknown.is_empty() {
        Ok(())
    } else {
        Err(ValidationError::UnknownCliVars {
            unknown: unknown.into_iter().collect(),
            config_path: config_path.to_path_buf(),
        })
    }
}

fn validate_function_sources(project_dir: &Path, config: &AmpYaml) -> Result<(), ValidationError> {
    let Some(functions) = &config.functions else {
        return Ok(());
    };

    for (func_name, func_def) in functions {
        let path = project_dir.join(&func_def.source);
        match fs::metadata(&path) {
            Ok(metadata) => {
                if !metadata.is_file() {
                    return Err(ValidationError::FunctionSourceMissing {
                        func_name: func_name.clone(),
                        path,
                    });
                }
                fs::File::open(&path).map_err(|source| ValidationError::FunctionSourceRead {
                    func_name: func_name.clone(),
                    path,
                    source,
                })?;
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(ValidationError::FunctionSourceMissing {
                    func_name: func_name.clone(),
                    path,
                });
            }
            Err(err) => {
                return Err(ValidationError::FunctionSourceRead {
                    func_name: func_name.clone(),
                    path,
                    source: err,
                });
            }
        }
    }

    Ok(())
}

fn validate_function_types(config: &AmpYaml) -> Result<(), ValidationError> {
    let Some(functions) = &config.functions else {
        return Ok(());
    };

    for (func_name, func_def) in functions {
        for type_str in &func_def.input_types {
            let _ = parse_function_type(func_name, type_str)?;
        }

        let _ = parse_function_type(func_name, &func_def.output_type)?;
    }

    Ok(())
}

fn parse_function_type(
    func_name: &FuncName,
    type_str: &str,
) -> Result<ManifestDataType, ValidationError> {
    let value = serde_json::Value::String(type_str.to_string());
    serde_json::from_value(value).map_err(|source| ValidationError::InvalidFunctionType {
        func_name: func_name.clone(),
        type_str: type_str.to_string(),
        source,
    })
}

fn parse_function_signature(
    func_name: &FuncName,
    func_def: &config::FunctionDef,
) -> Result<(Vec<ArrowDataType>, ArrowDataType), ValidationError> {
    let mut inputs = Vec::with_capacity(func_def.input_types.len());
    for type_str in &func_def.input_types {
        let data_type = parse_function_type(func_name, type_str)?;
        inputs.push(data_type.into_arrow());
    }

    let output = parse_function_type(func_name, &func_def.output_type)?.into_arrow();

    Ok((inputs, output))
}

fn validate_network_inference(
    discovered_tables: &BTreeMap<TableName, PathBuf>,
    dependencies: &DependencyGraph,
) -> Result<(), ValidationError> {
    for table_name in discovered_tables.keys() {
        manifest::default_network_resolver(table_name, dependencies).map_err(|source| {
            ValidationError::NetworkInference {
                table: table_name.clone(),
                source,
            }
        })?;
    }

    Ok(())
}

fn build_jinja_context(config: &AmpYaml, cli_vars: &[(String, String)]) -> JinjaContext {
    let yaml_vars = config.vars.clone().unwrap_or_default();
    JinjaContext::builder()
        .with_cli_vars(cli_vars.iter().cloned())
        .with_yaml_vars(yaml_vars)
        .with_dep_aliases(config.dependencies.keys().cloned())
        .build()
}

fn register_dependency_tables(ctx: &mut SchemaContext, dep_graph: &DependencyGraph) {
    for (alias, hash) in &dep_graph.direct {
        if let Some(node) = dep_graph.get(hash) {
            for (table_name, table) in &node.resolved.manifest.tables {
                let arrow_schema = table.schema.arrow.clone().into_schema_ref();
                ctx.register_table(DependencyTable {
                    schema_name: alias.to_string(),
                    table_name: table_name.clone(),
                    schema: arrow_schema,
                });
            }
        }
    }
}

fn register_dependency_udfs(
    ctx: &mut SchemaContext,
    dep_graph: &DependencyGraph,
) -> Result<(), ValidationError> {
    for (alias, hash) in &dep_graph.direct {
        let Some(node) = dep_graph.get(hash) else {
            continue;
        };
        let manifest = &node.resolved.manifest;

        for (func_name, func_def) in &manifest.functions {
            let input_types = func_def
                .input_types
                .iter()
                .cloned()
                .map(ManifestDataType::into_arrow)
                .collect();
            let output_type = func_def.output_type.clone().into_arrow();
            let udf_name = qualified_udf_name(alias.as_str(), func_name);
            let udf = StubUdf::from_exact_signature(udf_name, input_types, output_type);
            ctx.register_udf(udf.into());
        }

        if manifest.kind.as_str() == "evm-rpc" {
            let udf = StubUdf::eth_call(alias.as_str());
            ctx.register_udf(udf.into());
        }
    }

    Ok(())
}

fn register_self_udfs(ctx: &mut SchemaContext, config: &AmpYaml) -> Result<(), ValidationError> {
    let Some(functions) = &config.functions else {
        return Ok(());
    };

    for (func_name, func_def) in functions {
        let (inputs, output) = parse_function_signature(func_name, func_def)?;
        let udf_name = qualified_udf_name(SELF_REF_KEYWORD, func_name);
        let udf = StubUdf::from_exact_signature(udf_name, inputs, output);
        ctx.register_udf(udf.into());
    }

    Ok(())
}

fn validate_table_references(
    table_name: &TableName,
    stmt: &datafusion::sql::parser::Statement,
    config: &AmpYaml,
    dependencies: &DependencyGraph,
) -> Result<(), ValidationError> {
    let table_refs = resolve_table_references::<DepAlias>(stmt).map_err(|err| match &err {
        ResolveTableReferencesError::InvalidTableName { .. } => ValidationError::InvalidTableName {
            table: table_name.clone(),
            source: err,
        },
        ResolveTableReferencesError::CatalogQualifiedTable { .. } => {
            ValidationError::CatalogQualifiedTableInSql {
                table: table_name.clone(),
                source: err,
            }
        }
        _ => ValidationError::TableReferenceResolution {
            table: table_name.clone(),
            source: err,
        },
    })?;

    for table_ref in table_refs {
        match table_ref {
            TableReference::Bare { .. } => {
                return Err(ValidationError::UnqualifiedTableReference {
                    table: table_name.clone(),
                    table_ref: table_ref.to_quoted_string(),
                });
            }
            TableReference::Partial { schema, table } => {
                let alias = schema.as_ref();
                let Some(hash) = dependencies.direct.get(alias) else {
                    return Err(ValidationError::DependencyAliasNotFound {
                        table: table_name.clone(),
                        alias: alias.clone(),
                    });
                };

                let Some(node) = dependencies.get(hash) else {
                    return Err(ValidationError::DependencyAliasNotFound {
                        table: table_name.clone(),
                        alias: alias.clone(),
                    });
                };

                if !node.resolved.manifest.tables.contains_key(table.as_ref()) {
                    return Err(ValidationError::TableNotFoundInDependency {
                        table: table_name.clone(),
                        alias: alias.clone(),
                        referenced_table: table.as_ref().clone(),
                    });
                }
            }
        }
    }

    let func_refs = resolve_function_references::<DepAliasOrSelfRef>(stmt).map_err(|err| {
        ValidationError::FunctionReferenceResolution {
            table: table_name.clone(),
            source: err,
        }
    })?;

    for func_ref in func_refs {
        match func_ref {
            FunctionReference::Bare { .. } => continue,
            FunctionReference::Qualified { schema, function } => match schema.as_ref() {
                DepAliasOrSelfRef::SelfRef => {
                    let Some(functions) = &config.functions else {
                        return Err(ValidationError::SelfFunctionNotFound {
                            table: table_name.clone(),
                            function: function.as_ref().clone(),
                        });
                    };

                    if !functions.contains_key(function.as_ref()) {
                        return Err(ValidationError::SelfFunctionNotFound {
                            table: table_name.clone(),
                            function: function.as_ref().clone(),
                        });
                    }
                }
                DepAliasOrSelfRef::DepAlias(alias) => {
                    let Some(hash) = dependencies.direct.get(alias) else {
                        return Err(ValidationError::DependencyAliasNotFound {
                            table: table_name.clone(),
                            alias: alias.clone(),
                        });
                    };

                    let Some(node) = dependencies.get(hash) else {
                        return Err(ValidationError::DependencyAliasNotFound {
                            table: table_name.clone(),
                            alias: alias.clone(),
                        });
                    };

                    if function.as_ref() == ETH_CALL_FUNCTION_NAME {
                        if node.resolved.manifest.kind.as_str() != "evm-rpc" {
                            return Err(ValidationError::EthCallNotAvailable {
                                table: table_name.clone(),
                                alias: alias.clone(),
                            });
                        }
                        continue;
                    }

                    if !node
                        .resolved
                        .manifest
                        .functions
                        .contains_key(function.as_ref())
                    {
                        return Err(ValidationError::FunctionNotFoundInDependency {
                            table: table_name.clone(),
                            alias: alias.clone(),
                            function: function.as_ref().clone(),
                        });
                    }
                }
            },
        }
    }

    Ok(())
}

async fn validate_table(
    table_name: &TableName,
    sql_path: &Path,
    project_dir: &Path,
    jinja_ctx: &JinjaContext,
    schema_ctx: &SchemaContext,
    config: &AmpYaml,
    dependencies: &DependencyGraph,
) -> Result<ValidatedTable, ValidationError> {
    let template_path = project_dir.join(sql_path);
    let template =
        fs::read_to_string(&template_path).map_err(|source| ValidationError::ReadSqlFile {
            path: template_path.clone(),
            source,
        })?;

    let rendered_sql = render_sql(&template, table_name.as_str(), jinja_ctx).map_err(|source| {
        ValidationError::JinjaRender {
            table: table_name.clone(),
            source,
        }
    })?;

    let sql_str: SqlStr = rendered_sql
        .parse()
        .map_err(|_| ValidationError::InvalidSqlString {
            table: table_name.clone(),
        })?;
    let stmt = parse(&sql_str).map_err(|source| ValidationError::SqlParse {
        table: table_name.clone(),
        source,
    })?;

    let select_info = validate_select(&stmt, table_name.clone()).map_err(|source| {
        ValidationError::NotSelectStatement {
            table: table_name.clone(),
            source,
        }
    })?;

    validate_table_references(table_name, &stmt, config, dependencies)?;

    let schema = schema_ctx
        .infer_schema(&select_info.query)
        .await
        .map_err(|source| ValidationError::SchemaInference {
            table: table_name.clone(),
            source,
        })?;

    validate_incremental_constraints(&select_info.query, schema_ctx, table_name).await?;

    Ok(ValidatedTable {
        source_path: sql_path.to_path_buf(),
        rendered_sql,
        schema,
    })
}

async fn validate_incremental_constraints(
    query: &datafusion::sql::sqlparser::ast::Query,
    schema_ctx: &SchemaContext,
    table_name: &TableName,
) -> Result<(), ValidationError> {
    let ctx = schema_ctx
        .create_session_for_validation()
        .map_err(|source| ValidationError::SchemaInference {
            table: table_name.clone(),
            source,
        })?;

    let stmt = datafusion::sql::parser::Statement::Statement(Box::new(
        datafusion::sql::sqlparser::ast::Statement::Query(Box::new(query.clone())),
    ));

    let plan = query_context::sql_to_plan(&ctx, stmt)
        .await
        .map_err(|source| ValidationError::SqlPlanning {
            table: table_name.clone(),
            source,
        })?;

    is_incremental(&plan).map_err(|source| ValidationError::NonIncremental {
        table: table_name.clone(),
        source,
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

    use datafusion::arrow::datatypes::DataType as ArrowDataType;
    use datasets_common::{
        dataset_kind_str::DatasetKindStr,
        hash::Hash,
        manifest::{ArrowSchema, Field, Function, FunctionSource, TableSchema},
        table_name::TableName,
    };
    use datasets_derived::{deps::DepAlias, sql_str::SqlStr};

    use super::{
        AmpYaml, ValidationError, validate_cli_vars, validate_incremental_constraints,
        validate_table_references,
    };
    use crate::{
        dependency_manifest::{DependencyManifest, DependencyTable},
        resolver::{DependencyGraph, DependencyNode, ResolvedDependency},
        schema::SchemaContext,
    };

    fn parse_statement(sql: &str) -> datafusion::sql::parser::Statement {
        let sql: SqlStr = sql.parse().expect("valid SQL string");
        common::sql::parse(&sql).expect("should parse SQL")
    }

    fn make_table_schema() -> TableSchema {
        TableSchema {
            arrow: ArrowSchema {
                fields: vec![Field {
                    name: "id".to_string(),
                    type_: ArrowDataType::Int64.into(),
                    nullable: false,
                }],
            },
        }
    }

    fn make_dependency_manifest(
        kind: &str,
        tables: &[&str],
        functions: &[&str],
    ) -> DependencyManifest {
        let mut table_map = BTreeMap::new();
        for table in tables {
            table_map.insert(
                table.parse().expect("valid table name"),
                DependencyTable {
                    schema: make_table_schema(),
                    network: "mainnet".parse().expect("valid network"),
                },
            );
        }

        let mut function_map = BTreeMap::new();
        for function in functions {
            function_map.insert(
                function.parse().expect("valid function name"),
                Function {
                    input_types: vec![ArrowDataType::Int64.into()],
                    output_type: ArrowDataType::Int64.into(),
                    source: FunctionSource {
                        source: Arc::from("function stub() {}"),
                        filename: format!("{function}.js"),
                    },
                },
            );
        }

        DependencyManifest {
            kind: DatasetKindStr::new(kind.to_string()),
            dependencies: BTreeMap::new(),
            tables: table_map,
            functions: function_map,
        }
    }

    fn make_dependency_graph(alias: &str, manifest: DependencyManifest) -> DependencyGraph {
        let mut graph = DependencyGraph::new();
        let alias: DepAlias = alias.parse().expect("valid alias");
        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");

        graph.direct.insert(alias.clone(), hash.clone());
        graph.nodes.insert(
            hash.clone(),
            DependencyNode {
                resolved: ResolvedDependency {
                    hash,
                    manifest,
                    namespace: "test".parse().expect("valid namespace"),
                    name: "dataset".parse().expect("valid name"),
                },
                deps: vec![],
            },
        );

        graph
    }

    fn minimal_config() -> AmpYaml {
        let yaml = r#"
amp: 1.0.0
namespace: test_ns
name: test_dataset
version: 1.0.0
tables: tables
"#;
        AmpYaml::from_yaml(yaml).expect("should parse config")
    }

    fn config_with_functions(functions: &[&str]) -> AmpYaml {
        let mut yaml = String::from(
            "amp: 1.0.0\nnamespace: test_ns\nname: test_dataset\nversion: 1.0.0\ntables: tables\n",
        );

        if !functions.is_empty() {
            yaml.push_str("functions:\n");
            for func in functions {
                yaml.push_str(&format!(
                    "  {func}:\n    input_types: [Int64]\n    output_type: Int64\n    source: functions/{func}.js\n"
                ));
            }
        }

        AmpYaml::from_yaml(&yaml).expect("should parse config")
    }

    #[test]
    fn validate_cli_vars_rejects_unknown_keys() {
        //* Given
        let yaml = r#"
amp: 1.0.0
namespace: test_ns
name: test_dataset
version: 1.0.0
vars:
  network: mainnet
"#;
        let config = AmpYaml::from_yaml(yaml).expect("should parse config");
        let config_path = PathBuf::from("amp.yaml");
        let cli_vars = vec![
            ("network".to_string(), "testnet".to_string()),
            ("chain_id".to_string(), "1".to_string()),
        ];

        //* When
        let result = validate_cli_vars(&config_path, &config, &cli_vars);

        //* Then
        let err = result.expect_err("should reject unknown vars");
        match err {
            ValidationError::UnknownCliVars {
                unknown,
                config_path: path,
            } => {
                assert_eq!(path, config_path);
                assert_eq!(unknown, vec!["chain_id".to_string()]);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn validate_cli_vars_accepts_known_keys() {
        //* Given
        let yaml = r#"
amp: 1.0.0
namespace: test_ns
name: test_dataset
version: 1.0.0
vars:
  network: mainnet
  chain_id: "1"
"#;
        let config = AmpYaml::from_yaml(yaml).expect("should parse config");
        let config_path = PathBuf::from("amp.yaml");
        let cli_vars = vec![
            ("network".to_string(), "testnet".to_string()),
            ("chain_id".to_string(), "10".to_string()),
        ];

        //* When
        let result = validate_cli_vars(&config_path, &config, &cli_vars);

        //* Then
        assert!(result.is_ok(), "should accept declared vars");
    }

    mod function_sources {
        use std::fs;

        use tempfile::TempDir;

        use super::{AmpYaml, ValidationError};
        use crate::validation::validate_function_sources;

        #[test]
        fn accepts_existing_function_sources() {
            //* Given
            let dir = TempDir::new().expect("should create temp dir");
            let functions_dir = dir.path().join("functions");
            fs::create_dir_all(&functions_dir).expect("should create functions dir");
            fs::write(
                functions_dir.join("my_func.js"),
                "function myFunc(x) { return x; }",
            )
            .expect("should write function");

            let yaml = r#"
amp: 1.0.0
namespace: test_ns
name: test_dataset
version: 1.0.0
tables: tables
functions:
  myFunc:
    input_types: [Int64]
    output_type: Int64
    source: functions/my_func.js
"#;

            let config = AmpYaml::from_yaml(yaml).expect("should parse config");

            //* When
            let result = validate_function_sources(dir.path(), &config);

            //* Then
            assert!(result.is_ok(), "function source should be accepted");
        }

        #[test]
        fn rejects_missing_function_source() {
            //* Given
            let dir = TempDir::new().expect("should create temp dir");
            let yaml = r#"
amp: 1.0.0
namespace: test_ns
name: test_dataset
version: 1.0.0
tables: tables
functions:
  missingFunc:
    input_types: [Utf8]
    output_type: Utf8
    source: functions/missing.js
"#;

            let config = AmpYaml::from_yaml(yaml).expect("should parse config");

            //* When
            let result = validate_function_sources(dir.path(), &config);

            //* Then
            let err = result.expect_err("should reject missing function source");
            assert!(matches!(err, ValidationError::FunctionSourceMissing { .. }));
        }
    }

    mod function_types {
        use super::{AmpYaml, ValidationError};
        use crate::validation::validate_function_types;

        #[test]
        fn validate_function_types_rejects_invalid_input_type() {
            //* Given
            let yaml = r#"
amp: 1.0.0
namespace: test_ns
name: test_dataset
version: 1.0.0
tables: tables
functions:
  myFunc:
    input_types: [NotAType]
    output_type: Utf8
    source: functions/my_func.js
"#;
            let config = AmpYaml::from_yaml(yaml).expect("should parse config");

            //* When
            let result = validate_function_types(&config);

            //* Then
            let err = result.expect_err("should reject invalid input type");
            match err {
                ValidationError::InvalidFunctionType { type_str, .. } => {
                    assert_eq!(type_str, "NotAType");
                }
                other => panic!("unexpected error: {other:?}"),
            }
        }

        #[test]
        fn validate_function_types_rejects_invalid_output_type() {
            //* Given
            let yaml = r#"
amp: 1.0.0
namespace: test_ns
name: test_dataset
version: 1.0.0
tables: tables
functions:
  myFunc:
    input_types: [Utf8]
    output_type: NotAType
    source: functions/my_func.js
"#;
            let config = AmpYaml::from_yaml(yaml).expect("should parse config");

            //* When
            let result = validate_function_types(&config);

            //* Then
            let err = result.expect_err("should reject invalid output type");
            match err {
                ValidationError::InvalidFunctionType { type_str, .. } => {
                    assert_eq!(type_str, "NotAType");
                }
                other => panic!("unexpected error: {other:?}"),
            }
        }
    }

    mod reference_validation {
        use super::*;

        #[test]
        fn validate_table_references_rejects_unqualified_table_reference() {
            //* Given
            let config = minimal_config();
            let manifest = make_dependency_manifest("evm-rpc", &["blocks"], &[]);
            let dep_graph = make_dependency_graph("eth", manifest);
            let stmt = parse_statement("SELECT * FROM blocks");
            let table: TableName = "my_table".parse().expect("valid table name");

            //* When
            let result = validate_table_references(&table, &stmt, &config, &dep_graph);

            //* Then
            let err = result.expect_err("should reject unqualified table reference");
            assert!(matches!(
                err,
                ValidationError::UnqualifiedTableReference { .. }
            ));
        }

        #[test]
        fn validate_table_references_rejects_catalog_qualified_table_reference() {
            //* Given
            let config = minimal_config();
            let manifest = make_dependency_manifest("evm-rpc", &["blocks"], &[]);
            let dep_graph = make_dependency_graph("eth", manifest);
            let stmt = parse_statement("SELECT * FROM catalog.schema.table");
            let table: TableName = "my_table".parse().expect("valid table name");

            //* When
            let result = validate_table_references(&table, &stmt, &config, &dep_graph);

            //* Then
            let err = result.expect_err("should reject catalog-qualified table reference");
            assert!(matches!(
                err,
                ValidationError::CatalogQualifiedTableInSql { .. }
            ));
        }

        #[test]
        fn validate_table_references_rejects_unknown_dependency_alias() {
            //* Given
            let config = minimal_config();
            let manifest = make_dependency_manifest("evm-rpc", &["blocks"], &[]);
            let dep_graph = make_dependency_graph("eth", manifest);
            let stmt = parse_statement("SELECT * FROM missing.blocks");
            let table: TableName = "my_table".parse().expect("valid table name");

            //* When
            let result = validate_table_references(&table, &stmt, &config, &dep_graph);

            //* Then
            let err = result.expect_err("should reject unknown dependency alias");
            assert!(matches!(
                err,
                ValidationError::DependencyAliasNotFound { .. }
            ));
        }

        #[test]
        fn validate_table_references_rejects_missing_dependency_table() {
            //* Given
            let config = minimal_config();
            let manifest = make_dependency_manifest("evm-rpc", &["blocks"], &[]);
            let dep_graph = make_dependency_graph("eth", manifest);
            let stmt = parse_statement("SELECT * FROM eth.missing_table");
            let table: TableName = "my_table".parse().expect("valid table name");

            //* When
            let result = validate_table_references(&table, &stmt, &config, &dep_graph);

            //* Then
            let err = result.expect_err("should reject missing dependency table");
            assert!(matches!(
                err,
                ValidationError::TableNotFoundInDependency { .. }
            ));
        }

        #[test]
        fn validate_table_references_accepts_self_function_reference() {
            //* Given
            let config = config_with_functions(&["myFunc"]);
            let dep_graph = DependencyGraph::new();
            let stmt = parse_statement("SELECT self.myFunc(1)");
            let table: TableName = "my_table".parse().expect("valid table name");

            //* When
            let result = validate_table_references(&table, &stmt, &config, &dep_graph);

            //* Then
            assert!(result.is_ok(), "self function reference should be accepted");
        }

        #[test]
        fn validate_table_references_rejects_missing_self_function() {
            //* Given
            let config = minimal_config();
            let dep_graph = DependencyGraph::new();
            let stmt = parse_statement("SELECT self.missingFunc(1)");
            let table: TableName = "my_table".parse().expect("valid table name");

            //* When
            let result = validate_table_references(&table, &stmt, &config, &dep_graph);

            //* Then
            let err = result.expect_err("should reject missing self function");
            assert!(matches!(err, ValidationError::SelfFunctionNotFound { .. }));
        }

        #[test]
        fn validate_table_references_accepts_dependency_function_reference() {
            //* Given
            let config = minimal_config();
            let manifest = make_dependency_manifest("manifest", &["blocks"], &["depFunc"]);
            let dep_graph = make_dependency_graph("dep", manifest);
            let stmt = parse_statement("SELECT dep.depFunc(1)");
            let table: TableName = "my_table".parse().expect("valid table name");

            //* When
            let result = validate_table_references(&table, &stmt, &config, &dep_graph);

            //* Then
            assert!(
                result.is_ok(),
                "dependency function reference should be accepted"
            );
        }

        #[test]
        fn validate_table_references_rejects_missing_dependency_function() {
            //* Given
            let config = minimal_config();
            let manifest = make_dependency_manifest("manifest", &["blocks"], &[]);
            let dep_graph = make_dependency_graph("dep", manifest);
            let stmt = parse_statement("SELECT dep.missingFunc(1)");
            let table: TableName = "my_table".parse().expect("valid table name");

            //* When
            let result = validate_table_references(&table, &stmt, &config, &dep_graph);

            //* Then
            let err = result.expect_err("should reject missing dependency function");
            assert!(matches!(
                err,
                ValidationError::FunctionNotFoundInDependency { .. }
            ));
        }

        #[test]
        fn validate_table_references_accepts_eth_call_for_evm_rpc_dependency() {
            //* Given
            let config = minimal_config();
            let manifest = make_dependency_manifest("evm-rpc", &["blocks"], &[]);
            let dep_graph = make_dependency_graph("dep", manifest);
            let stmt = parse_statement("SELECT dep.eth_call(NULL, NULL, NULL, 'latest') AS result");
            let table: TableName = "my_table".parse().expect("valid table name");

            //* When
            let result = validate_table_references(&table, &stmt, &config, &dep_graph);

            //* Then
            assert!(result.is_ok(), "eth_call should be accepted for evm-rpc");
        }

        #[test]
        fn validate_table_references_rejects_eth_call_for_non_evm_dependency() {
            //* Given
            let config = minimal_config();
            let manifest = make_dependency_manifest("firehose", &["blocks"], &[]);
            let dep_graph = make_dependency_graph("dep", manifest);
            let stmt = parse_statement("SELECT dep.eth_call(NULL, NULL, NULL, 'latest') AS result");
            let table: TableName = "my_table".parse().expect("valid table name");

            //* When
            let result = validate_table_references(&table, &stmt, &config, &dep_graph);

            //* Then
            let err = result.expect_err("should reject eth_call for non-evm dependency");
            assert!(matches!(err, ValidationError::EthCallNotAvailable { .. }));
        }
    }

    mod plan_validation {
        use super::*;

        #[tokio::test]
        async fn validate_incremental_constraints_rejects_underscore_alias() {
            //* Given
            let schema_ctx = SchemaContext::new();
            let stmt = parse_statement("SELECT 1 AS _bad");
            let table: TableName = "model".parse().expect("valid table name");
            let select_info =
                crate::query::validate_select(&stmt, table.clone()).expect("valid select");

            //* When
            let result =
                validate_incremental_constraints(&select_info.query, &schema_ctx, &table).await;

            //* Then
            let err = result.expect_err("should reject underscore-prefixed alias");
            assert!(matches!(err, ValidationError::SqlPlanning { .. }));
        }

        #[tokio::test]
        async fn validate_incremental_constraints_accepts_union_all() {
            //* Given
            let schema_ctx = SchemaContext::new();
            // UNION ALL is allowed; plain UNION implies DISTINCT which is not incremental
            let stmt = parse_statement("SELECT 1 AS id UNION ALL SELECT 2 AS id");
            let table: TableName = "model".parse().expect("valid table name");
            let select_info =
                crate::query::validate_select(&stmt, table.clone()).expect("valid select");

            //* When
            let result =
                validate_incremental_constraints(&select_info.query, &schema_ctx, &table).await;

            //* Then
            assert!(
                result.is_ok(),
                "union all should be allowed: {:?}",
                result.err()
            );
        }

        #[tokio::test]
        async fn validate_incremental_constraints_rejects_union_distinct() {
            //* Given
            let schema_ctx = SchemaContext::new();
            // Plain UNION implies DISTINCT which is not incremental
            let stmt = parse_statement("SELECT 1 AS id UNION SELECT 2 AS id");
            let table: TableName = "model".parse().expect("valid table name");
            let select_info =
                crate::query::validate_select(&stmt, table.clone()).expect("valid select");

            //* When
            let result =
                validate_incremental_constraints(&select_info.query, &schema_ctx, &table).await;

            //* Then
            let err = result.expect_err("union (distinct) should be rejected");
            assert!(matches!(err, ValidationError::NonIncremental { .. }));
        }
    }
}
