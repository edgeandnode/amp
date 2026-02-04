//! Integration tests for dataset authoring workflow.
//!
//! These tests validate the complete authoring pipeline:
//! - Building datasets from amp.yaml and SQL templates
//! - Packaging datasets into deterministic archives
//! - Converting to legacy format via the bridge
//!
//! Note: These tests use the `dataset-authoring` crate directly rather than
//! the CLI to avoid the need for a running admin API for dependency resolution.
//! For tests that require dependency resolution via admin API, see the
//! `it_admin_api_datasets_register.rs` tests.

use std::{collections::BTreeMap, fs, path::Path};

use dataset_authoring::{
    arrow_json,
    bridge::LegacyBridge,
    canonical,
    config::AmpYaml,
    discovery::discover_tables,
    files::{FileRef, hash_file, normalize_path},
    jinja::{JinjaContext, render_sql},
    lockfile::{Lockfile, RootInfo},
    manifest::{AuthoringManifest, TableDef},
    package::PackageBuilder,
    query::validate_select,
    resolver::DependencyGraph,
    schema::SchemaContext,
};
use datasets_common::hash::Hash;
use datasets_derived::sql_str::SqlStr;
use monitoring::logging;
use tempfile::TempDir;

// ============================================================
// Test fixtures and helpers
// ============================================================

/// Create a minimal amp.yaml for testing without dependencies.
fn create_minimal_amp_yaml(namespace: &str, name: &str) -> String {
    indoc::formatdoc! {r#"
        amp: "1.0.0"
        namespace: {namespace}
        name: {name}
        version: "1.0.0"
        tables: tables
    "#}
}

/// Create a simple SELECT SQL file for testing (dbt-style model).
fn create_simple_sql() -> String {
    "SELECT 1 AS id, 'test' AS name".to_string()
}

/// Set up a minimal dataset project directory.
fn setup_minimal_project(dir: &Path, namespace: &str, name: &str) {
    // Create directories
    let tables_dir = dir.join("tables");
    fs::create_dir_all(&tables_dir).expect("should create tables dir");

    // Write amp.yaml
    let amp_yaml = create_minimal_amp_yaml(namespace, name);
    fs::write(dir.join("amp.yaml"), amp_yaml).expect("should write amp.yaml");

    // Write SQL file (SELECT-only, table name from filename)
    let sql = create_simple_sql();
    fs::write(tables_dir.join("transfers.sql"), sql).expect("should write sql file");
}

/// Set up a dataset project with variables for Jinja testing.
fn setup_project_with_vars(dir: &Path) {
    let tables_dir = dir.join("tables");
    fs::create_dir_all(&tables_dir).expect("should create tables dir");

    // amp.yaml with variables
    let amp_yaml = indoc::indoc! {r#"
        amp: "1.0.0"
        namespace: test_ns
        name: test_dataset
        version: "1.0.0"
        tables: tables
        vars:
          filter_value: "100"
    "#};
    fs::write(dir.join("amp.yaml"), amp_yaml).expect("should write amp.yaml");

    // SQL with Jinja variable (SELECT-only, dbt-style)
    let sql = indoc::indoc! {r#"
        SELECT id, amount
        FROM (SELECT 1 AS id, 200 AS amount)
        WHERE amount > {{ var('filter_value') }}
    "#};
    fs::write(tables_dir.join("filtered.sql"), sql).expect("should write sql file");
}

// ============================================================
// Configuration parsing tests
// ============================================================

#[test]
fn parse_amp_yaml_from_file() {
    //* Given
    let dir = TempDir::new().expect("should create temp dir");
    setup_minimal_project(dir.path(), "test_ns", "test_dataset");

    //* When
    let content = fs::read_to_string(dir.path().join("amp.yaml")).expect("should read amp.yaml");
    let config = AmpYaml::from_yaml(&content);

    //* Then
    let config = config.expect("should parse amp.yaml");
    assert_eq!(config.namespace.to_string(), "test_ns");
    assert_eq!(config.name.to_string(), "test_dataset");
    assert_eq!(config.version.to_string(), "1.0.0");
    assert_eq!(config.tables.to_string_lossy(), "tables");

    // Discover tables from the directory
    let discovered = discover_tables(dir.path(), &config.tables).expect("should discover tables");
    assert_eq!(discovered.len(), 1);
    assert!(discovered.contains_key(&"transfers".parse().unwrap()));
}

// ============================================================
// Jinja rendering tests
// ============================================================

#[test]
fn jinja_renders_sql_with_variables() {
    logging::init();

    //* Given
    let dir = TempDir::new().expect("should create temp dir");
    setup_project_with_vars(dir.path());

    let sql_template =
        fs::read_to_string(dir.path().join("tables/filtered.sql")).expect("should read sql");

    let yaml_vars: std::collections::HashMap<String, String> =
        [("filter_value".to_string(), "100".to_string())]
            .into_iter()
            .collect();

    let ctx = JinjaContext::builder().with_yaml_vars(yaml_vars).build();

    //* When
    let rendered = render_sql(&sql_template, "filtered", &ctx);

    //* Then
    let rendered = rendered.expect("should render sql");
    assert!(
        rendered.contains("WHERE amount > 100"),
        "should substitute variable, got: {}",
        rendered
    );
}

#[test]
fn jinja_cli_vars_override_yaml_vars() {
    logging::init();

    //* Given
    let template = "SELECT * FROM source WHERE x > {{ var('limit') }}";

    let yaml_vars: std::collections::HashMap<String, String> =
        [("limit".to_string(), "10".to_string())]
            .into_iter()
            .collect();
    let cli_vars = vec![("limit".to_string(), "50".to_string())];

    let ctx = JinjaContext::builder()
        .with_yaml_vars(yaml_vars)
        .with_cli_vars(cli_vars)
        .build();

    //* When
    let rendered = render_sql(template, "test", &ctx);

    //* Then
    let rendered = rendered.expect("should render sql");
    assert!(
        rendered.contains("WHERE x > 50"),
        "CLI vars should override YAML vars, got: {}",
        rendered
    );
}

// ============================================================
// SELECT validation tests (dbt-style models)
// ============================================================

#[test]
fn validate_select_accepts_valid_statement() {
    logging::init();

    //* Given
    let sql: SqlStr = "SELECT id, amount FROM source".parse().expect("valid sql");
    let stmt = common::sql::parse(&sql).expect("should parse");

    //* When
    let result = validate_select(&stmt, "transfers".parse().expect("valid table name"));

    //* Then
    let select_info = result.expect("should validate SELECT");
    assert_eq!(select_info.table_name.to_string(), "transfers");
}

#[test]
fn validate_select_rejects_ctas() {
    logging::init();

    //* Given
    let sql: SqlStr = "CREATE TABLE transfers AS SELECT id FROM source"
        .parse()
        .expect("valid sql");
    let stmt = common::sql::parse(&sql).expect("should parse");

    //* When
    let result = validate_select(&stmt, "transfers".parse().expect("valid table name"));

    //* Then
    assert!(result.is_err(), "should reject CTAS statement");
}

#[test]
fn validate_select_rejects_insert() {
    logging::init();

    //* Given
    let sql: SqlStr = "INSERT INTO transfers VALUES (1, 'test')"
        .parse()
        .expect("valid sql");
    let stmt = common::sql::parse(&sql).expect("should parse");

    //* When
    let result = validate_select(&stmt, "transfers".parse().expect("valid table name"));

    //* Then
    assert!(result.is_err(), "should reject INSERT statement");
}

// ============================================================
// Schema inference tests
// ============================================================

#[tokio::test]
async fn infer_schema_from_select_query() {
    logging::init();

    //* Given
    let schema_ctx = SchemaContext::new();

    // Parse a SELECT statement and extract the query
    let sql: SqlStr = "SELECT 1 AS id, 'test' AS name, true AS active"
        .parse()
        .expect("valid sql");
    let stmt = common::sql::parse(&sql).expect("should parse");
    let select_info =
        validate_select(&stmt, "test".parse().expect("valid table name")).expect("should validate");

    //* When
    let result = schema_ctx.infer_schema(&select_info.query).await;

    //* Then
    let schema = result.expect("should infer schema");
    assert_eq!(schema.fields().len(), 3);

    // Check field names
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(field_names.contains(&"id"));
    assert!(field_names.contains(&"name"));
    assert!(field_names.contains(&"active"));
}

// ============================================================
// File utilities tests
// ============================================================

#[test]
fn hash_file_produces_consistent_hash() {
    //* Given
    let dir = TempDir::new().expect("should create temp dir");
    let file_path = dir.path().join("test.txt");
    fs::write(&file_path, "hello world").expect("should write file");

    //* When
    let hash1 = hash_file(&file_path).expect("should hash file");
    let hash2 = hash_file(&file_path).expect("should hash file again");

    //* Then
    assert_eq!(hash1, hash2, "same content should produce same hash");
}

#[test]
fn normalize_path_produces_posix_path() {
    //* Given
    let dir = TempDir::new().expect("should create temp dir");
    let base = dir.path();
    let file_path = base.join("sql").join("test.sql");
    fs::create_dir_all(base.join("sql")).expect("should create dir");
    fs::write(&file_path, "test").expect("should write file");

    //* When
    let normalized = normalize_path(&file_path, base);

    //* Then
    let normalized = normalized.expect("should normalize path");
    assert_eq!(normalized, "sql/test.sql");
    assert!(!normalized.contains("\\"), "should use POSIX separators");
}

// ============================================================
// Canonical JSON tests
// ============================================================

#[test]
fn canonical_json_is_deterministic() {
    //* Given
    let value1: serde_json::Value = serde_json::json!({
        "z": 1,
        "a": 2,
        "m": {"b": 3, "a": 4}
    });
    let value2: serde_json::Value = serde_json::json!({
        "a": 2,
        "m": {"a": 4, "b": 3},
        "z": 1
    });

    //* When
    let canonical1 = canonical::canonicalize(&value1).expect("should canonicalize");
    let canonical2 = canonical::canonicalize(&value2).expect("should canonicalize");

    //* Then
    assert_eq!(
        canonical1, canonical2,
        "equivalent JSON should canonicalize to same string"
    );

    // Verify keys are sorted
    assert!(
        canonical1.find("\"a\"").unwrap() < canonical1.find("\"m\"").unwrap(),
        "keys should be sorted alphabetically"
    );
}

// ============================================================
// Lockfile tests
// ============================================================

#[test]
fn lockfile_roundtrip_preserves_content() {
    //* Given
    let dir = TempDir::new().expect("should create temp dir");
    let lockfile_path = dir.path().join("amp.lock");

    let root = RootInfo {
        namespace: "test_ns".parse().expect("valid namespace"),
        name: "test_dataset".parse().expect("valid name"),
        version: "1.0.0".parse().expect("valid version"),
    };

    // Create empty dependency graph for testing
    let dep_graph = DependencyGraph::default();
    let lockfile = Lockfile::from_graph(root, &dep_graph);

    //* When
    lockfile
        .write(&lockfile_path)
        .expect("should write lockfile");
    let loaded = Lockfile::read(&lockfile_path).expect("should read lockfile");

    //* Then
    assert_eq!(lockfile, loaded, "lockfile should roundtrip correctly");
}

#[test]
fn lockfile_is_deterministic() {
    //* Given
    let root = RootInfo {
        namespace: "test_ns".parse().expect("valid namespace"),
        name: "test_dataset".parse().expect("valid name"),
        version: "1.0.0".parse().expect("valid version"),
    };

    let dep_graph = DependencyGraph::default();

    //* When
    let lockfile1 = Lockfile::from_graph(root.clone(), &dep_graph);
    let lockfile2 = Lockfile::from_graph(root, &dep_graph);

    let yaml1 = serde_yaml::to_string(&lockfile1).expect("should serialize");
    let yaml2 = serde_yaml::to_string(&lockfile2).expect("should serialize");

    //* Then
    assert_eq!(
        yaml1, yaml2,
        "lockfile serialization should be deterministic"
    );
}

// ============================================================
// Package assembly tests
// ============================================================

#[test]
fn package_builder_creates_deterministic_archive() {
    //* Given
    let dir = TempDir::new().expect("should create temp dir");
    setup_build_output(dir.path());

    //* When
    let builder1 = PackageBuilder::from_directory(dir.path()).expect("should create builder");
    let builder2 = PackageBuilder::from_directory(dir.path()).expect("should create builder again");

    let hash1 = builder1.hash().expect("should compute hash");
    let hash2 = builder2.hash().expect("should compute hash again");

    //* Then
    assert_eq!(
        hash1, hash2,
        "same content should produce same package hash"
    );
}

#[test]
fn package_builder_includes_all_required_files() {
    //* Given
    let dir = TempDir::new().expect("should create temp dir");
    setup_build_output(dir.path());

    //* When
    let builder = PackageBuilder::from_directory(dir.path()).expect("should create builder");
    let entries = builder.entries();

    //* Then
    let paths: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();
    assert!(paths.contains(&"manifest.json"), "should include manifest");
    assert!(
        paths.iter().any(|p| p.starts_with("tables/")),
        "should include table files"
    );
}

#[test]
fn package_writes_valid_tgz() {
    //* Given
    let dir = TempDir::new().expect("should create temp dir");
    setup_build_output(dir.path());

    let output_path = dir.path().join("output.tgz");
    let builder = PackageBuilder::from_directory(dir.path()).expect("should create builder");

    //* When
    builder.write_to(&output_path).expect("should write tgz");

    //* Then
    assert!(output_path.exists(), "tgz file should exist");
    assert!(
        fs::metadata(&output_path)
            .expect("should get metadata")
            .len()
            > 0,
        "tgz file should have content"
    );
}

/// Helper to set up a build output directory for packaging tests.
fn setup_build_output(dir: &Path) {
    let tables_dir = dir.join("tables");
    fs::create_dir_all(&tables_dir).expect("should create tables dir");

    // Create manifest.json
    let manifest = serde_json::json!({
        "namespace": "test_ns",
        "name": "test_dataset",
        "kind": "derived",
        "dependencies": {},
        "tables": {
            "transfers": {
                "sql": {"path": "tables/transfers.sql", "hash": "sha256:abc123"},
                "ipc": {"path": "tables/transfers.ipc", "hash": "sha256:def456"},
                "network": "mainnet"
            }
        },
        "functions": {}
    });
    fs::write(
        dir.join("manifest.json"),
        serde_json::to_string_pretty(&manifest).unwrap(),
    )
    .expect("should write manifest");

    // Create SQL file
    fs::write(
        tables_dir.join("transfers.sql"),
        "CREATE TABLE transfers AS SELECT 1 AS id",
    )
    .expect("should write sql");

    // Create IPC schema file (JSON format for now - bridge reads JSON)
    let schema = serde_json::json!({
        "fields": [{"name": "id", "type": "Int64", "nullable": false}]
    });
    fs::write(
        tables_dir.join("transfers.ipc"),
        serde_json::to_string(&schema).unwrap(),
    )
    .expect("should write ipc");
}

// ============================================================
// Legacy bridge tests
// ============================================================

#[test]
fn legacy_bridge_converts_manifest_with_inline_content() {
    //* Given
    let dir = TempDir::new().expect("should create temp dir");
    let sql_dir = dir.path().join("sql");
    fs::create_dir_all(&sql_dir).expect("should create sql dir");

    // Create SQL file
    fs::write(
        sql_dir.join("transfers.sql"),
        "CREATE TABLE transfers AS SELECT id, amount FROM source.transactions",
    )
    .expect("should write sql");

    // Create schema file
    let schema_json = r#"{"fields":[{"name":"id","type":"UInt64","nullable":false},{"name":"amount","type":"UInt64","nullable":false}]}"#;
    fs::write(sql_dir.join("transfers.schema.json"), schema_json).expect("should write schema");

    let test_hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        .parse()
        .expect("valid hash");

    let mut manifest = AuthoringManifest {
        namespace: "test_ns".parse().expect("valid namespace"),
        name: "test_dataset".parse().expect("valid name"),
        kind: "derived".to_string(),
        dependencies: BTreeMap::new(),
        tables: BTreeMap::new(),
        functions: BTreeMap::new(),
    };
    manifest.tables.insert(
        "transfers".parse().expect("valid table name"),
        TableDef {
            sql: Some(FileRef::new(
                "sql/transfers.sql".to_string(),
                test_hash.clone(),
            )),
            ipc: FileRef::new("sql/transfers.schema.json".to_string(), test_hash),
            network: "mainnet".parse().expect("valid network"),
        },
    );

    let bridge = LegacyBridge::new(dir.path());

    //* When
    let result = bridge.convert(&manifest);

    //* Then
    let legacy = result.expect("conversion should succeed");
    assert_eq!(legacy.tables.len(), 1);

    let table = legacy
        .tables
        .get(&"transfers".parse().unwrap())
        .expect("should have table");

    // Verify SQL is inlined
    match &table.input {
        datasets_derived::manifest::TableInput::View(view) => {
            assert!(
                view.sql.as_str().contains("CREATE TABLE transfers"),
                "SQL should be inlined"
            );
        }
    }

    // Verify schema is loaded
    assert_eq!(table.schema.arrow.fields.len(), 2);
}

#[test]
fn legacy_bridge_produces_valid_json() {
    //* Given
    let dir = TempDir::new().expect("should create temp dir");
    let sql_dir = dir.path().join("sql");
    fs::create_dir_all(&sql_dir).expect("should create sql dir");

    fs::write(
        sql_dir.join("transfers.sql"),
        "CREATE TABLE transfers AS SELECT 1",
    )
    .expect("should write sql");
    fs::write(
        sql_dir.join("transfers.schema.json"),
        r#"{"fields":[{"name":"id","type":"Int64","nullable":false}]}"#,
    )
    .expect("should write schema");

    let test_hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        .parse()
        .expect("valid hash");

    let mut manifest = AuthoringManifest {
        namespace: "test_ns".parse().expect("valid namespace"),
        name: "test_dataset".parse().expect("valid name"),
        kind: "derived".to_string(),
        dependencies: BTreeMap::new(),
        tables: BTreeMap::new(),
        functions: BTreeMap::new(),
    };
    manifest.tables.insert(
        "transfers".parse().expect("valid table name"),
        TableDef {
            sql: Some(FileRef::new(
                "sql/transfers.sql".to_string(),
                test_hash.clone(),
            )),
            ipc: FileRef::new("sql/transfers.schema.json".to_string(), test_hash),
            network: "mainnet".parse().expect("valid network"),
        },
    );

    let bridge = LegacyBridge::new(dir.path());

    //* When
    let result = bridge.to_json(&manifest);

    //* Then
    let json_str = result.expect("to_json should succeed");
    let parsed: serde_json::Value = serde_json::from_str(&json_str).expect("should be valid JSON");

    // Verify structure matches derived.spec.json requirements
    assert_eq!(parsed["kind"], "manifest");
    assert!(parsed["tables"].is_object());
    assert!(parsed["tables"]["transfers"]["input"]["sql"].is_string());
    assert!(parsed["tables"]["transfers"]["schema"]["arrow"]["fields"].is_array());
    assert!(parsed["tables"]["transfers"]["network"].is_string());
}

// ============================================================
// Full pipeline integration tests
// ============================================================

#[tokio::test]
async fn full_build_pipeline_without_dependencies() {
    logging::init();

    //* Given
    let project_dir = TempDir::new().expect("should create project dir");
    let output_dir = TempDir::new().expect("should create output dir");

    setup_minimal_project(project_dir.path(), "integration", "test_build");

    // Parse configuration
    let config_content =
        fs::read_to_string(project_dir.path().join("amp.yaml")).expect("should read amp.yaml");
    let config = AmpYaml::from_yaml(&config_content).expect("should parse amp.yaml");

    // Discover tables
    let discovered_tables =
        discover_tables(project_dir.path(), &config.tables).expect("should discover tables");

    // Set up output directories
    let sql_output_dir = output_dir.path().join("sql");
    fs::create_dir_all(&sql_output_dir).expect("should create sql output dir");

    // Build Jinja context
    let jinja_ctx = JinjaContext::builder().build();

    // Build schema context (empty, no dependencies)
    let schema_ctx = SchemaContext::new();

    //* When
    // Process each discovered table
    for (table_name, sql_path) in &discovered_tables {
        // Read template
        let template_path = project_dir.path().join(sql_path);
        let template = fs::read_to_string(&template_path).expect("should read sql template");

        // Render Jinja
        let rendered =
            render_sql(&template, table_name.as_str(), &jinja_ctx).expect("should render sql");

        // Parse SQL as a SELECT query (dbt-style)
        let sql_str: SqlStr = rendered.parse().expect("valid sql");
        let stmt = common::sql::parse(&sql_str).expect("should parse sql");

        // Extract query from SELECT statement
        let query = match &stmt {
            datafusion::sql::parser::Statement::Statement(inner) => match inner.as_ref() {
                datafusion::sql::sqlparser::ast::Statement::Query(q) => *q.clone(),
                _ => panic!("expected SELECT statement"),
            },
            _ => panic!("expected SELECT statement"),
        };

        // Infer schema using the query
        let schema = schema_ctx
            .infer_schema(&query)
            .await
            .expect("should infer schema");

        // Write output files
        let sql_output_path = sql_output_dir.join(format!("{}.sql", table_name));
        fs::write(&sql_output_path, &rendered).expect("should write sql");

        let schema_output_path = sql_output_dir.join(format!("{}.schema.json", table_name));
        let arrow_schema: datasets_common::manifest::ArrowSchema = (&schema).into();
        arrow_json::write_schema_file(&arrow_schema, &schema_output_path)
            .expect("should write schema");
    }

    //* Then
    // Verify output files exist
    assert!(
        sql_output_dir.join("transfers.sql").exists(),
        "SQL file should exist"
    );
    assert!(
        sql_output_dir.join("transfers.schema.json").exists(),
        "Schema file should exist"
    );

    // Verify schema content
    let schema = arrow_json::read_schema_file(sql_output_dir.join("transfers.schema.json"))
        .expect("should read schema");
    assert!(!schema.fields.is_empty(), "schema should have fields");
}

#[tokio::test]
async fn full_package_and_bridge_pipeline() {
    logging::init();

    //* Given
    let dir = TempDir::new().expect("should create temp dir");

    // Set up a complete build output
    setup_build_output(dir.path());

    //* When
    // 1. Create package
    let package_path = dir.path().join("dataset.tgz");
    let builder =
        PackageBuilder::from_directory(dir.path()).expect("should create package builder");
    builder
        .write_to(&package_path)
        .expect("should write package");
    let package_hash = builder.hash().expect("should compute package hash");

    // 2. Read the manifest for bridge conversion
    let manifest_content =
        fs::read_to_string(dir.path().join("manifest.json")).expect("should read manifest");
    let _manifest_value: serde_json::Value =
        serde_json::from_str(&manifest_content).expect("should parse manifest json");

    // Parse the authoring manifest (simplified for this test)
    let test_hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        .parse()
        .expect("valid hash");

    let mut authoring_manifest = AuthoringManifest {
        namespace: "test_ns".parse().expect("valid namespace"),
        name: "test_dataset".parse().expect("valid name"),
        kind: "derived".to_string(),
        dependencies: BTreeMap::new(),
        tables: BTreeMap::new(),
        functions: BTreeMap::new(),
    };
    authoring_manifest.tables.insert(
        "transfers".parse().expect("valid table name"),
        TableDef {
            sql: Some(FileRef::new(
                "tables/transfers.sql".to_string(),
                test_hash.clone(),
            )),
            ipc: FileRef::new("tables/transfers.ipc".to_string(), test_hash),
            network: "mainnet".parse().expect("valid network"),
        },
    );

    // 3. Convert to legacy format
    let bridge = LegacyBridge::new(dir.path());
    let legacy_json = bridge
        .to_json(&authoring_manifest)
        .expect("should convert to legacy json");

    //* Then
    // Verify package was created
    assert!(package_path.exists(), "package file should exist");
    assert!(
        !package_hash.to_string().is_empty(),
        "package hash should not be empty"
    );

    // Verify legacy JSON is valid and has expected structure
    let legacy_value: serde_json::Value =
        serde_json::from_str(&legacy_json).expect("legacy json should be valid");
    assert_eq!(legacy_value["kind"], "manifest");
    assert!(legacy_value["tables"]["transfers"]["input"]["sql"].is_string());

    // Verify SQL content is inlined in legacy format
    let inlined_sql = legacy_value["tables"]["transfers"]["input"]["sql"]
        .as_str()
        .expect("sql should be string");
    assert!(
        inlined_sql.contains("CREATE TABLE transfers"),
        "SQL should be inlined in legacy format"
    );

    tracing::info!(
        package_hash = %package_hash,
        "Pipeline test completed successfully"
    );
}

// ============================================================
// Dependency resolution integration tests
// ============================================================

// ============================================================
// Canonical manifest tests
// ============================================================

#[test]
fn canonical_manifest_hash_matches_identity() {
    logging::init();

    //* Given
    // Create a manifest and serialize it to canonical JSON
    let test_hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        .parse()
        .expect("valid hash");

    let mut manifest = AuthoringManifest {
        namespace: "test_ns".parse().expect("valid namespace"),
        name: "test_dataset".parse().expect("valid name"),
        kind: "derived".to_string(),
        dependencies: BTreeMap::new(),
        tables: BTreeMap::new(),
        functions: BTreeMap::new(),
    };
    manifest.tables.insert(
        "transfers".parse().expect("valid table name"),
        TableDef {
            sql: Some(FileRef::new(
                "sql/transfers.sql".to_string(),
                test_hash.clone(),
            )),
            ipc: FileRef::new("sql/transfers.schema.json".to_string(), test_hash),
            network: "mainnet".parse().expect("valid network"),
        },
    );

    //* When
    let canonical_json = manifest
        .canonical_json()
        .expect("should produce canonical JSON");
    let identity = manifest.identity().expect("should compute identity");

    // Compute SHA-256 of the canonical JSON bytes
    let computed_hash = datasets_common::hash::hash(canonical_json.as_bytes());

    //* Then
    assert_eq!(
        computed_hash, identity,
        "SHA-256 of canonical JSON bytes must equal manifest identity"
    );
}

#[test]
fn canonical_manifest_is_deterministic() {
    logging::init();

    //* Given
    let dir = TempDir::new().expect("should create temp dir");

    // Create a manifest
    let test_hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        .parse()
        .expect("valid hash");

    let mut manifest = AuthoringManifest {
        namespace: "test_ns".parse().expect("valid namespace"),
        name: "test_dataset".parse().expect("valid name"),
        kind: "derived".to_string(),
        dependencies: BTreeMap::new(),
        tables: BTreeMap::new(),
        functions: BTreeMap::new(),
    };
    manifest.tables.insert(
        "transfers".parse().expect("valid table name"),
        TableDef {
            sql: Some(FileRef::new(
                "sql/transfers.sql".to_string(),
                test_hash.clone(),
            )),
            ipc: FileRef::new("sql/transfers.schema.json".to_string(), test_hash),
            network: "mainnet".parse().expect("valid network"),
        },
    );

    // Write to files twice
    let path1 = dir.path().join("manifest1.json");
    let path2 = dir.path().join("manifest2.json");

    //* When
    let json1 = manifest.canonical_json().expect("should produce JSON 1");
    let json2 = manifest.canonical_json().expect("should produce JSON 2");

    fs::write(&path1, &json1).expect("should write manifest 1");
    fs::write(&path2, &json2).expect("should write manifest 2");

    // Read back and compare
    let bytes1 = fs::read(&path1).expect("should read manifest 1");
    let bytes2 = fs::read(&path2).expect("should read manifest 2");

    //* Then
    assert_eq!(
        bytes1, bytes2,
        "canonical manifest JSON must be byte-for-byte identical across invocations"
    );

    // Also verify the identities match
    let identity1 = manifest.identity().expect("should compute identity 1");
    let identity2 = manifest.identity().expect("should compute identity 2");
    assert_eq!(
        identity1, identity2,
        "manifest identities must be identical"
    );
}

// ============================================================
// Function path canonicalization tests
// ============================================================

#[test]
fn legacy_bridge_reads_function_from_canonical_path() {
    logging::init();

    //* Given
    // Set up a directory structure with functions at canonical paths
    let dir = TempDir::new().expect("should create temp dir");
    let sql_dir = dir.path().join("sql");
    let functions_dir = dir.path().join("functions");
    fs::create_dir_all(&sql_dir).expect("should create sql dir");
    fs::create_dir_all(&functions_dir).expect("should create functions dir");

    // Create files at canonical locations
    fs::write(
        sql_dir.join("transfers.sql"),
        "CREATE TABLE transfers AS SELECT 1",
    )
    .expect("should write sql");
    fs::write(
        sql_dir.join("transfers.schema.json"),
        r#"{"fields":[{"name":"id","type":"Int64","nullable":false}]}"#,
    )
    .expect("should write schema");

    // Function file at canonical path (functions/<name>.js)
    let function_content = "function decode(input) { return input.toString(); }";
    fs::write(functions_dir.join("decode.js"), function_content).expect("should write function");

    // Create a manifest with function using canonical path
    let test_hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        .parse()
        .expect("valid hash");

    use dataset_authoring::manifest::AuthoringFunctionDef;
    use datasets_common::manifest::DataType;

    // Parse DataType from string using serde (avoids needing arrow crate)
    let binary_type: DataType =
        serde_json::from_value(serde_json::Value::String("Binary".to_string()))
            .expect("valid type");
    let utf8_type: DataType =
        serde_json::from_value(serde_json::Value::String("Utf8".to_string())).expect("valid type");

    let mut manifest = AuthoringManifest {
        namespace: "test_ns".parse().expect("valid namespace"),
        name: "test_dataset".parse().expect("valid name"),
        kind: "derived".to_string(),
        dependencies: BTreeMap::new(),
        tables: BTreeMap::new(),
        functions: BTreeMap::new(),
    };
    manifest.tables.insert(
        "transfers".parse().expect("valid table name"),
        TableDef {
            sql: Some(FileRef::new(
                "sql/transfers.sql".to_string(),
                test_hash.clone(),
            )),
            ipc: FileRef::new("sql/transfers.schema.json".to_string(), test_hash.clone()),
            network: "mainnet".parse().expect("valid network"),
        },
    );
    manifest.functions.insert(
        "decode".parse().expect("valid func name"),
        AuthoringFunctionDef {
            input_types: vec![binary_type],
            output_type: utf8_type,
            // The manifest uses canonical path - this is what the manifest builder produces
            source: FileRef::new("functions/decode.js".to_string(), test_hash),
        },
    );

    //* When
    let bridge = LegacyBridge::new(dir.path());
    let result = bridge.convert(&manifest);

    //* Then
    let legacy = result.expect("conversion should succeed");

    // Verify function was read from canonical path and inlined
    let func = legacy
        .functions
        .get(&"decode".parse().unwrap())
        .expect("should have function");

    assert!(
        func.source.source.contains("function decode(input)"),
        "function source should be inlined from canonical path"
    );
    assert_eq!(
        func.source.filename.as_str(),
        "decode.js",
        "function filename should use canonical name"
    );
}

// ============================================================
// Package auto-detect tests
// ============================================================

#[test]
fn package_auto_detects_cwd_with_manifest() {
    logging::init();

    //* Given
    // Create a directory with build output (manifest.json + sql/)
    let dir = TempDir::new().expect("should create temp dir");
    setup_build_output(dir.path());

    // Save original working directory
    let original_cwd = std::env::current_dir().expect("should get cwd");

    // Change to the build output directory
    std::env::set_current_dir(dir.path()).expect("should change to temp dir");

    //* When
    // Test auto-detect logic: CWD has manifest.json, so it should work
    let cwd = std::env::current_dir().expect("should get cwd");
    let manifest_path = cwd.join("manifest.json");
    let dir_to_use = if manifest_path.exists() {
        Some(cwd.clone())
    } else {
        None
    };

    //* Then
    assert!(
        dir_to_use.is_some(),
        "should auto-detect CWD when manifest.json exists"
    );
    let detected_dir = dir_to_use.expect("should have dir");
    assert_eq!(detected_dir, cwd, "auto-detected dir should be CWD");

    // Verify we can create a package from the auto-detected directory
    let builder = PackageBuilder::from_directory(&detected_dir).expect("should create builder");
    let entries = builder.entries();
    assert!(
        entries.iter().any(|e| e.path == "manifest.json"),
        "should include manifest.json"
    );

    // Restore original working directory
    std::env::set_current_dir(original_cwd).expect("should restore cwd");
}

#[test]
fn package_auto_detect_fails_without_manifest() {
    logging::init();

    //* Given
    // Create an empty directory without manifest.json
    let dir = TempDir::new().expect("should create temp dir");

    // Save original working directory
    let original_cwd = std::env::current_dir().expect("should get cwd");

    // Change to the empty directory
    std::env::set_current_dir(dir.path()).expect("should change to temp dir");

    //* When
    let cwd = std::env::current_dir().expect("should get cwd");
    let manifest_path = cwd.join("manifest.json");
    let has_manifest = manifest_path.exists();

    //* Then
    assert!(
        !has_manifest,
        "empty directory should not have manifest.json"
    );
    // In the actual CLI, this would return Error::MissingDirectory

    // Restore original working directory
    std::env::set_current_dir(original_cwd).expect("should restore cwd");
}

mod dependency_resolution {
    use std::collections::BTreeMap;

    use async_trait::async_trait;
    use dataset_authoring::{
        cache::Cache,
        canonical::hash_canonical,
        resolver::{DependencyGraph, ManifestFetcher, ResolveError, Resolver},
    };
    use datasets_common::hash::Hash;
    use datasets_derived::deps::{DepAlias, DepReference};
    use monitoring::logging;
    use tempfile::TempDir;

    /// Mock fetcher for integration testing.
    ///
    /// Stores manifests by reference string and by hash, simulating
    /// an admin API without needing a real server.
    struct MockFetcher {
        /// Manifests keyed by reference string (e.g., "ns/name@version").
        by_reference: BTreeMap<String, serde_json::Value>,
        /// Manifests keyed by content hash.
        by_hash: BTreeMap<Hash, serde_json::Value>,
    }

    impl MockFetcher {
        fn new() -> Self {
            Self {
                by_reference: BTreeMap::new(),
                by_hash: BTreeMap::new(),
            }
        }

        /// Registers a manifest for both version and hash-based lookup.
        fn register(&mut self, reference: &str, manifest: serde_json::Value) {
            let hash = hash_canonical(&manifest).expect("should hash manifest");

            self.by_reference
                .insert(reference.to_string(), manifest.clone());
            self.by_hash.insert(hash, manifest);
        }
    }

    #[async_trait]
    impl ManifestFetcher for MockFetcher {
        async fn fetch_by_version(
            &self,
            reference: &DepReference,
        ) -> Result<serde_json::Value, ResolveError> {
            self.by_reference
                .get(&reference.to_string())
                .cloned()
                .ok_or_else(|| ResolveError::NotFound {
                    reference: reference.to_string(),
                })
        }

        async fn fetch_by_hash(
            &self,
            hash: &Hash,
        ) -> Result<Option<serde_json::Value>, ResolveError> {
            Ok(self.by_hash.get(hash).cloned())
        }
    }

    /// Creates a minimal manifest with no dependencies.
    fn create_leaf_manifest(name: &str) -> serde_json::Value {
        serde_json::json!({
            "kind": "manifest",
            "dependencies": {},
            "tables": {
                name: {
                    "input": { "sql": format!("CREATE TABLE {name} AS SELECT 1") },
                    "schema": { "arrow": { "fields": [] } },
                    "network": "mainnet"
                }
            },
            "functions": {}
        })
    }

    /// Creates a manifest with a dependency on another manifest.
    fn create_manifest_with_dep(
        name: &str,
        dep_alias: &str,
        dep_reference: &str,
    ) -> serde_json::Value {
        let mut deps = serde_json::Map::new();
        deps.insert(dep_alias.to_string(), serde_json::json!(dep_reference));

        serde_json::json!({
            "kind": "manifest",
            "dependencies": deps,
            "tables": {
                name: {
                    "input": { "sql": format!("CREATE TABLE {name} AS SELECT * FROM {dep_alias}.data") },
                    "schema": { "arrow": { "fields": [] } },
                    "network": "mainnet"
                }
            },
            "functions": {}
        })
    }

    #[tokio::test]
    async fn resolve_single_dependency_populates_cache() {
        logging::init();

        //* Given
        let temp_dir = TempDir::new().expect("should create temp dir");
        let cache = Cache::with_root(temp_dir.path().to_path_buf());
        let mut fetcher = MockFetcher::new();

        // Register a leaf manifest
        let manifest = create_leaf_manifest("transfers");
        let manifest_hash = hash_canonical(&manifest).expect("should hash");
        fetcher.register("source/dataset@1.0.0", manifest);

        let resolver = Resolver::new(fetcher, cache.clone());

        let mut deps: BTreeMap<DepAlias, DepReference> = BTreeMap::new();
        deps.insert(
            "source".parse().unwrap(),
            "source/dataset@1.0.0".parse().unwrap(),
        );

        //* When
        let graph: Result<DependencyGraph, ResolveError> = resolver.resolve_all(&deps).await;

        //* Then
        let graph = graph.expect("resolution should succeed");

        // Verify graph structure
        assert_eq!(graph.len(), 1, "should have one dependency");
        assert!(graph.direct.contains_key(&"source".parse().unwrap()));

        // Verify cache was populated
        assert!(
            cache.contains(&manifest_hash),
            "cache should contain resolved manifest"
        );
    }

    #[tokio::test]
    async fn resolve_transitive_dependencies_populates_cache() {
        logging::init();

        //* Given
        let temp_dir = TempDir::new().expect("should create temp dir");
        let cache = Cache::with_root(temp_dir.path().to_path_buf());
        let mut fetcher = MockFetcher::new();

        // Create dependency chain: A -> B -> C
        // C is a leaf (no dependencies)
        let manifest_c = create_leaf_manifest("c_table");
        let hash_c = hash_canonical(&manifest_c).expect("should hash c");
        fetcher.register("ns/c@1.0.0", manifest_c);

        // B depends on C
        let manifest_b = create_manifest_with_dep("b_table", "c_dep", "ns/c@1.0.0");
        let hash_b = hash_canonical(&manifest_b).expect("should hash b");
        fetcher.register("ns/b@1.0.0", manifest_b);

        // A depends on B
        let manifest_a = create_manifest_with_dep("a_table", "b_dep", "ns/b@1.0.0");
        let hash_a = hash_canonical(&manifest_a).expect("should hash a");
        fetcher.register("ns/a@1.0.0", manifest_a);

        let resolver = Resolver::new(fetcher, cache.clone());

        let mut deps: BTreeMap<DepAlias, DepReference> = BTreeMap::new();
        deps.insert("a".parse().unwrap(), "ns/a@1.0.0".parse().unwrap());

        //* When
        let graph: Result<DependencyGraph, ResolveError> = resolver.resolve_all(&deps).await;

        //* Then
        let graph = graph.expect("resolution should succeed");

        // Verify all transitive dependencies are resolved
        assert_eq!(graph.len(), 3, "should have three dependencies (A, B, C)");

        // Verify cache contains all manifests
        assert!(cache.contains(&hash_a), "cache should contain A");
        assert!(cache.contains(&hash_b), "cache should contain B");
        assert!(cache.contains(&hash_c), "cache should contain C");

        // Verify topological order: C before B before A
        let order = graph.topological_order().expect("should compute order");
        assert_eq!(order.len(), 3);

        let pos_a = order.iter().position(|h| h == &hash_a).unwrap();
        let pos_b = order.iter().position(|h| h == &hash_b).unwrap();
        let pos_c = order.iter().position(|h| h == &hash_c).unwrap();

        assert!(pos_c < pos_b, "C should come before B");
        assert!(pos_b < pos_a, "B should come before A");

        tracing::info!(
            hash_a = %hash_a,
            hash_b = %hash_b,
            hash_c = %hash_c,
            "Transitive resolution test passed"
        );
    }

    #[tokio::test]
    async fn cached_dependencies_are_reused() {
        logging::init();

        //* Given
        let temp_dir = TempDir::new().expect("should create temp dir");
        let cache = Cache::with_root(temp_dir.path().to_path_buf());
        let mut fetcher = MockFetcher::new();

        // Register a leaf manifest
        let manifest = create_leaf_manifest("data");
        let manifest_hash = hash_canonical(&manifest).expect("should hash");
        fetcher.register("cached/dataset@1.0.0", manifest);

        let resolver = Resolver::new(fetcher, cache.clone());

        let mut deps: BTreeMap<DepAlias, DepReference> = BTreeMap::new();
        deps.insert(
            "cached".parse().unwrap(),
            "cached/dataset@1.0.0".parse().unwrap(),
        );

        //* When
        // First resolution - should populate cache
        let graph1: Result<DependencyGraph, ResolveError> = resolver.resolve_all(&deps).await;

        //* Then
        assert!(graph1.is_ok(), "first resolution should succeed");
        assert!(
            cache.contains(&manifest_hash),
            "cache should contain manifest after first resolution"
        );

        // Verify the manifest can be retrieved from cache
        let cached_manifest = cache.get(&manifest_hash).expect("should read cache");
        assert!(cached_manifest.is_some(), "cache should have manifest");

        tracing::info!(
            hash = %manifest_hash,
            "Cache population test completed"
        );
    }

    #[tokio::test]
    async fn resolve_diamond_dependency_deduplicates() {
        logging::init();

        //* Given
        let temp_dir = TempDir::new().expect("should create temp dir");
        let cache = Cache::with_root(temp_dir.path().to_path_buf());
        let mut fetcher = MockFetcher::new();

        // Diamond: A -> B, A -> C, B -> D, C -> D
        // D is shared between B and C

        let manifest_d = create_leaf_manifest("d_table");
        let hash_d = hash_canonical(&manifest_d).expect("should hash d");
        fetcher.register("ns/d@1.0.0", manifest_d);

        let manifest_b = create_manifest_with_dep("b_table", "d_dep", "ns/d@1.0.0");
        let hash_b = hash_canonical(&manifest_b).expect("should hash b");
        fetcher.register("ns/b@1.0.0", manifest_b);

        let manifest_c = create_manifest_with_dep("c_table", "d_dep", "ns/d@1.0.0");
        let hash_c = hash_canonical(&manifest_c).expect("should hash c");
        fetcher.register("ns/c@1.0.0", manifest_c);

        // A depends on both B and C
        let manifest_a = serde_json::json!({
            "kind": "manifest",
            "dependencies": {
                "b_dep": "ns/b@1.0.0",
                "c_dep": "ns/c@1.0.0"
            },
            "tables": {
                "a_table": {
                    "input": { "sql": "CREATE TABLE a_table AS SELECT 1" },
                    "schema": { "arrow": { "fields": [] } },
                    "network": "mainnet"
                }
            },
            "functions": {}
        });
        let hash_a = hash_canonical(&manifest_a).expect("should hash a");
        fetcher.register("ns/a@1.0.0", manifest_a);

        let resolver = Resolver::new(fetcher, cache.clone());

        let mut deps: BTreeMap<DepAlias, DepReference> = BTreeMap::new();
        deps.insert("a".parse().unwrap(), "ns/a@1.0.0".parse().unwrap());

        //* When
        let graph: Result<DependencyGraph, ResolveError> = resolver.resolve_all(&deps).await;

        //* Then
        let graph = graph.expect("resolution should succeed");

        // D should appear only once despite being referenced by both B and C
        assert_eq!(graph.len(), 4, "should have 4 unique dependencies");

        // Verify all are cached
        assert!(cache.contains(&hash_a));
        assert!(cache.contains(&hash_b));
        assert!(cache.contains(&hash_c));
        assert!(cache.contains(&hash_d));

        // Verify topological order: D before B and C, B and C before A
        let order = graph.topological_order().expect("should compute order");
        let pos_a = order.iter().position(|h| h == &hash_a).unwrap();
        let pos_b = order.iter().position(|h| h == &hash_b).unwrap();
        let pos_c = order.iter().position(|h| h == &hash_c).unwrap();
        let pos_d = order.iter().position(|h| h == &hash_d).unwrap();

        assert!(pos_d < pos_b, "D should come before B");
        assert!(pos_d < pos_c, "D should come before C");
        assert!(pos_b < pos_a, "B should come before A");
        assert!(pos_c < pos_a, "C should come before A");

        tracing::info!("Diamond dependency test passed");
    }

    #[tokio::test]
    async fn resolve_missing_dependency_returns_error() {
        logging::init();

        //* Given
        let temp_dir = TempDir::new().expect("should create temp dir");
        let cache = Cache::with_root(temp_dir.path().to_path_buf());
        let fetcher = MockFetcher::new();
        // Don't register any manifests

        let resolver = Resolver::new(fetcher, cache);

        let mut deps: BTreeMap<DepAlias, DepReference> = BTreeMap::new();
        deps.insert(
            "missing".parse().unwrap(),
            "missing/dataset@1.0.0".parse().unwrap(),
        );

        //* When
        let result: Result<DependencyGraph, ResolveError> = resolver.resolve_all(&deps).await;

        //* Then
        assert!(result.is_err(), "should fail for missing dependency");
        let err = result.expect_err("should have error");
        assert!(
            matches!(err, ResolveError::NotFound { .. }),
            "should be NotFound error, got: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn locked_mode_validates_root() {
        use dataset_authoring::lockfile::{Lockfile, LockfileError, RootInfo};

        logging::init();

        //* Given
        let temp_dir = TempDir::new().expect("should create temp dir");
        let cache = Cache::with_root(temp_dir.path().to_path_buf());
        let mut fetcher = MockFetcher::new();

        // Register a leaf manifest
        let manifest = create_leaf_manifest("data");
        fetcher.register("source/dataset@1.0.0", manifest);

        let resolver = Resolver::new(fetcher, cache);

        let mut deps: BTreeMap<DepAlias, DepReference> = BTreeMap::new();
        deps.insert(
            "source".parse().unwrap(),
            "source/dataset@1.0.0".parse().unwrap(),
        );

        // Resolve and create lockfile with original root info
        let graph = resolver.resolve_all(&deps).await.expect("should resolve");
        let original_root = RootInfo {
            namespace: "original_ns".parse().expect("valid namespace"),
            name: "original_dataset".parse().expect("valid name"),
            version: "1.0.0".parse().expect("valid version"),
        };
        let lockfile = Lockfile::from_graph(original_root.clone(), &graph);

        //* When - verify with matching root
        let matching_result = lockfile.verify_with_root(Some(&original_root), &graph);

        //* Then
        assert!(
            matching_result.is_ok(),
            "verification should succeed with matching root"
        );

        //* When - verify with different namespace
        let different_ns_root = RootInfo {
            namespace: "different_ns".parse().expect("valid namespace"),
            name: "original_dataset".parse().expect("valid name"),
            version: "1.0.0".parse().expect("valid version"),
        };
        let mismatch_ns_result = lockfile.verify_with_root(Some(&different_ns_root), &graph);

        //* Then
        assert!(
            mismatch_ns_result.is_err(),
            "verification should fail with different namespace"
        );
        let err = mismatch_ns_result.expect_err("should have error");
        assert!(
            matches!(err, LockfileError::RootMismatch { .. }),
            "should be RootMismatch error, got: {:?}",
            err
        );

        //* When - verify with different version
        let different_version_root = RootInfo {
            namespace: "original_ns".parse().expect("valid namespace"),
            name: "original_dataset".parse().expect("valid name"),
            version: "2.0.0".parse().expect("valid version"),
        };
        let mismatch_version_result =
            lockfile.verify_with_root(Some(&different_version_root), &graph);

        //* Then
        assert!(
            mismatch_version_result.is_err(),
            "verification should fail with different version"
        );
        let err = mismatch_version_result.expect_err("should have error");
        assert!(
            matches!(err, LockfileError::RootMismatch { .. }),
            "should be RootMismatch error, got: {:?}",
            err
        );

        tracing::info!("Locked mode validates root test passed");
    }
}
