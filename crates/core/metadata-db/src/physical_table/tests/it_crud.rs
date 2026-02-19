//! Core location operations tests

use pgtemp::PgTempDB;

use crate::{
    DatasetName, DatasetNamespace,
    db::Connection,
    error::Error,
    manifests::ManifestHash,
    physical_table::{self, GetActiveByLocationIdError, TableName},
    physical_table_revision::{self, LocationId, TablePath},
};

#[tokio::test]
async fn insert_creates_location_and_returns_id() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = Connection::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let namespace = DatasetNamespace::from_ref_unchecked("test-namespace");
    let name = DatasetName::from_ref_unchecked("test-dataset");
    let hash = ManifestHash::from_ref_unchecked(
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    );
    let table_name = TableName::from_ref_unchecked("test_table");
    let path = TablePath::from_ref_unchecked(
        "test-dataset/test_table/01234567-89ab-cdef-0123-456789abcdef",
    );

    //* When
    let location_id =
        register_table_and_revision(&mut conn, &namespace, &name, &hash, &table_name, &path)
            .await
            .expect("Failed to insert location");
    physical_table::mark_active_by_id(&mut conn, location_id, namespace, name, hash, table_name)
        .await
        .expect("Failed to mark location active");

    //* Then
    assert!(
        *location_id > 0,
        "register should return valid positive location_id"
    );

    // Verify the location was created correctly
    let (row_location_id, row_manifest_hash, row_table_name, row_path, row_active) =
        get_location_by_id(&mut conn, location_id)
            .await
            .expect("Failed to fetch inserted location");

    assert_eq!(
        row_location_id, location_id,
        "database should store location with returned ID"
    );
    assert_eq!(
        row_manifest_hash, "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "database should persist manifest_hash"
    );
    assert_eq!(
        row_table_name, "test_table",
        "database should persist table_name"
    );
    assert_eq!(
        row_path, "test-dataset/test_table/01234567-89ab-cdef-0123-456789abcdef",
        "database should persist path"
    );
    assert!(row_active, "database should persist active status");
}

#[tokio::test]
async fn mark_inactive_by_table_id_deactivates_only_matching_active_locations() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = Connection::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let namespace = DatasetNamespace::from_ref_unchecked("test-namespace");
    let name = DatasetName::from_ref_unchecked("test-dataset");
    let hash = ManifestHash::from_ref_unchecked(
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    );

    let table_name = TableName::from_ref_unchecked("test_table");
    let table2_name = TableName::from_ref_unchecked("test_table2");
    let other_table_name = TableName::from_ref_unchecked("other_table");

    // Create active location for first target table
    let path1 = TablePath::from_ref_unchecked("test-dataset/test_table/target1-revision");
    let target_id1 =
        register_table_and_revision(&mut conn, &namespace, &name, &hash, &table_name, &path1)
            .await
            .expect("Failed to insert target location 1");
    physical_table::mark_active_by_id(&mut conn, target_id1, &namespace, &name, &hash, &table_name)
        .await
        .expect("Failed to mark location active");

    // Create active location for second target table
    let path2 = TablePath::from_ref_unchecked("test-dataset/test_table2/target2-revision");
    let target_id2 =
        register_table_and_revision(&mut conn, &namespace, &name, &hash, &table2_name, &path2)
            .await
            .expect("Failed to insert target location 2");
    physical_table::mark_active_by_id(
        &mut conn,
        target_id2,
        &namespace,
        &name,
        &hash,
        &table2_name,
    )
    .await
    .expect("Failed to mark location active");

    // Create already inactive location for target table (should remain unchanged)
    let path3 = TablePath::from_ref_unchecked("test-dataset/test_table/already-inactive-revision");
    let inactive_id =
        register_table_and_revision(&mut conn, &namespace, &name, &hash, &table_name, &path3)
            .await
            .expect("Failed to insert inactive location");

    // Create active location for different table (should remain unchanged)
    let path4 = TablePath::from_ref_unchecked("test-dataset/other_table/other-revision");
    let other_id = register_table_and_revision(
        &mut conn,
        &namespace,
        &name,
        &hash,
        &other_table_name,
        &path4,
    )
    .await
    .expect("Failed to insert other table location");
    physical_table::mark_active_by_id(
        &mut conn,
        other_id,
        &namespace,
        &name,
        &hash,
        &other_table_name,
    )
    .await
    .expect("Failed to mark other location active");

    //* When - Mark only the first table inactive
    physical_table::mark_inactive_by_table_name(&mut conn, &namespace, &name, &hash, &table_name)
        .await
        .expect("Failed to mark locations inactive");

    //* Then
    // Check that only first target table location is now inactive
    let target1_active = is_location_active(&mut conn, target_id1)
        .await
        .expect("Failed to check target1 active status");
    let target2_active = is_location_active(&mut conn, target_id2)
        .await
        .expect("Failed to check target2 active status"); // Different table, should stay active
    let inactive_still_inactive = is_location_active(&mut conn, inactive_id)
        .await
        .expect("Failed to check inactive location status");
    let other_still_active = is_location_active(&mut conn, other_id)
        .await
        .expect("Failed to check other location status");

    assert!(
        !target1_active,
        "mark_inactive_by_table_id should deactivate active location matching table_name"
    );
    assert!(
        target2_active,
        "mark_inactive_by_table_id should not affect different table_name"
    );
    assert!(
        !inactive_still_inactive,
        "mark_inactive_by_table_id should not affect already inactive locations"
    );
    assert!(
        other_still_active,
        "mark_inactive_by_table_id should not affect different table_name"
    );
}

#[tokio::test]
async fn mark_active_by_id_activates_specific_location() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = Connection::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");

    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let namespace = DatasetNamespace::from_ref_unchecked("test-namespace");
    let name = DatasetName::from_ref_unchecked("test-dataset");
    let hash = ManifestHash::from_ref_unchecked(
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    );

    let table_name = TableName::from_ref_unchecked("test_table");

    let path1 = TablePath::from_ref_unchecked("test-dataset/test_table/to-activate-revision");
    let target_id =
        register_table_and_revision(&mut conn, &namespace, &name, &hash, &table_name, &path1)
            .await
            .expect("Failed to insert location to activate");

    let path2 = TablePath::from_ref_unchecked("test-dataset/test_table/stay-inactive-revision");
    let other_id =
        register_table_and_revision(&mut conn, &namespace, &name, &hash, &table_name, &path2)
            .await
            .expect("Failed to insert other location");

    //* When
    physical_table::mark_active_by_id(&mut conn, target_id, &namespace, &name, &hash, &table_name)
        .await
        .expect("Failed to mark location active");

    //* Then
    let target_active = is_location_active(&mut conn, target_id)
        .await
        .expect("Failed to check target location active status");

    let other_still_inactive = is_location_active(&mut conn, other_id)
        .await
        .expect("Failed to check other location active status");

    assert!(
        target_active,
        "mark_active_by_id should activate location matching ID"
    );
    assert!(
        !other_still_inactive,
        "mark_active_by_id should only affect specified location_id"
    );
}

#[tokio::test]
async fn get_active_by_location_id_returns_some_when_active() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = Connection::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let namespace = DatasetNamespace::from_ref_unchecked("test-namespace");
    let name = DatasetName::from_ref_unchecked("test-dataset");
    let hash = ManifestHash::from_ref_unchecked(
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    );

    let table_name = TableName::from_ref_unchecked("test_table");
    let path = TablePath::from_ref_unchecked("test-dataset/test_table/active-for-get-active");

    let inserted_id =
        register_table_and_revision(&mut conn, &namespace, &name, &hash, &table_name, &path)
            .await
            .expect("Failed to insert location");
    physical_table::mark_active_by_id(
        &mut conn,
        inserted_id,
        &namespace,
        &name,
        &hash,
        &table_name,
    )
    .await
    .expect("Failed to mark location active");

    //* When
    let table = physical_table::get_active_by_location_id(&mut conn, inserted_id)
        .await
        .expect("Failed to get active table by location id");

    //* Then
    assert_eq!(
        table.manifest_hash.as_str(),
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "get_active_by_location_id should return PhysicalTable with correct manifest_hash"
    );
    assert_eq!(
        table.table_name.as_str(),
        "test_table",
        "get_active_by_location_id should return PhysicalTable with correct table_name"
    );
    assert_eq!(
        table.active_revision_id,
        Some(inserted_id),
        "get_active_by_location_id should return PhysicalTable with correct active_revision_id"
    );
}

#[tokio::test]
async fn get_active_by_location_id_returns_err_inactive_when_inactive() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = Connection::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let namespace = DatasetNamespace::from_ref_unchecked("test-namespace");
    let name = DatasetName::from_ref_unchecked("test-dataset");
    let hash = ManifestHash::from_ref_unchecked(
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    );

    let table_name = TableName::from_ref_unchecked("test_table");
    let path = TablePath::from_ref_unchecked("test-dataset/test_table/inactive-for-get-active");

    let inserted_id =
        register_table_and_revision(&mut conn, &namespace, &name, &hash, &table_name, &path)
            .await
            .expect("Failed to insert location");
    // Do NOT call mark_active_by_id - revision stays inactive

    //* When
    let result = physical_table::get_active_by_location_id(&mut conn, inserted_id).await;

    //* Then
    assert!(
        matches!(
            result,
            Err(Error::GetActiveByLocationId(
                GetActiveByLocationIdError::Inactive
            ))
        ),
        "get_active_by_location_id should return Inactive error when revision is not active, got: {result:?}"
    );
}

#[tokio::test]
async fn get_active_by_location_id_returns_err_not_found_for_nonexistent() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = Connection::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let nonexistent_id = LocationId::try_from(999999_i64).expect("Failed to create LocationId");

    //* When
    let result = physical_table::get_active_by_location_id(&mut conn, nonexistent_id).await;

    //* Then
    assert!(
        matches!(
            result,
            Err(Error::GetActiveByLocationId(
                GetActiveByLocationIdError::NotFound
            ))
        ),
        "get_active_by_location_id should return NotFound error for nonexistent location_id, got: {result:?}"
    );
}

// Helper functions for tests

/// Helper function to fetch location details by ID (Only checks active revision)
async fn get_location_by_id<'c, E>(
    exe: E,
    location_id: LocationId,
) -> Result<(LocationId, String, String, String, bool), sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let query = indoc::indoc! {"
        SELECT ptr.id, pt.manifest_hash, pt.table_name, ptr.path,
               (pt.active_revision_id = ptr.id) AS active
        FROM physical_table_revisions ptr
        JOIN physical_tables pt ON pt.active_revision_id = ptr.id
        WHERE ptr.id = $1
    "};
    sqlx::query_as(query).bind(location_id).fetch_one(exe).await
}

/// Helper function to check if location is active by ID
async fn is_location_active<'c, E>(exe: E, location_id: LocationId) -> Result<bool, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let query = indoc::indoc! {"
        SELECT COALESCE(pt.active_revision_id = ptr.id, false) AS activ
        FROM physical_table_revisions ptr
        LEFT JOIN physical_tables pt ON pt.active_revision_id = ptr.id
        WHERE ptr.id = $1
    "};
    sqlx::query_scalar(query)
        .bind(location_id)
        .fetch_one(exe)
        .await
}

// Helper function to register a table and revision
async fn register_table_and_revision(
    conn: &mut Connection,
    namespace: &DatasetNamespace<'_>,
    name: &DatasetName<'_>,
    hash: &ManifestHash<'_>,
    table_name: &TableName<'_>,
    path: &TablePath<'_>,
) -> Result<LocationId, Error> {
    physical_table::register(&mut *conn, namespace, name, hash, table_name).await?;
    let metadata_json = serde_json::json!({
        "dataset_namespace": namespace,
        "dataset_name": name,
        "manifest_hash": hash,
        "table_name": table_name,
    });
    let raw =
        serde_json::value::to_raw_value(&metadata_json).expect("test metadata should serialize");
    let metadata = physical_table_revision::RevisionMetadata::from_owned_unchecked(raw);
    let revision_id = physical_table_revision::register(conn, path, metadata).await?;
    Ok(revision_id)
}
