//! Core location operations tests

use pgtemp::PgTempDB;

use crate::{
    DatasetName, DatasetNamespace, WorkerInfo, WorkerNodeId,
    db::Connection,
    jobs::{self, JobId},
    manifests::ManifestHash,
    physical_table::{self, LocationId, TableName, TablePath},
    workers,
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
    let active = true;

    //* When
    let location_id =
        physical_table::register(&mut conn, namespace, name, hash, table_name, path, active)
            .await
            .expect("Failed to insert location");

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
async fn insert_on_conflict_returns_existing_id() {
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
    let path = TablePath::from_ref_unchecked("test-dataset/test_table/unique-revision-id");

    // Insert first location
    let first_id = physical_table::register(
        &mut conn,
        &namespace,
        &name,
        &hash,
        &table_name,
        &path,
        true,
    )
    .await
    .expect("Failed to insert first location");

    //* When - Try to insert with same path but different data
    let second_id =
        physical_table::register(&mut conn, namespace, name, hash, table_name, path, false)
            .await
            .expect("Failed to insert second location");

    //* Then - Should return the same ID due to conflict resolution
    assert_eq!(
        first_id, second_id,
        "register with duplicate path should return existing location_id (conflict resolution)"
    );
}

#[tokio::test]
async fn path_to_location_id_finds_existing_location() {
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
    let path = TablePath::from_ref_unchecked("test-dataset/test_table/find-me-revision");

    let expected_id =
        physical_table::register(&mut conn, namespace, name, hash, table_name, &path, false)
            .await
            .expect("Failed to insert location");

    //* When
    let found_id = physical_table::path_to_id(&mut conn, path)
        .await
        .expect("Failed to search for location");

    //* Then
    assert_eq!(
        found_id,
        Some(expected_id),
        "path_to_id should find location_id by path lookup"
    );
}

#[tokio::test]
async fn path_to_location_id_returns_none_when_not_found() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = Connection::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let path = TablePath::from_ref_unchecked("test-dataset/test_table/nonexistent-revision");

    //* When
    let found_id = physical_table::path_to_id(&mut conn, path)
        .await
        .expect("Failed to search for location");

    //* Then
    assert_eq!(
        found_id, None,
        "path_to_id should return None when path not found"
    );
}

#[tokio::test]
async fn get_active_by_table_id_filters_by_table_and_active_status() {
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

    // Create active location for target table
    let path1 = TablePath::from_ref_unchecked("test-dataset/test_table/active1-revision");
    let active_id1 = physical_table::register(
        &mut conn,
        &namespace,
        &name,
        &hash,
        &table_name,
        &path1,
        true,
    )
    .await
    .expect("Failed to insert active location 1");

    // Create another active location for different table (still should be returned)
    let path2 = TablePath::from_ref_unchecked("test-dataset/test_table2/active2-revision");
    let active_id2 = physical_table::register(
        &mut conn,
        &namespace,
        &name,
        &hash,
        &table2_name,
        &path2,
        true,
    )
    .await
    .expect("Failed to insert active location 2");

    // Create inactive location for target table (should be filtered out)
    let path3 = TablePath::from_ref_unchecked("test-dataset/test_table/inactive-revision");
    physical_table::register(
        &mut conn,
        &namespace,
        &name,
        &hash,
        &table_name,
        path3,
        false,
    )
    .await
    .expect("Failed to insert inactive location");

    // Create active location for different table (should be filtered out)
    let path4 = TablePath::from_ref_unchecked("test-dataset/other_table/other-revision");
    physical_table::register(
        &mut conn,
        &namespace,
        &name,
        &hash,
        &other_table_name,
        path4,
        true,
    )
    .await
    .expect("Failed to insert location for other table");

    //* When - Get locations for first table
    let active_location1 = physical_table::get_active_physical_table(&mut conn, &hash, &table_name)
        .await
        .expect("Failed to get active locations for table 1");
    let active_location2 =
        physical_table::get_active_physical_table(&mut conn, &hash, &table2_name)
            .await
            .expect("Failed to get active locations for table 2");

    //* Then
    assert!(
        active_location1.is_some(),
        "get_active_physical_table should return active location for table 1"
    );
    assert!(
        active_location2.is_some(),
        "get_active_physical_table should return active location for table 2"
    );

    let active_location1 = active_location1.unwrap();
    let active_location2 = active_location2.unwrap();

    // Check that we got the right locations
    assert_eq!(
        active_location1.path, path1,
        "get_active_physical_table should return location with matching path for table 1"
    );
    assert_eq!(
        active_location2.path, path2,
        "get_active_physical_table should return location with matching path for table 2"
    );

    // Check that we got the right IDs
    assert_eq!(
        active_location1.id, active_id1,
        "get_active_physical_table should filter by table_name (table 1)"
    );
    assert_eq!(
        active_location2.id, active_id2,
        "get_active_physical_table should filter by table_name (table 2)"
    );
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
    let target_id1 = physical_table::register(
        &mut conn,
        &namespace,
        &name,
        &hash,
        &table_name,
        path1,
        true,
    )
    .await
    .expect("Failed to insert target location 1");

    // Create active location for second target table
    let path2 = TablePath::from_ref_unchecked("test-dataset/test_table2/target2-revision");
    let target_id2 = physical_table::register(
        &mut conn,
        &namespace,
        &name,
        &hash,
        &table2_name,
        path2,
        true,
    )
    .await
    .expect("Failed to insert target location 2");

    // Create already inactive location for target table (should remain unchanged)
    let path3 = TablePath::from_ref_unchecked("test-dataset/test_table/already-inactive-revision");
    let inactive_id = physical_table::register(
        &mut conn,
        &namespace,
        &name,
        &hash,
        &table_name,
        path3,
        false,
    )
    .await
    .expect("Failed to insert inactive location");

    // Create active location for different table (should remain unchanged)
    let path4 = TablePath::from_ref_unchecked("test-dataset/other_table/other-revision");
    let other_id = physical_table::register(
        &mut conn,
        &namespace,
        &name,
        &hash,
        &other_table_name,
        path4,
        true,
    )
    .await
    .expect("Failed to insert other table location");

    //* When - Mark only the first table inactive
    physical_table::mark_inactive_by_table_id(&mut conn, &hash, &table_name)
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
    let target_id = physical_table::register(
        &mut conn,
        &namespace,
        &name,
        &hash,
        &table_name,
        path1,
        false,
    )
    .await
    .expect("Failed to insert location to activate");

    let path2 = TablePath::from_ref_unchecked("test-dataset/test_table/stay-inactive-revision");
    let other_id = physical_table::register(
        &mut conn,
        &namespace,
        &name,
        &hash,
        &table_name,
        path2,
        false,
    )
    .await
    .expect("Failed to insert other location");

    //* When
    physical_table::mark_active_by_id(&mut conn, target_id, &hash, &table_name)
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
async fn assign_job_writer_assigns_job_to_multiple_locations() {
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

    // Create a worker and job
    let worker_id = WorkerNodeId::from_ref_unchecked("test-writer-worker");
    let worker_info = WorkerInfo::default(); // {}
    workers::register(&mut conn, &worker_id, worker_info)
        .await
        .expect("Failed to register worker");

    let job_desc = serde_json::json!({"operation": "write"});
    let job_desc_str =
        serde_json::to_string(&job_desc).expect("Failed to serialize job description");
    let job_id = jobs::sql::insert_with_default_status(&mut conn, worker_id, &job_desc_str)
        .await
        .expect("Failed to register job");

    let table_name = TableName::from_ref_unchecked("output_table");

    // Create locations to assign
    let path1 = TablePath::from_ref_unchecked("test-dataset/output_table/assign1-revision");
    let location_id1 = physical_table::register(
        &mut conn,
        &namespace,
        &name,
        &hash,
        &table_name,
        path1,
        false,
    )
    .await
    .expect("Failed to insert location 1");

    let path2 = TablePath::from_ref_unchecked("test-dataset/output_table/assign2-revision");
    let location_id2 = physical_table::register(
        &mut conn,
        &namespace,
        &name,
        &hash,
        &table_name,
        path2,
        false,
    )
    .await
    .expect("Failed to insert location 2");

    let path3 = TablePath::from_ref_unchecked("test-dataset/output_table/assign3-revision");
    let location_id3 = physical_table::register(
        &mut conn,
        &namespace,
        &name,
        &hash,
        &table_name,
        path3,
        false,
    )
    .await
    .expect("Failed to insert location 3");

    // Create a location that should not be assigned
    let path4 = TablePath::from_ref_unchecked("test-dataset/output_table/not-assigned-revision");
    let unassigned_id =
        physical_table::register(&mut conn, namespace, name, hash, &table_name, path4, false)
            .await
            .expect("Failed to insert unassigned location");

    //* When
    physical_table::assign_job_writer(
        &mut conn,
        &[location_id1, location_id2, location_id3],
        job_id,
    )
    .await
    .expect("Failed to assign job writer");

    //* Then
    let writer1 = get_writer_by_location_id(&mut conn, location_id1)
        .await
        .expect("Failed to get writer for location_id1");
    let writer2 = get_writer_by_location_id(&mut conn, location_id2)
        .await
        .expect("Failed to get writer for location_id2");
    let writer3 = get_writer_by_location_id(&mut conn, location_id3)
        .await
        .expect("Failed to get writer for location_id3");
    let writer_unassigned = get_writer_by_location_id(&mut conn, unassigned_id)
        .await
        .expect("Failed to get writer for unassigned location");

    assert_eq!(
        writer1,
        Some(job_id),
        "assign_job_writer should set writer for location 1"
    );
    assert_eq!(
        writer2,
        Some(job_id),
        "assign_job_writer should set writer for location 2"
    );
    assert_eq!(
        writer3,
        Some(job_id),
        "assign_job_writer should set writer for location 3"
    );
    assert_eq!(
        writer_unassigned, None,
        "assign_job_writer should only affect specified locations"
    );
}

#[tokio::test]
async fn get_by_id_returns_existing_location() {
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
    let path = TablePath::from_ref_unchecked("test-dataset/test_table/get-by-id-revision");

    let inserted_id =
        physical_table::register(&mut conn, namespace, name, hash, &table_name, &path, true)
            .await
            .expect("Failed to insert location");

    //* When
    let location = physical_table::get_by_id_with_details(&mut conn, inserted_id)
        .await
        .expect("Failed to get location by id");

    //* Then
    assert!(
        location.is_some(),
        "get_by_id_with_details should return existing location"
    );
    let location = location.unwrap();
    assert_eq!(
        location.id(),
        inserted_id,
        "get_by_id_with_details should return location matching ID"
    );
    assert_eq!(
        location.table.dataset_name, "test-dataset",
        "get_by_id_with_details should return location with persisted dataset_name"
    );
    assert_eq!(
        location.table.table_name, "test_table",
        "get_by_id_with_details should return location with persisted table_name"
    );
    assert_eq!(
        location.path(),
        &path,
        "get_by_id_with_details should return location with persisted path"
    );
    assert!(
        location.active(),
        "get_by_id_with_details should return location with persisted active status"
    );
}

#[tokio::test]
async fn get_by_id_returns_none_for_nonexistent_location() {
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
    let location = physical_table::get_by_id_with_details(&mut conn, nonexistent_id)
        .await
        .expect("Failed to get location by id");

    //* Then
    assert!(
        location.is_none(),
        "get_by_id_with_details should return None for nonexistent location_id"
    );
}

// Helper functions for tests

/// Helper function to fetch location details by ID
async fn get_location_by_id<'c, E>(
    exe: E,
    location_id: LocationId,
) -> Result<(LocationId, String, String, String, bool), sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let query =
        "SELECT id, manifest_hash, table_name, path, active FROM physical_tables WHERE id = $1";
    sqlx::query_as(query).bind(location_id).fetch_one(exe).await
}

/// Helper function to check if location is active by ID
async fn is_location_active<'c, E>(exe: E, location_id: LocationId) -> Result<bool, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let query = "SELECT active FROM physical_tables WHERE id = $1";
    sqlx::query_scalar(query)
        .bind(location_id)
        .fetch_one(exe)
        .await
}

/// Helper function to get writer job ID by location ID
async fn get_writer_by_location_id<'c, E>(
    exe: E,
    location_id: LocationId,
) -> Result<Option<JobId>, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let query = "SELECT writer FROM physical_tables WHERE id = $1";
    sqlx::query_scalar(query)
        .bind(location_id)
        .fetch_one(exe)
        .await
}
