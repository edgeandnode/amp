use pgtemp::PgTempDB;

use crate::{
    datasets::{DatasetName, DatasetNamespace},
    db::Connection,
    error::Error,
    jobs::{self, JobId},
    manifests::ManifestHash,
    physical_table::{self, TableName},
    physical_table_revision::{self, LocationId, TablePath},
    workers::{self, WorkerInfo, WorkerNodeId},
};

#[tokio::test]
async fn get_by_location_id_with_details_returns_existing_location() {
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
    let location = physical_table_revision::get_by_location_id_with_details(&mut conn, inserted_id)
        .await
        .expect("Failed to get location by id");

    //* Then
    assert!(
        location.is_some(),
        "get_by_location_id_with_details should return existing location"
    );
    let location = location.expect("location should exist for active revision");
    assert_eq!(
        location.id(),
        inserted_id,
        "get_by_location_id_with_details should return location matching ID"
    );
    let meta: serde_json::Value = serde_json::from_str(location.revision.metadata.as_str())
        .expect("metadata should be valid JSON");
    assert_eq!(
        meta["dataset_name"], "test-dataset",
        "get_by_location_id_with_details should return location with persisted dataset_name"
    );
    assert_eq!(
        meta["table_name"], "test_table",
        "get_by_location_id_with_details should return location with persisted table_name"
    );
    assert_eq!(
        meta["dataset_namespace"], "test-namespace",
        "get_by_location_id_with_details should return location with persisted dataset_namespace"
    );
    assert_eq!(
        meta["manifest_hash"], "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "get_by_location_id_with_details should return location with persisted manifest_hash"
    );
    assert_eq!(
        &location.revision.path, &path,
        "get_by_location_id_with_details should return location with persisted path"
    );
    assert!(
        location.active(),
        "get_by_location_id_with_details should return location with persisted active status"
    );
}

#[tokio::test]
async fn get_by_location_id_with_details_returns_none_for_nonexistent_location() {
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
    let location =
        physical_table_revision::get_by_location_id_with_details(&mut conn, nonexistent_id)
            .await
            .expect("Failed to get location by id");

    //* Then
    assert!(
        location.is_none(),
        "get_by_location_id_with_details should return None for nonexistent location_id"
    );
}

#[tokio::test]
async fn get_by_location_id_with_details_returns_inactive_revision() {
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
    let path =
        TablePath::from_ref_unchecked("test-dataset/test_table/inactive-revision-for-get-by-id");

    let inserted_id =
        register_table_and_revision(&mut conn, &namespace, &name, &hash, &table_name, &path)
            .await
            .expect("Failed to insert location");
    // Do NOT call mark_active_by_id - revision stays inactive

    //* When
    let location = physical_table_revision::get_by_location_id_with_details(&mut conn, inserted_id)
        .await
        .expect("Failed to get location by id");

    //* Then
    assert!(
        location.is_some(),
        "get_by_location_id_with_details should return inactive revision by ID"
    );
    let location = location.expect("location should exist for inactive revision");
    assert_eq!(
        location.id(),
        inserted_id,
        "get_by_location_id_with_details should return location matching ID"
    );
    assert!(
        !location.active(),
        "get_by_location_id_with_details should return inactive revision with active=false"
    );
    assert_eq!(
        &location.revision.path, &path,
        "get_by_location_id_with_details should return location with persisted path"
    );
}

#[tokio::test]
async fn get_by_location_id_with_details_returns_revision_with_writer_when_assigned() {
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

    let worker_id = WorkerNodeId::from_ref_unchecked("test-writer-worker");
    let worker_info = WorkerInfo::default();
    workers::register(&mut conn, &worker_id, worker_info)
        .await
        .expect("Failed to register worker");

    let job_desc = crate::jobs::JobDescriptorRaw::from_owned_unchecked(
        serde_json::value::to_raw_value(&serde_json::json!({"operation": "write"}))
            .expect("Failed to serialize job description"),
    );
    let job_id = jobs::sql::insert_with_default_status(&mut conn, worker_id, &job_desc)
        .await
        .expect("Failed to register job");

    let table_name = TableName::from_ref_unchecked("writer_table");
    let path = TablePath::from_ref_unchecked("test-dataset/writer_table/writer-assigned-revision");

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

    physical_table_revision::assign_job_writer(&mut conn, &[inserted_id], job_id)
        .await
        .expect("Failed to assign job writer");

    //* When
    let location = physical_table_revision::get_by_location_id_with_details(&mut conn, inserted_id)
        .await
        .expect("Failed to get location by id");

    //* Then
    let location = location.expect("Location should exist");
    assert!(
        location.writer.is_some(),
        "get_by_location_id_with_details should return writer when assigned"
    );
    let writer = location
        .writer
        .expect("writer should be present after assignment");
    assert_eq!(
        writer.id, job_id,
        "get_by_location_id_with_details should return correct writer job_id"
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
    let active_id1 =
        register_table_and_revision(&mut conn, &namespace, &name, &hash, &table_name, &path1)
            .await
            .expect("Failed to insert active location 1");

    physical_table::mark_active_by_id(&mut conn, active_id1, &namespace, &name, &hash, &table_name)
        .await
        .expect("Failed to mark location active");

    // Create another active location for different table (still should be returned)
    let path2 = TablePath::from_ref_unchecked("test-dataset/test_table2/active2-revision");
    let active_id2 =
        register_table_and_revision(&mut conn, &namespace, &name, &hash, &table2_name, &path2)
            .await
            .expect("Failed to insert active location 2");

    physical_table::mark_active_by_id(
        &mut conn,
        active_id2,
        &namespace,
        &name,
        &hash,
        &table2_name,
    )
    .await
    .expect("Failed to mark location active");

    // Create inactive location for target table (should be filtered out)
    let path3 = TablePath::from_ref_unchecked("test-dataset/test_table/inactive-revision");
    register_table_and_revision(&mut conn, &namespace, &name, &hash, &table_name, &path3)
        .await
        .expect("Failed to insert inactive location");

    // Create active location for different table (should be filtered out)
    let path4 = TablePath::from_ref_unchecked("test-dataset/other_table/other-revision");
    let active_id3 = register_table_and_revision(
        &mut conn,
        &namespace,
        &name,
        &hash,
        &other_table_name,
        &path4,
    )
    .await
    .expect("Failed to insert location for other table");
    physical_table::mark_active_by_id(
        &mut conn,
        active_id3,
        &namespace,
        &name,
        &hash,
        &other_table_name,
    )
    .await
    .expect("Failed to mark location active");

    //* When - Get locations for first table
    let active_location1 = physical_table_revision::get_active(&mut conn, &hash, &table_name)
        .await
        .expect("Failed to get active locations for table 1");
    let active_location2 = physical_table_revision::get_active(&mut conn, &hash, &table2_name)
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

    let active_location1 =
        active_location1.expect("active_location1 should be Some after marking active");
    let active_location2 =
        active_location2.expect("active_location2 should be Some after marking active");

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
    let first_id =
        register_table_and_revision(&mut conn, &namespace, &name, &hash, &table_name, &path)
            .await
            .expect("Failed to insert first location");

    //* When - Try to insert with same path but different data
    let second_id =
        register_table_and_revision(&mut conn, &namespace, &name, &hash, &table_name, &path)
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
        register_table_and_revision(&mut conn, &namespace, &name, &hash, &table_name, &path)
            .await
            .expect("Failed to insert location");

    //* When
    let found_id = physical_table_revision::path_to_id(&mut conn, path)
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
    let found_id = physical_table_revision::path_to_id(&mut conn, path)
        .await
        .expect("Failed to search for location");

    //* Then
    assert_eq!(
        found_id, None,
        "path_to_id should return None when path not found"
    );
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

    let job_desc = crate::jobs::JobDescriptorRaw::from_owned_unchecked(
        serde_json::value::to_raw_value(&serde_json::json!({"operation": "write"}))
            .expect("Failed to serialize job description"),
    );
    let job_id = jobs::sql::insert_with_default_status(&mut conn, worker_id, &job_desc)
        .await
        .expect("Failed to register job");

    let table_name = TableName::from_ref_unchecked("output_table");

    // Create locations to assign
    let path1 = TablePath::from_ref_unchecked("test-dataset/output_table/assign1-revision");
    let location_id1 =
        register_table_and_revision(&mut conn, &namespace, &name, &hash, &table_name, &path1)
            .await
            .expect("Failed to insert location 1");

    let path2 = TablePath::from_ref_unchecked("test-dataset/output_table/assign2-revision");
    let location_id2 =
        register_table_and_revision(&mut conn, &namespace, &name, &hash, &table_name, &path2)
            .await
            .expect("Failed to insert location 2");

    let path3 = TablePath::from_ref_unchecked("test-dataset/output_table/assign3-revision");
    let location_id3 =
        register_table_and_revision(&mut conn, &namespace, &name, &hash, &table_name, &path3)
            .await
            .expect("Failed to insert location 3");

    // Create a location that should not be assigned
    let path4 = TablePath::from_ref_unchecked("test-dataset/output_table/not-assigned-revision");
    let unassigned_id =
        register_table_and_revision(&mut conn, &namespace, &name, &hash, &table_name, &path4)
            .await
            .expect("Failed to insert unassigned location");

    //* When
    physical_table_revision::assign_job_writer(
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

/// Helper function to get writer job ID by location ID
async fn get_writer_by_location_id<'c, E>(
    exe: E,
    location_id: LocationId,
) -> Result<Option<JobId>, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let query = "SELECT writer FROM physical_table_revisions WHERE id = $1";
    sqlx::query_scalar(query)
        .bind(location_id)
        .fetch_one(exe)
        .await
}
