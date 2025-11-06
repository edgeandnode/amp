//! Core location operations tests

use pgtemp::PgTempDB;
use url::Url;

use crate::{
    TableId, WorkerInfo, WorkerNodeId,
    db::Connection,
    jobs::{self, JobId},
    manifests::ManifestHashOwned,
    physical_table::{self, LocationId},
    workers,
};

/// Helper function to create a test manifest hash
fn test_manifest_hash() -> ManifestHashOwned {
    ManifestHashOwned::from_ref_unchecked(
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    )
}

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

    let columns: Vec<String> = sqlx::query_scalar("SELECT column_name FROM information_schema.columns WHERE table_name = 'physical_table' ORDER BY column_name").fetch_all(&mut conn).await.expect("Failed to query schema");
    println!("Physical table columns: {:?}", columns);

    let table = TableId {
        manifest_hash: test_manifest_hash(),
        table: "test-table",
    };
    let bucket = Some("test-bucket");
    let path = "/test/path/file.parquet";
    let url =
        Url::parse("s3://test-bucket/test/path/file.parquet").expect("Failed to parse test URL");
    let active = true;

    //* When
    let location_id = physical_table::insert(
        &mut conn,
        table,
        "test-namespace",
        "test-dataset",
        bucket,
        path,
        &url,
        active,
    )
    .await
    .expect("Failed to insert location");

    //* Then
    assert!(*location_id > 0);

    // Verify the location was created correctly
    let (
        row_location_id,
        row_manifest_hash,
        row_table_name,
        row_bucket,
        row_path,
        row_url,
        row_active,
    ) = get_location_by_id(&mut conn, location_id)
        .await
        .expect("Failed to fetch inserted location");

    assert_eq!(row_location_id, location_id);
    assert_eq!(
        row_manifest_hash,
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    );
    assert_eq!(row_table_name, "test-table");
    assert_eq!(row_bucket, Some("test-bucket".to_string()));
    assert_eq!(row_path, "/test/path/file.parquet");
    assert_eq!(row_url, url.as_str());
    assert!(row_active);
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

    let table = TableId {
        manifest_hash: test_manifest_hash(),
        table: "test-table",
    };
    let url = Url::parse("s3://test-bucket/unique-file.parquet")
        .expect("Failed to parse unique file URL");

    // Insert first location
    let first_id = physical_table::insert(
        &mut conn,
        table.clone(),
        "test-namespace",
        "test-dataset",
        Some("bucket1"),
        "/path1",
        &url,
        true,
    )
    .await
    .expect("Failed to insert first location");

    //* When - Try to insert with same URL but different data
    let second_id = physical_table::insert(
        &mut conn,
        table,
        "test-namespace",
        "test-dataset",
        Some("bucket2"),
        "/path2",
        &url,
        false,
    )
    .await
    .expect("Failed to insert second location");

    //* Then - Should return the same ID due to conflict resolution
    assert_eq!(first_id, second_id);
}

#[tokio::test]
async fn url_to_location_id_finds_existing_location() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = Connection::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let table = TableId {
        manifest_hash: test_manifest_hash(),
        table: "test-table",
    };
    let url = Url::parse("s3://test-bucket/find-me.parquet").expect("Failed to parse find-me URL");

    let expected_id = physical_table::insert(
        &mut conn,
        table,
        "test-namespace",
        "test-dataset",
        None,
        "/find-me.parquet",
        &url,
        false,
    )
    .await
    .expect("Failed to insert location");

    //* When
    let found_id = physical_table::url_to_id(&mut conn, &url)
        .await
        .expect("Failed to search for location");

    //* Then
    assert_eq!(found_id, Some(expected_id));
}

#[tokio::test]
async fn url_to_location_id_returns_none_when_not_found() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = Connection::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    let url = Url::parse("s3://test-bucket/nonexistent.parquet")
        .expect("Failed to parse nonexistent URL");

    //* When
    let found_id = physical_table::url_to_id(&mut conn, &url)
        .await
        .expect("Failed to search for location");

    //* Then
    assert_eq!(found_id, None);
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

    let table = TableId {
        manifest_hash: test_manifest_hash(),
        table: "test-table",
    };
    let table2 = TableId {
        manifest_hash: test_manifest_hash(),
        table: "test-table2",
    };
    let other_table = TableId {
        manifest_hash: test_manifest_hash(),
        table: "other-table",
    };

    // Create active location for target table
    let url1 = Url::parse("s3://bucket/active1.parquet").expect("Failed to parse active1 URL");
    let active_id1 = physical_table::insert(
        &mut conn,
        table.clone(),
        "test-namespace",
        "test-dataset",
        None,
        "/active1.parquet",
        &url1,
        true,
    )
    .await
    .expect("Failed to insert active location 1");

    // Create another active location for different table (still should be returned)
    let url2 = Url::parse("s3://bucket/active2.parquet").expect("Failed to parse active2 URL");
    let active_id2 = physical_table::insert(
        &mut conn,
        table2.clone(),
        "test-namespace",
        "test-dataset",
        None,
        "/active2.parquet",
        &url2,
        true,
    )
    .await
    .expect("Failed to insert active location 2");

    // Create inactive location for target table (should be filtered out)
    let url3 = Url::parse("s3://bucket/inactive.parquet").expect("Failed to parse inactive URL");
    physical_table::insert(
        &mut conn,
        table.clone(),
        "test-namespace",
        "test-dataset",
        None,
        "/inactive.parquet",
        &url3,
        false,
    )
    .await
    .expect("Failed to insert inactive location");

    // Create active location for different table (should be filtered out)
    let url4 =
        Url::parse("s3://bucket/other-table.parquet").expect("Failed to parse other-table URL");
    physical_table::insert(
        &mut conn,
        other_table,
        "test-namespace",
        "test-dataset",
        None,
        "/other-table.parquet",
        &url4,
        true,
    )
    .await
    .expect("Failed to insert location for other table");

    //* When - Get locations for first table
    let active_location1 = physical_table::get_active_physical_table(&mut conn, table)
        .await
        .expect("Failed to get active locations for table 1");
    let active_location2 = physical_table::get_active_physical_table(&mut conn, table2)
        .await
        .expect("Failed to get active locations for table 2");

    //* Then
    assert!(active_location1.is_some());
    assert!(active_location2.is_some());

    let active_location1 = active_location1.unwrap();
    let active_location2 = active_location2.unwrap();

    // Check that we got the right locations
    assert_eq!(active_location1.url, url1);
    assert_eq!(active_location2.url, url2);

    // Check that we got the right IDs
    assert_eq!(active_location1.id, active_id1);
    assert_eq!(active_location2.id, active_id2);
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

    let table = TableId {
        manifest_hash: test_manifest_hash(),
        table: "test-table",
    };
    let table2 = TableId {
        manifest_hash: test_manifest_hash(),
        table: "test-table2",
    };
    let other_table = TableId {
        manifest_hash: test_manifest_hash(),
        table: "other-table",
    };

    // Create active location for first target table
    let url1 = Url::parse("s3://bucket/target1.parquet").expect("Failed to parse target1 URL");
    let target_id1 = physical_table::insert(
        &mut conn,
        table.clone(),
        "test-namespace",
        "test-dataset",
        None,
        "/target1.parquet",
        &url1,
        true,
    )
    .await
    .expect("Failed to insert target location 1");

    // Create active location for second target table
    let url2 = Url::parse("s3://bucket/target2.parquet").expect("Failed to parse target2 URL");
    let target_id2 = physical_table::insert(
        &mut conn,
        table2,
        "test-namespace",
        "test-dataset",
        None,
        "/target2.parquet",
        &url2,
        true,
    )
    .await
    .expect("Failed to insert target location 2");

    // Create already inactive location for target table (should remain unchanged)
    let url3 = Url::parse("s3://bucket/already-inactive.parquet")
        .expect("Failed to parse already-inactive URL");
    let inactive_id = physical_table::insert(
        &mut conn,
        table.clone(),
        "test-namespace",
        "test-dataset",
        None,
        "/already-inactive.parquet",
        &url3,
        false,
    )
    .await
    .expect("Failed to insert inactive location");

    // Create active location for different table (should remain unchanged)
    let url4 = Url::parse("s3://bucket/other.parquet").expect("Failed to parse other URL");
    let other_id = physical_table::insert(
        &mut conn,
        other_table,
        "test-namespace",
        "test-dataset",
        None,
        "/other.parquet",
        &url4,
        true,
    )
    .await
    .expect("Failed to insert other table location");

    //* When - Mark only the first table inactive
    physical_table::mark_inactive_by_table_id(&mut conn, table)
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

    assert!(!target1_active); // This was deactivated
    assert!(target2_active); // Different table, stays active 
    assert!(!inactive_still_inactive); // Was already inactive
    assert!(other_still_active); // Different dataset, stays active
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

    let table = TableId {
        manifest_hash: test_manifest_hash(),
        table: "test-table",
    };

    let url1 =
        Url::parse("s3://bucket/to-activate.parquet").expect("Failed to parse to-activate URL");
    let target_id = physical_table::insert(
        &mut conn,
        table.clone(),
        "test-namespace",
        "test-dataset",
        None,
        "/to-activate.parquet",
        &url1,
        false,
    )
    .await
    .expect("Failed to insert location to activate");

    let url2 =
        Url::parse("s3://bucket/stay-inactive.parquet").expect("Failed to parse stay-inactive URL");
    let other_id = physical_table::insert(
        &mut conn,
        table.clone(),
        "test-namespace",
        "test-dataset",
        None,
        "/stay-inactive.parquet",
        &url2,
        false,
    )
    .await
    .expect("Failed to insert other location");

    //* When
    physical_table::mark_active_by_id(&mut conn, table, &target_id)
        .await
        .expect("Failed to mark location active");

    //* Then
    let target_active = is_location_active(&mut conn, target_id)
        .await
        .expect("Failed to check target location active status");
    let other_still_inactive = is_location_active(&mut conn, other_id)
        .await
        .expect("Failed to check other location active status");

    assert!(target_active);
    assert!(!other_still_inactive);
}

#[tokio::test]
async fn get_by_job_id_returns_locations_written_by_job() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = Connection::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    // Create a worker and job
    let worker_id = WorkerNodeId::from_ref_unchecked("test-worker");
    let worker_info = WorkerInfo::default(); // {}
    workers::register(&mut conn, worker_id.clone(), worker_info)
        .await
        .expect("Failed to register worker");

    let job_desc = serde_json::json!({"operation": "dump"});
    let job_desc_str =
        serde_json::to_string(&job_desc).expect("Failed to serialize job description");
    let job_id = jobs::sql::insert_with_default_status(&mut conn, worker_id, &job_desc_str)
        .await
        .expect("Failed to register job");

    let table1 = TableId {
        manifest_hash: test_manifest_hash(),
        table: "test-table1",
    };
    let table2 = TableId {
        manifest_hash: test_manifest_hash(),
        table: "test-table2",
    };
    let table3 = TableId {
        manifest_hash: test_manifest_hash(),
        table: "test-table3",
    };

    // Create locations and assign them to the job
    let url1 =
        Url::parse("s3://bucket/job-output1.parquet").expect("Failed to parse job-output1 URL");
    let location_id1 = physical_table::insert(
        &mut conn,
        table1,
        "test-namespace",
        "test-dataset",
        None,
        "/job-output1.parquet",
        &url1,
        true,
    )
    .await
    .expect("Failed to insert location 1");

    let url2 =
        Url::parse("s3://bucket/job-output2.parquet").expect("Failed to parse job-output2 URL");
    let location_id2 = physical_table::insert(
        &mut conn,
        table2,
        "test-namespace",
        "test-dataset",
        None,
        "/job-output2.parquet",
        &url2,
        true,
    )
    .await
    .expect("Failed to insert location 2");

    // Assign locations to job
    physical_table::assign_job_writer(&mut conn, &[location_id1, location_id2], job_id)
        .await
        .expect("Failed to assign job writer");

    // Create a location not assigned to the job (should not be returned)
    let url3 =
        Url::parse("s3://bucket/other-output.parquet").expect("Failed to parse other-output URL");
    physical_table::insert(
        &mut conn,
        table3,
        "test-namespace",
        "test-dataset",
        None,
        "/other-output.parquet",
        &url3,
        true,
    )
    .await
    .expect("Failed to insert unrelated location");

    //* When
    let job_locations = physical_table::get_by_job_id(&mut conn, job_id)
        .await
        .expect("Failed to get locations by job ID");

    //* Then
    assert_eq!(job_locations.len(), 2);

    let returned_ids: Vec<LocationId> = job_locations.iter().map(|loc| loc.id).collect();
    assert!(returned_ids.contains(&location_id1));
    assert!(returned_ids.contains(&location_id2));

    // Verify location details
    for location in &job_locations {
        assert_eq!(location.dataset_name, "test-dataset");
        assert!(location.table_name == "test-table1" || location.table_name == "test-table2");
        assert!(location.active);
    }
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

    // Create a worker and job
    let worker_id = WorkerNodeId::from_ref_unchecked("test-writer-worker");
    let worker_info = WorkerInfo::default(); // {}
    workers::register(&mut conn, worker_id.clone(), worker_info)
        .await
        .expect("Failed to register worker");

    let job_desc = serde_json::json!({"operation": "write"});
    let job_desc_str =
        serde_json::to_string(&job_desc).expect("Failed to serialize job description");
    let job_id = jobs::sql::insert_with_default_status(&mut conn, worker_id, &job_desc_str)
        .await
        .expect("Failed to register job");

    let table = TableId {
        manifest_hash: test_manifest_hash(),
        table: "output-table",
    };

    // Create locations to assign
    let url1 = Url::parse("s3://bucket/assign1.parquet").expect("Failed to parse assign1 URL");
    let location_id1 = physical_table::insert(
        &mut conn,
        table.clone(),
        "test-namespace",
        "test-dataset",
        None,
        "/assign1.parquet",
        &url1,
        false,
    )
    .await
    .expect("Failed to insert location 1");

    let url2 = Url::parse("s3://bucket/assign2.parquet").expect("Failed to parse assign2 URL");
    let location_id2 = physical_table::insert(
        &mut conn,
        table.clone(),
        "test-namespace",
        "test-dataset",
        None,
        "/assign2.parquet",
        &url2,
        false,
    )
    .await
    .expect("Failed to insert location 2");

    let url3 = Url::parse("s3://bucket/assign3.parquet").expect("Failed to parse assign3 URL");
    let location_id3 = physical_table::insert(
        &mut conn,
        table.clone(),
        "test-namespace",
        "test-dataset",
        None,
        "/assign3.parquet",
        &url3,
        false,
    )
    .await
    .expect("Failed to insert location 3");

    // Create a location that should not be assigned
    let url4 =
        Url::parse("s3://bucket/not-assigned.parquet").expect("Failed to parse not-assigned URL");
    let unassigned_id = physical_table::insert(
        &mut conn,
        table,
        "test-namespace",
        "test-dataset",
        None,
        "/not-assigned.parquet",
        &url4,
        false,
    )
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

    assert_eq!(writer1, Some(job_id));
    assert_eq!(writer2, Some(job_id));
    assert_eq!(writer3, Some(job_id));
    assert_eq!(writer_unassigned, None);
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

    let table = TableId {
        manifest_hash: test_manifest_hash(),
        table: "test-table",
    };
    let url = Url::parse("s3://bucket/get-by-id.parquet").expect("Failed to parse URL");

    let inserted_id = physical_table::insert(
        &mut conn,
        table,
        "test-namespace",
        "test-dataset",
        Some("bucket"),
        "/get-by-id.parquet",
        &url,
        true,
    )
    .await
    .expect("Failed to insert location");

    //* When
    let location = physical_table::get_by_id_with_details(&mut conn, inserted_id)
        .await
        .expect("Failed to get location by id");

    println!("location: {:?}", location);

    //* Then
    assert!(location.is_some());
    let location = location.unwrap();
    assert_eq!(location.id(), inserted_id);
    assert_eq!(location.location.dataset_name, "test-dataset");
    assert_eq!(location.location.table_name, "test-table");
    assert_eq!(location.url(), &url);
    assert!(location.active());
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
    assert!(location.is_none());
}

// Helper functions for tests

/// Helper function to fetch location details by ID
async fn get_location_by_id<'c, E>(
    exe: E,
    location_id: LocationId,
) -> Result<
    (
        LocationId,
        String,
        String,
        Option<String>,
        String,
        String,
        bool,
    ),
    sqlx::Error,
>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let query = "SELECT id, manifest_hash, table_name, bucket, path, url, active FROM physical_tables WHERE id = $1";
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
