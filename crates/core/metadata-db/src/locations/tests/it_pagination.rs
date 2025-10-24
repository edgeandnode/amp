//! Pagination tests for location listing

use pgtemp::PgTempDB;
use url::Url;

use crate::{TableId, db::Connection, locations};

#[tokio::test]
async fn list_locations_first_page_when_empty() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = Connection::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    //* When
    let locations = locations::list_first_page(&mut *conn, 10)
        .await
        .expect("Failed to list locations");

    //* Then
    assert!(locations.is_empty());
}

#[tokio::test]
async fn list_locations_first_page_respects_limit() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = Connection::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    // Create 5 locations with unique table names to avoid unique constraint violation
    for i in 0..5 {
        let table = TableId {
            dataset: "test-dataset",
            dataset_version: Some("v1.0"),
            table: &format!("test-table-{}", i),
        };
        let url =
            Url::parse(&format!("s3://bucket/file{}.parquet", i)).expect("Failed to parse URL");
        locations::insert(
            &mut *conn,
            table,
            None,
            &format!("/file{}.parquet", i),
            &url,
            true,
        )
        .await
        .expect("Failed to insert location");

        // Small delay to ensure different timestamps
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }

    //* When
    let locations = locations::list_first_page(&mut *conn, 3)
        .await
        .expect("Failed to list locations");

    //* Then
    assert_eq!(locations.len(), 3);
    assert!(locations[0].id > locations[1].id);
    assert!(locations[1].id > locations[2].id);
    for location in &locations {
        assert_eq!(location.dataset, "test-dataset");
        assert_eq!(location.dataset_version, "v1.0");
        assert!(location.table.starts_with("test-table-"));
        assert!(location.active);
    }
}

#[tokio::test]
async fn list_locations_next_page_uses_cursor() {
    //* Given
    let temp_db = PgTempDB::new();
    let mut conn = Connection::connect_with_retry(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to metadata db");
    conn.run_migrations()
        .await
        .expect("Failed to run migrations");

    // Create 10 locations with unique table names to avoid unique constraint violation
    let mut all_location_ids = Vec::new();
    for i in 0..10 {
        let table = TableId {
            dataset: "test-dataset",
            dataset_version: Some("v1.0"),
            table: &format!("test-table-page-{}", i),
        };
        let url =
            Url::parse(&format!("s3://bucket/page{}.parquet", i)).expect("Failed to parse URL");
        let location_id = locations::insert(
            &mut *conn,
            table,
            None,
            &format!("/page{}.parquet", i),
            &url,
            true,
        )
        .await
        .expect("Failed to insert location");
        all_location_ids.push(location_id);

        // Small delay to ensure different timestamps
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }

    // Get the first page to establish cursor
    let first_page = locations::list_first_page(&mut *conn, 3)
        .await
        .expect("Failed to list first page");
    let cursor = first_page
        .last()
        .expect("First page should not be empty")
        .id;

    //* When
    let second_page = locations::list_next_page(&mut *conn, 3, cursor)
        .await
        .expect("Failed to list second page");

    //* Then
    assert_eq!(second_page.len(), 3);
    // Verify no overlap with first page
    let first_page_ids: Vec<_> = first_page.iter().map(|l| l.id).collect();
    for location in &second_page {
        assert!(!first_page_ids.contains(&location.id));
    }
    // Verify ordering
    assert!(second_page[0].id > second_page[1].id);
    assert!(second_page[1].id > second_page[2].id);
    // Verify cursor worked correctly
    assert!(cursor > second_page[0].id);
}
