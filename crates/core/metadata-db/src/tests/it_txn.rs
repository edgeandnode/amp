//! Integration tests for transaction support

use pgtemp::PgTempDB;

use crate::{
    DEFAULT_POOL_SIZE, DatasetName, DatasetNamespace, ManifestHash, ManifestPath, datasets,
    manifests,
};

#[tokio::test]
async fn commit_persists_changes() {
    //* Given
    let temp_db = PgTempDB::new();
    let conn = crate::connect_pool_with_retry(&temp_db.connection_uri(), DEFAULT_POOL_SIZE)
        .await
        .expect("Failed to connect to metadata db");

    let namespace = DatasetNamespace::from_ref_unchecked("test-namespace");
    let name = DatasetName::from_ref_unchecked("test-dataset-commit");
    let manifest_hash = ManifestHash::from_ref_unchecked(
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    );
    let manifest_path = ManifestPath::from_ref_unchecked("path/to/manifest-commit.json");

    // Begin transaction
    let mut tx = conn.begin_txn().await.expect("Failed to begin transaction");

    // Make changes within transaction
    manifests::register(&mut tx, &manifest_hash, &manifest_path)
        .await
        .expect("Failed to register manifest in transaction");
    datasets::link_manifest(&mut tx, &namespace, &name, &manifest_hash)
        .await
        .expect("Failed to link manifest in transaction");

    //* When
    let commit_result = tx.commit().await;

    //* Then
    assert!(commit_result.is_ok(), "transaction commit should succeed");

    // Verify data persisted by querying outside the transaction
    let path = manifests::get_path(&conn, manifest_hash)
        .await
        .expect("Failed to query manifest path after commit");
    assert_eq!(
        path,
        Some(manifest_path),
        "Manifest should be persisted after commit"
    );
}

#[tokio::test]
async fn explicit_rollback_discards_changes() {
    //* Given
    let temp_db = PgTempDB::new();
    let conn = crate::connect_pool_with_retry(&temp_db.connection_uri(), DEFAULT_POOL_SIZE)
        .await
        .expect("Failed to connect to metadata db");

    let namespace = DatasetNamespace::from_ref_unchecked("test-namespace");
    let name = DatasetName::from_ref_unchecked("test-dataset-rollback");
    let manifest_hash = ManifestHash::from_ref_unchecked(
        "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
    );
    let manifest_path = ManifestPath::from_ref_unchecked("path/to/manifest-rollback.json");

    let mut tx = conn.begin_txn().await.expect("Failed to begin transaction");

    // Make changes within transaction
    manifests::register(&mut tx, &manifest_hash, manifest_path)
        .await
        .expect("Failed to register manifest in transaction");
    datasets::link_manifest(&mut tx, &namespace, &name, &manifest_hash)
        .await
        .expect("Failed to link manifest in transaction");

    //* When
    let rollback_result = tx.rollback().await;

    //* Then
    assert!(
        rollback_result.is_ok(),
        "transaction rollback should succeed"
    );

    // Verify data was NOT persisted
    let path = manifests::get_path(&conn, manifest_hash)
        .await
        .expect("Failed to query manifest path after rollback");
    assert_eq!(
        path, None,
        "Manifest should NOT be persisted after rollback"
    );
}

#[tokio::test]
async fn rollback_on_drop_discards_changes() {
    //* Given
    let temp_db = PgTempDB::new();
    let conn = crate::connect_pool_with_retry(&temp_db.connection_uri(), DEFAULT_POOL_SIZE)
        .await
        .expect("Failed to connect to metadata db");

    let namespace = DatasetNamespace::from_ref_unchecked("test-namespace");
    let name = DatasetName::from_ref_unchecked("test-dataset-drop");
    let manifest_hash = ManifestHash::from_ref_unchecked(
        "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
    );
    let manifest_path = ManifestPath::from_ref_unchecked("path/to/manifest-drop.json");

    let mut tx = conn.begin_txn().await.expect("Failed to begin transaction");

    // Make changes within transaction
    manifests::register(&mut tx, &manifest_hash, manifest_path)
        .await
        .expect("Failed to register manifest in transaction");
    datasets::link_manifest(&mut tx, &namespace, &name, &manifest_hash)
        .await
        .expect("Failed to link manifest in transaction");

    //* When
    drop(tx);

    //* Then
    // Verify data was NOT persisted
    let path = manifests::get_path(&conn, manifest_hash)
        .await
        .expect("Failed to query manifest path after drop");
    assert_eq!(
        path, None,
        "Manifest should NOT be persisted after explicit drop"
    );
}
