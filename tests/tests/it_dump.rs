use monitoring::logging;
use nozzle::dump_cmd::dump;
use tests::testlib::{ctx::TestCtxBuilder, fixtures::SnapshotContext, helpers as test_helpers};

#[tokio::test]
async fn evm_rpc_single_dump() {
    logging::init();

    // * Given
    let dataset_name = "eth_rpc";
    let test_env = TestCtxBuilder::new("evm_rpc_single_dump")
        .with_dataset_manifest(dataset_name)
        .with_provider_config("rpc_eth_mainnet")
        .with_dataset_snapshot(dataset_name)
        .build()
        .await
        .expect("Failed to build test environment");

    let dataset = test_env
        .daemon_server()
        .dataset_store()
        .get_dataset(dataset_name, None)
        .await
        .expect("Failed to load dataset")
        .expect("Dataset should exist");

    let block = dataset
        .start_block
        .expect("Dataset should have a start block");

    // Create reference snapshot from pre-loaded snapshot data
    let reference = {
        let tables = test_helpers::restore_dataset_snapshot(
            test_env.daemon_server().config(),
            test_env.metadata_db(),
            test_env.daemon_server().dataset_store(),
            dataset_name,
        )
        .await
        .expect("Failed to restore snapshot dataset");

        SnapshotContext::from_tables(test_env.daemon_server().config(), tables)
            .await
            .expect("Failed to create reference snapshot")
    };

    // * When
    // Dump the dataset and create a snapshot from it
    let dumped = {
        let dumped_tables = dump(
            test_env.daemon_server().config().clone(),
            test_env.metadata_db().clone(),
            vec![dataset_name.to_string()],
            true,               // force_reprocess
            Some(block as i64), // end_block
            1,                  // n_jobs
            100,                // partition_size_mb
            None,               // start_block
            None,               // microbatch_max_interval
            None,               // microbatch_max_rows
            false,              // skip_consistency_check
            None,               // metrics
            None,               // meter
            false,              // track_progress
        )
        .await
        .expect("Failed to dump dataset");

        SnapshotContext::from_tables(test_env.daemon_server().config(), dumped_tables)
            .await
            .expect("Failed to create temp dump snapshot")
    };

    //* Then
    // Validate table consistency
    for table in dumped.physical_tables() {
        test_helpers::check_table_consistency(table)
            .await
            .expect("Table consistency check failed");
    }

    // Compare snapshots
    test_helpers::assert_snapshots_eq(&dumped, &reference).await;
}

#[tokio::test]
async fn eth_beacon_single_dump() {
    logging::init();

    // * Given
    let dataset_name = "eth_beacon";
    let test_env = TestCtxBuilder::new("eth_beacon_single_dump")
        .with_dataset_manifest(dataset_name)
        .with_provider_config("beacon_eth_mainnet")
        .with_dataset_snapshot(dataset_name)
        .build()
        .await
        .expect("Failed to build test environment");

    let dataset = test_env
        .daemon_server()
        .dataset_store()
        .get_dataset(dataset_name, None)
        .await
        .expect("Failed to load dataset")
        .expect("Dataset should exist");

    let block = dataset
        .start_block
        .expect("Dataset should have a start block");

    // Create reference snapshot from pre-loaded snapshot data
    let reference = {
        let tables = test_helpers::restore_dataset_snapshot(
            test_env.daemon_server().config(),
            test_env.metadata_db(),
            test_env.daemon_server().dataset_store(),
            dataset_name,
        )
        .await
        .expect("Failed to restore snapshot dataset");

        SnapshotContext::from_tables(test_env.daemon_server().config(), tables)
            .await
            .expect("Failed to create reference snapshot")
    };

    // * When
    // Dump the dataset and create a snapshot from it
    let dumped = {
        let dumped_tables = dump(
            test_env.daemon_server().config().clone(),
            test_env.metadata_db().clone(),
            vec![dataset_name.to_string()],
            true,               // force_reprocess
            Some(block as i64), // end_block
            1,                  // n_jobs
            100,                // partition_size_mb
            None,               // start_block
            None,               // microbatch_max_interval
            None,               // microbatch_max_rows
            false,              // skip_consistency_check
            None,               // metrics
            None,               // meter
            false,              // track_progress
        )
        .await
        .expect("Failed to dump dataset");

        SnapshotContext::from_tables(test_env.daemon_server().config(), dumped_tables)
            .await
            .expect("Failed to create temp dump snapshot")
    };

    //* Then
    // Validate table consistency
    for table in dumped.physical_tables() {
        test_helpers::check_table_consistency(table)
            .await
            .expect("Table consistency check failed");
    }

    // Compare snapshots
    test_helpers::assert_snapshots_eq(&dumped, &reference).await;
}

#[tokio::test]
async fn evm_rpc_single_dump_fetch_receipts_per_tx() {
    logging::init();

    // * Given
    let dataset_name = "eth_rpc";
    let test_env = TestCtxBuilder::new("evm_rpc_single_dump_fetch_receipts_per_tx")
        .with_dataset_manifest(dataset_name)
        .with_provider_config("per_tx_receipt/rpc_eth_mainnet") // Special provider with fetch_receipts_per_tx = true
        .with_dataset_snapshot(dataset_name)
        .build()
        .await
        .expect("Failed to build test environment");

    let dataset = test_env
        .daemon_server()
        .dataset_store()
        .get_dataset(dataset_name, None)
        .await
        .expect("Failed to load dataset")
        .expect("Dataset should exist");

    let block = dataset
        .start_block
        .expect("Dataset should have a start block");

    // Create reference snapshot from pre-loaded snapshot data
    let reference = {
        let tables = test_helpers::restore_dataset_snapshot(
            test_env.daemon_server().config(),
            test_env.metadata_db(),
            test_env.daemon_server().dataset_store(),
            dataset_name,
        )
        .await
        .expect("Failed to restore snapshot dataset");

        SnapshotContext::from_tables(test_env.daemon_server().config(), tables)
            .await
            .expect("Failed to create reference snapshot")
    };

    // * When
    // Dump the dataset and create a snapshot from it
    let dumped = {
        let dumped_tables = dump(
            test_env.daemon_server().config().clone(),
            test_env.metadata_db().clone(),
            vec![dataset_name.to_string()],
            true,               // force_reprocess
            Some(block as i64), // end_block
            1,                  // n_jobs
            100,                // partition_size_mb
            None,               // start_block
            None,               // microbatch_max_interval
            None,               // microbatch_max_rows
            false,              // skip_consistency_check
            None,               // metrics
            None,               // meter
            false,              // track_progress
        )
        .await
        .expect("Failed to dump dataset");

        SnapshotContext::from_tables(test_env.daemon_server().config(), dumped_tables)
            .await
            .expect("Failed to create temp dump snapshot")
    };

    //* Then
    // Validate table consistency
    for table in dumped.physical_tables() {
        test_helpers::check_table_consistency(table)
            .await
            .expect("Table consistency check failed");
    }

    // Compare snapshots
    test_helpers::assert_snapshots_eq(&dumped, &reference).await;
}

#[tokio::test]
async fn evm_rpc_base_single_dump() {
    logging::init();

    // * Given
    let dataset_name = "base_rpc";
    let test_env = TestCtxBuilder::new("evm_rpc_base_single_dump")
        .with_dataset_manifest(dataset_name)
        .with_provider_config("rpc_eth_base")
        .with_dataset_snapshot(dataset_name)
        .build()
        .await
        .expect("Failed to build test environment");

    let dataset = test_env
        .daemon_server()
        .dataset_store()
        .get_dataset(dataset_name, None)
        .await
        .expect("Failed to load dataset")
        .expect("Dataset should exist");

    let block = dataset
        .start_block
        .expect("Dataset should have a start block");

    // Create reference snapshot from pre-loaded snapshot data
    let reference = {
        let tables = test_helpers::restore_dataset_snapshot(
            test_env.daemon_server().config(),
            test_env.metadata_db(),
            test_env.daemon_server().dataset_store(),
            dataset_name,
        )
        .await
        .expect("Failed to restore snapshot dataset");

        SnapshotContext::from_tables(test_env.daemon_server().config(), tables)
            .await
            .expect("Failed to create reference snapshot")
    };

    // * When
    // Dump the dataset and create a snapshot from it
    let dumped = {
        let dumped_tables = dump(
            test_env.daemon_server().config().clone(),
            test_env.metadata_db().clone(),
            vec![dataset_name.to_string()],
            true,               // force_reprocess
            Some(block as i64), // end_block
            1,                  // n_jobs
            100,                // partition_size_mb
            None,               // start_block
            None,               // microbatch_max_interval
            None,               // microbatch_max_rows
            false,              // skip_consistency_check
            None,               // metrics
            None,               // meter
            false,              // track_progress
        )
        .await
        .expect("Failed to dump dataset");

        SnapshotContext::from_tables(test_env.daemon_server().config(), dumped_tables)
            .await
            .expect("Failed to create temp dump snapshot")
    };

    //* Then
    // Validate table consistency
    for table in dumped.physical_tables() {
        test_helpers::check_table_consistency(table)
            .await
            .expect("Table consistency check failed");
    }

    // Compare snapshots
    test_helpers::assert_snapshots_eq(&dumped, &reference).await;
}

#[tokio::test]
async fn evm_rpc_base_single_dump_fetch_receipts_per_tx() {
    logging::init();

    // * Given
    let dataset_name = "base_rpc";
    let test_env = TestCtxBuilder::new("evm_rpc_base_single_dump_fetch_receipts_per_tx")
        .with_dataset_manifest(dataset_name)
        .with_provider_config("per_tx_receipt/rpc_eth_base") // Special provider with fetch_receipts_per_tx = true
        .with_dataset_snapshot(dataset_name)
        .build()
        .await
        .expect("Failed to build test environment");

    let dataset = test_env
        .daemon_server()
        .dataset_store()
        .get_dataset(dataset_name, None)
        .await
        .expect("Failed to load dataset")
        .expect("Dataset should exist");

    let block = dataset
        .start_block
        .expect("Dataset should have a start block");

    // Create reference snapshot from pre-loaded snapshot data
    let reference = {
        let tables = test_helpers::restore_dataset_snapshot(
            test_env.daemon_server().config(),
            test_env.metadata_db(),
            test_env.daemon_server().dataset_store(),
            dataset_name,
        )
        .await
        .expect("Failed to restore snapshot dataset");

        SnapshotContext::from_tables(test_env.daemon_server().config(), tables)
            .await
            .expect("Failed to create reference snapshot")
    };

    // * When
    // Dump the dataset and create a snapshot from it
    let dumped = {
        let dumped_tables = dump(
            test_env.daemon_server().config().clone(),
            test_env.metadata_db().clone(),
            vec![dataset_name.to_string()],
            true,               // force_reprocess
            Some(block as i64), // end_block
            1,                  // n_jobs
            100,                // partition_size_mb
            None,               // start_block
            None,               // microbatch_max_interval
            None,               // microbatch_max_rows
            false,              // skip_consistency_check
            None,               // metrics
            None,               // meter
            false,              // track_progress
        )
        .await
        .expect("Failed to dump dataset");

        SnapshotContext::from_tables(test_env.daemon_server().config(), dumped_tables)
            .await
            .expect("Failed to create temp dump snapshot")
    };

    //* Then
    // Validate table consistency
    for table in dumped.physical_tables() {
        test_helpers::check_table_consistency(table)
            .await
            .expect("Table consistency check failed");
    }

    // Compare snapshots
    test_helpers::assert_snapshots_eq(&dumped, &reference).await;
}

#[tokio::test]
async fn evm_rpc_base_failed_tx_filtering() {
    logging::init();

    // * Given
    let dataset_name = "base_rpc_failed_tx_filtering";
    let test_env = TestCtxBuilder::new("evm_rpc_base_failed_tx_filtering")
        // Manifest starting at a block that is known to contain failed transactions.
        .with_dataset_manifest(dataset_name)
        .with_provider_config("rpc_eth_base")
        .with_dataset_snapshot(dataset_name)
        .build()
        .await
        .expect("Failed to build test environment");

    let dataset = test_env
        .daemon_server()
        .dataset_store()
        .get_dataset(dataset_name, None)
        .await
        .expect("Failed to load dataset")
        .expect("Dataset should exist");

    let start_block = dataset
        .start_block
        .expect("Dataset should have a start block");
    let end_block = start_block + 3;

    // Create reference snapshot from pre-loaded snapshot data
    let reference = {
        let tables = test_helpers::restore_dataset_snapshot(
            test_env.daemon_server().config(),
            test_env.metadata_db(),
            test_env.daemon_server().dataset_store(),
            dataset_name,
        )
        .await
        .expect("Failed to restore snapshot dataset");

        SnapshotContext::from_tables(test_env.daemon_server().config(), tables)
            .await
            .expect("Failed to create reference snapshot")
    };

    // * When
    // Dump the dataset and create a snapshot from it
    let dumped = {
        let dumped_tables = dump(
            test_env.daemon_server().config().clone(),
            test_env.metadata_db().clone(),
            vec![dataset_name.to_string()],
            true,                   // force_reprocess
            Some(end_block as i64), // end_block
            1,                      // n_jobs
            100,                    // partition_size_mb
            None,                   // start_block
            None,                   // microbatch_max_interval
            None,                   // microbatch_max_rows
            false,                  // skip_consistency_check
            None,                   // metrics
            None,                   // meter
            false,                  // track_progress
        )
        .await
        .expect("Failed to dump dataset");

        SnapshotContext::from_tables(test_env.daemon_server().config(), dumped_tables)
            .await
            .expect("Failed to create temp dump snapshot")
    };

    //* Then
    // Validate table consistency
    for table in dumped.physical_tables() {
        test_helpers::check_table_consistency(table)
            .await
            .expect("Table consistency check failed");
    }

    // Compare snapshots
    test_helpers::assert_snapshots_eq(&dumped, &reference).await;
}

#[tokio::test]
async fn eth_firehose_single_dump() {
    logging::init();

    // * Given
    let dataset_name = "eth_firehose";
    let test_env = TestCtxBuilder::new("eth_firehose_single_dump")
        .with_dataset_manifest(dataset_name)
        .with_provider_config("firehose_eth_mainnet")
        .with_dataset_snapshot(dataset_name)
        .build()
        .await
        .expect("Failed to build test environment");

    let dataset = test_env
        .daemon_server()
        .dataset_store()
        .get_dataset(dataset_name, None)
        .await
        .expect("Failed to load dataset")
        .expect("Dataset should exist");

    let block = dataset
        .start_block
        .expect("Dataset should have a start block");

    // Create reference snapshot from pre-loaded snapshot data
    let reference = {
        let tables = test_helpers::restore_dataset_snapshot(
            test_env.daemon_server().config(),
            test_env.metadata_db(),
            test_env.daemon_server().dataset_store(),
            dataset_name,
        )
        .await
        .expect("Failed to restore snapshot dataset");

        SnapshotContext::from_tables(test_env.daemon_server().config(), tables)
            .await
            .expect("Failed to create reference snapshot")
    };

    // * When
    // Dump the dataset and create a snapshot from it
    let dumped = {
        let dumped_tables = dump(
            test_env.daemon_server().config().clone(),
            test_env.metadata_db().clone(),
            vec![dataset_name.to_string()],
            true,               // force_reprocess
            Some(block as i64), // end_block
            1,                  // n_jobs
            100,                // partition_size_mb
            None,               // start_block
            None,               // microbatch_max_interval
            None,               // microbatch_max_rows
            false,              // skip_consistency_check
            None,               // metrics
            None,               // meter
            false,              // track_progress
        )
        .await
        .expect("Failed to dump dataset");

        SnapshotContext::from_tables(test_env.daemon_server().config(), dumped_tables)
            .await
            .expect("Failed to create temp dump snapshot")
    };

    //* Then
    // Validate table consistency
    for table in dumped.physical_tables() {
        test_helpers::check_table_consistency(table)
            .await
            .expect("Table consistency check failed");
    }

    // Compare snapshots
    test_helpers::assert_snapshots_eq(&dumped, &reference).await;
}

#[tokio::test]
async fn base_firehose_single_dump() {
    logging::init();

    // * Given
    let dataset_name = "base_firehose";
    let test_env = TestCtxBuilder::new("base_firehose_single_dump")
        .with_dataset_manifest(dataset_name)
        .with_provider_config("firehose_eth_base")
        .with_dataset_snapshot(dataset_name)
        .build()
        .await
        .expect("Failed to build test environment");

    let dataset = test_env
        .daemon_server()
        .dataset_store()
        .get_dataset(dataset_name, None)
        .await
        .expect("Failed to load dataset")
        .expect("Dataset should exist");

    let block = dataset
        .start_block
        .expect("Dataset should have a start block");

    // Create reference snapshot from pre-loaded snapshot data
    let reference = {
        let tables = test_helpers::restore_dataset_snapshot(
            test_env.daemon_server().config(),
            test_env.metadata_db(),
            test_env.daemon_server().dataset_store(),
            dataset_name,
        )
        .await
        .expect("Failed to restore snapshot dataset");

        SnapshotContext::from_tables(test_env.daemon_server().config(), tables)
            .await
            .expect("Failed to create reference snapshot")
    };

    // * When
    // Dump the dataset and create a snapshot from it
    let dumped = {
        let dumped_tables = dump(
            test_env.daemon_server().config().clone(),
            test_env.metadata_db().clone(),
            vec![dataset_name.to_string()],
            true,               // force_reprocess
            Some(block as i64), // end_block
            1,                  // n_jobs
            100,                // partition_size_mb
            None,               // start_block
            None,               // microbatch_max_interval
            None,               // microbatch_max_rows
            false,              // skip_consistency_check
            None,               // metrics
            None,               // meter
            false,              // track_progress
        )
        .await
        .expect("Failed to dump dataset");

        SnapshotContext::from_tables(test_env.daemon_server().config(), dumped_tables)
            .await
            .expect("Failed to create temp dump snapshot")
    };

    //* Then
    // Validate table consistency
    for table in dumped.physical_tables() {
        test_helpers::check_table_consistency(table)
            .await
            .expect("Table consistency check failed");
    }

    // Compare snapshots
    test_helpers::assert_snapshots_eq(&dumped, &reference).await;
}
