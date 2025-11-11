use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use common::{
    BoxError, ParquetFooterCache, catalog::physical::PhysicalTable, metadata::Generation,
};
use dataset_store::DatasetStore;
use datasets_common::reference::Reference;
use dump::{
    compaction::{AmpCompactor, SegmentSizeLimit},
    parquet_opts,
};
use futures::StreamExt;
use monitoring::logging;

use crate::testlib::{self, fixtures::DatasetPackage, helpers as test_helpers};

#[tokio::test]
async fn sql_dataset_input_batch_size() {
    let test = TestCtx::setup("sql_dataset_input_batch_size").await;

    // 2. First dump eth_rpc dependency on the spot
    let eth_rpc_ref: Reference = "_/eth_rpc@latest"
        .parse()
        .expect("should be valid reference");
    let start = test
        .dataset_store()
        .get_dataset(eth_rpc_ref)
        .await
        .unwrap()
        .start_block
        .unwrap();
    let end = start + 3;

    test.dump_dataset("_/eth_rpc@0.0.0", end, 1, None).await;

    // 3. Execute dump of sql_stream_ds with microbatch_max_interval=1
    test.dump_dataset("_/sql_stream_ds@0.0.0", end, 1, Some(1))
        .await;

    // 4. Get catalog and count files
    let catalog = test.catalog_for_dataset("sql_stream_ds").await.unwrap();

    // Find the even_blocks table
    let table = catalog
        .tables()
        .iter()
        .find(|t| t.table_name() == "even_blocks")
        .unwrap();

    let file_count = table.files().await.unwrap().len();

    // 5. With batch size 1 and 4 blocks, we expect 4 files to be dumped (even if some are empty)
    // since microbatch_max_interval=1 should create one file per block even_blocks only includes
    // even block numbers, so we expect 2 files with data for blocks 15000000 and 15000002, plus
    // empty files for odd blocks.
    assert_eq!(file_count, 4);

    test.spawn_compaction_and_await_completion(table).await;

    tokio::time::sleep(Duration::from_millis(150)).await; // Ensure file locks have expired

    // 6. After compaction, we expect an additional file to be created, with all data in it.
    let file_count_after = table.files().await.unwrap().len();
    assert_eq!(file_count_after, 5);

    test.spawn_collection_and_await_completion(table).await;
    // 7. After collection, we expect the original 4 files to be deleted,
    // leaving only the compacted file.
    let file_count_final = table
        .object_store()
        .list(Some(table.path()))
        .collect::<Vec<_>>()
        .await
        .len();
    assert_eq!(file_count_final, 1);

    let mut test_client = test.new_flight_client().await.unwrap();
    let (res, _batch_count) = test_client
        .run_query("select count(*) from sql_stream_ds.even_blocks", None)
        .await
        .unwrap();
    assert_eq!(res, serde_json::json!([{"count(*)": 2}]));
}

/// Test context wrapper for SQL dataset batch size testing.
///
/// This provides convenience methods for testing dataset dumping, compaction,
/// and collection workflows with specific batch size configurations.
struct TestCtx {
    ctx: testlib::ctx::TestCtx,
    cache: ParquetFooterCache,
}

impl TestCtx {
    /// Set up a new test context for SQL dataset batch size testing.
    async fn setup(test_name: &str) -> Self {
        logging::init();

        let ctx = testlib::ctx::TestCtxBuilder::new(test_name)
            .with_provider_config("rpc_eth_mainnet")
            .with_dataset_manifests(["eth_rpc"])
            .build()
            .await
            .expect("Failed to create test context");

        let cache = ParquetFooterCache::builder(
            (ctx.daemon_server().config().parquet.cache_size_mb * 1024 * 1024) as usize,
        )
        .build();

        // Deploy the TypeScript dataset
        let sql_stream_ds = DatasetPackage::new("sql_stream_ds", Some("amp.config.ts"));
        let cli = ctx.new_amp_cli();
        sql_stream_ds
            .register(&cli, "0.0.0")
            .await
            .expect("Failed to register sql_stream_ds dataset");

        Self { ctx, cache }
    }

    /// Get reference to the dataset store.
    fn dataset_store(&self) -> &Arc<DatasetStore> {
        self.ctx.daemon_server().dataset_store()
    }

    /// Create a new Flight client for this test context.
    async fn new_flight_client(&self) -> Result<testlib::fixtures::FlightClient, BoxError> {
        self.ctx.new_flight_client().await
    }

    /// Dump a dataset using testlib dump_dataset helper.
    async fn dump_dataset(
        &self,
        dataset: &str,
        end: u64,
        max_writers: u16,
        microbatch_max_interval: impl Into<Option<u64>>,
    ) {
        let dataset_ref: Reference = dataset.parse().unwrap();
        test_helpers::dump_dataset(
            self.ctx.daemon_server().config(),
            self.ctx.metadata_db(),
            dataset_ref,
            end,
            max_writers,
            microbatch_max_interval,
        )
        .await
        .expect("Failed to dump dataset");
    }

    /// Create a catalog for the specified dataset.
    async fn catalog_for_dataset(
        &self,
        dataset_name: &str,
    ) -> Result<common::catalog::physical::Catalog, BoxError> {
        test_helpers::catalog_for_dataset(
            dataset_name,
            self.ctx.daemon_server().dataset_store(),
            self.ctx.metadata_db(),
        )
        .await
    }

    /// Spawn compaction for a table and wait for completion.
    async fn spawn_compaction_and_await_completion(&self, table: &Arc<PhysicalTable>) {
        let config = self.ctx.daemon_server().config();
        let length = table.files().await.unwrap().len();
        let mut opts = parquet_opts(&config.parquet);
        opts.compactor.active.swap(true, Ordering::SeqCst);
        opts.collector.active.swap(false, Ordering::SeqCst);
        let opts_mut = Arc::make_mut(&mut opts);
        opts_mut.collector.file_lock_duration = Duration::from_millis(25);
        opts_mut.collector.interval = Duration::ZERO;
        opts_mut.compactor.interval = Duration::ZERO;
        opts_mut.compactor.algorithm.cooldown_duration = Duration::ZERO;
        opts_mut.partition = SegmentSizeLimit::new(1, 1, 1, length, Generation::default(), 1.5);
        let cache = self.cache.clone();
        let mut task = AmpCompactor::start(table, cache, &opts, None);
        task.join_current_then_spawn_new().await.unwrap();
        while !task.is_finished() {
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(Duration::from_millis(50)).await; // Ensure file locks have expired
    }

    /// Spawn collection for a table and wait for completion.
    async fn spawn_collection_and_await_completion(&self, table: &Arc<PhysicalTable>) {
        let config = self.ctx.daemon_server().config();
        let length = table.files().await.unwrap().len();
        let mut opts = parquet_opts(&config.parquet);
        opts.compactor.active.swap(false, Ordering::SeqCst);
        opts.collector.active.swap(true, Ordering::SeqCst);
        let opts_mut = Arc::make_mut(&mut opts);
        opts_mut.collector.file_lock_duration = Duration::ZERO;
        opts_mut.collector.interval = Duration::ZERO;
        opts_mut.compactor.interval = Duration::ZERO;
        opts_mut.partition = SegmentSizeLimit::new(1, 1, 1, length, Generation::default(), 1.5);
        let cache = self.cache.clone();
        let mut task = AmpCompactor::start(table, cache, &opts, None);
        task.join_current_then_spawn_new().await.unwrap();
        while !task.is_finished() {
            tokio::task::yield_now().await;
        }
    }
}
