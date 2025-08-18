# tests/performance/test_loader_performance.py
"""
Performance tests for data loaders to ensure production readiness.
"""

import time

import pyarrow as pa
import pytest

try:
    from src.nozzle.loaders.implementations.postgresql_loader import PostgreSQLLoader
    from src.nozzle.loaders.implementations.redis_loader import RedisLoader
    from src.nozzle.loaders.implementations.snowflake_loader import SnowflakeLoader

    from .benchmarks import record_benchmark
except ImportError:
    pytest.skip('nozzle modules not available', allow_module_level=True)


@pytest.mark.performance
@pytest.mark.postgresql
class TestPostgreSQLPerformance:
    """Performance tests for PostgreSQL loader"""

    def test_large_table_loading_performance(self, postgresql_config, performance_test_data, memory_monitor):
        """Test loading large datasets with performance monitoring"""
        loader = PostgreSQLLoader(postgresql_config)

        with loader:
            start_time = time.time()
            result = loader.load_table(performance_test_data, 'perf_test_large')
            duration = time.time() - start_time

            # Performance assertions
            rows_per_second = result.rows_loaded / duration
            assert rows_per_second > 1000, f'PostgreSQL throughput too low: {rows_per_second:.0f} rows/sec'
            assert duration < 60, f'Load took too long: {duration:.2f}s'

            # Record benchmark
            memory_mb = memory_monitor.get('initial_mb', 0)
            record_benchmark('large_table_loading_performance', 'postgresql', {'throughput': rows_per_second, 'memory_mb': memory_mb, 'duration': duration, 'dataset_size': result.rows_loaded})

            # Cleanup
            with loader.pool.getconn() as conn:
                try:
                    with conn.cursor() as cur:
                        cur.execute('DROP TABLE IF EXISTS perf_test_large')
                        conn.commit()
                finally:
                    loader.pool.putconn(conn)

    def test_batch_performance_scaling(self, postgresql_config, performance_test_data):
        """Test performance scaling with different batch processing approaches"""
        from src.nozzle.loaders.base import LoadMode
        
        batch_approaches = {
            'single_load': 50000,    # Load entire table at once
            'large_batches': 10000,  # Split into 5 batches of 10k rows
            'medium_batches': 5000,  # Split into 10 batches of 5k rows
            'small_batches': 1000,   # Split into 50 batches of 1k rows
        }
        results = {}

        for approach_name, batch_size in batch_approaches.items():
            loader = PostgreSQLLoader(postgresql_config)
            table_name = f'perf_batch_{approach_name}'

            with loader:
                start_time = time.time()
                
                if approach_name == 'single_load':
                    result = loader.load_table(performance_test_data, table_name)
                    total_rows = result.rows_loaded
                else:
                    total_rows = 0
                    num_rows = performance_test_data.num_rows
                    
                    for i in range(0, num_rows, batch_size):
                        end_idx = min(i + batch_size, num_rows)
                        batch_data = performance_test_data.slice(i, end_idx - i)
                        
                        mode = LoadMode.OVERWRITE if i == 0 else LoadMode.APPEND
                        batch_result = loader.load_batch(batch_data.to_batches()[0], table_name, mode=mode)
                        
                        if not batch_result.success:
                            raise RuntimeError(f"Batch {i//batch_size + 1} failed: {batch_result.error}")
                        
                        total_rows += batch_result.rows_loaded
                
                duration = time.time() - start_time
                throughput = total_rows / duration
                results[approach_name] = throughput

                print(f"{approach_name}: {total_rows} rows in {duration:.2f}s = {throughput:.0f} rows/sec")

                # Cleanup
                with loader.pool.getconn() as conn:
                    try:
                        with conn.cursor() as cur:
                            cur.execute(f'DROP TABLE IF EXISTS {table_name}')
                            conn.commit()
                    finally:
                        loader.pool.putconn(conn)

        single_load_perf = results['single_load']
        small_batch_perf = results['small_batches']

        assert single_load_perf > small_batch_perf * 0.8, \
            f'Single load ({single_load_perf:.0f}) should outperform small batches ({small_batch_perf:.0f})'

        # All approaches should achieve reasonable performance
        for approach, throughput in results.items():
            assert throughput > 500, f'{approach} too slow: {throughput:.0f} rows/sec'

    def test_connection_pool_performance(self, postgresql_config, small_test_table):
        """Test connection pool efficiency under load"""
        config = {**postgresql_config, 'max_connections': 5}
        loader = PostgreSQLLoader(config)

        with loader:
            # Simulate concurrent loads
            start_time = time.time()
            for i in range(10):
                result = loader.load_table(small_test_table, f'pool_test_{i}')
                assert result.success
            duration = time.time() - start_time

            # Should complete within reasonable time with connection pooling
            assert duration < 30, f'Connection pool inefficient: {duration:.2f}s'

            # Cleanup
            with loader.pool.getconn() as conn:
                try:
                    with conn.cursor() as cur:
                        for i in range(10):
                            cur.execute(f'DROP TABLE IF EXISTS pool_test_{i}')
                        conn.commit()
                finally:
                    loader.pool.putconn(conn)


@pytest.mark.performance
@pytest.mark.redis
class TestRedisPerformance:
    """Performance tests for Redis loader"""

    def test_pipeline_performance(self, redis_config, performance_test_data):
        """Test Redis pipeline performance optimization"""
        # Test with and without pipelining
        configs = [{**redis_config, 'pipeline_size': 1, 'data_structure': 'hash'}, {**redis_config, 'pipeline_size': 1000, 'data_structure': 'hash'}]

        results = {}

        for i, config in enumerate(configs):
            loader = RedisLoader(config)

            with loader:
                start_time = time.time()
                result = loader.load_table(performance_test_data, f'pipeline_test_{i}')
                duration = time.time() - start_time

                results[config['pipeline_size']] = result.rows_loaded / duration

                # Cleanup
                loader.redis_client.flushdb()

        # Pipelining should significantly improve performance
        assert results[1000] > results[1] * 2, 'Pipeline optimization not effective'

        # Record benchmark for pipelined performance
        record_benchmark(
            'pipeline_performance',
            'redis',
            {
                'throughput': results[1000],
                'memory_mb': 0,  # Not measured in this test
                'duration': 0,  # Not measured separately
                'dataset_size': performance_test_data.num_rows,
            },
        )

    def test_data_structure_performance(self, redis_config, performance_test_data):
        """Compare performance across Redis data structures"""
        structures = ['hash', 'string', 'sorted_set']
        results = {}

        for structure in structures:
            config = {**redis_config, 'data_structure': structure, 'pipeline_size': 1000, 'score_field': 'score' if structure == 'sorted_set' else None}
            loader = RedisLoader(config)

            with loader:
                start_time = time.time()
                result = loader.load_table(performance_test_data, f'struct_test_{structure}')
                duration = time.time() - start_time

                results[structure] = result.rows_loaded / duration

                # Cleanup
                loader.redis_client.flushdb()

        # All structures should achieve reasonable performance
        for structure, ops_per_sec in results.items():
            assert ops_per_sec > 500, f'{structure} too slow: {ops_per_sec:.0f} ops/sec'

            # Record benchmark for each data structure
            record_benchmark(f'data_structure_performance_{structure}', 'redis', {'throughput': ops_per_sec, 'memory_mb': 0, 'duration': 0, 'dataset_size': performance_test_data.num_rows})

    def test_memory_efficiency(self, redis_config, performance_test_data, memory_monitor):
        """Test Redis loader memory efficiency"""
        config = {**redis_config, 'data_structure': 'hash', 'pipeline_size': 1000}
        loader = RedisLoader(config)

        with loader:
            result = loader.load_table(performance_test_data, 'memory_test')

            # Get Redis memory usage
            info = loader.redis_client.info('memory')
            redis_memory_mb = info['used_memory'] / 1024 / 1024

            # Redis has overhead but should be reasonable for production use
            # For small datasets, Redis overhead is significant, so we check against absolute limits
            data_size_mb = performance_test_data.nbytes / 1024 / 1024
            if data_size_mb > 10:  # For larger datasets, check ratio
                assert redis_memory_mb < data_size_mb * 3, f'Redis memory too high: {redis_memory_mb:.1f}MB'
            else:  # For small datasets, check absolute limit
                assert redis_memory_mb < 50, f'Redis memory too high for small dataset: {redis_memory_mb:.1f}MB'

            # Record benchmark
            record_benchmark('memory_efficiency', 'redis', {'throughput': result.rows_loaded / result.duration if result.duration > 0 else 0, 'memory_mb': redis_memory_mb, 'duration': result.duration, 'dataset_size': result.rows_loaded})

            # Cleanup
            loader.redis_client.flushdb()


@pytest.mark.performance
@pytest.mark.delta_lake
class TestDeltaLakePerformance:
    """Performance tests for Delta Lake loader"""

    def test_large_file_write_performance(self, delta_basic_config, performance_test_data, memory_monitor):
        """Test Delta Lake write performance for large files"""
        try:
            from src.nozzle.loaders.implementations.deltalake_loader import DELTALAKE_AVAILABLE, DeltaLakeLoader
            # Skip all tests if deltalake is not available
            if not DELTALAKE_AVAILABLE:
                pytest.skip('Delta Lake not available', allow_module_level=True)
        except ImportError:
            pytest.skip('nozzle modules not available', allow_module_level=True)

        loader = DeltaLakeLoader(delta_basic_config)

        with loader:
            start_time = time.time()
            result = loader.load_table(performance_test_data, 'large_perf_test')
            duration = time.time() - start_time

            # Performance assertions
            rows_per_second = result.rows_loaded / duration
            assert rows_per_second > 5000, f'Delta Lake throughput too low: {rows_per_second:.0f} rows/sec'
            assert duration < 30, f'Write took too long: {duration:.2f}s'

            # Record benchmark
            memory_mb = memory_monitor.get('initial_mb', 0)
            record_benchmark('large_file_write_performance', 'delta_lake', {'throughput': rows_per_second, 'memory_mb': memory_mb, 'duration': duration, 'dataset_size': result.rows_loaded})

    def test_partitioned_write_performance(self, delta_partitioned_config, performance_test_data):
        """Test partitioned write performance"""
        try:
            from src.nozzle.loaders.implementations.deltalake_loader import DeltaLakeLoader
        except ImportError:
            pytest.skip('Delta Lake loader not available')

        # Add missing partition column to test data (year and month already exist)
        data_dict = performance_test_data.to_pydict()
        data_dict['day'] = [(i % 28) + 1 for i in range(len(data_dict['id']))]
        partitioned_table = pa.Table.from_pydict(data_dict)

        loader = DeltaLakeLoader(delta_partitioned_config)

        with loader:
            start_time = time.time()
            result = loader.load_table(partitioned_table, 'partitioned_perf_test')
            duration = time.time() - start_time

            # Partitioned writes should still be reasonably fast
            rows_per_second = result.rows_loaded / duration
            assert rows_per_second > 2000, f'Partitioned write too slow: {rows_per_second:.0f} rows/sec'


@pytest.mark.performance
@pytest.mark.snowflake
class TestSnowflakePerformance:
    """Performance tests for Snowflake loader"""

    def test_large_table_loading_performance(self, snowflake_config, performance_test_data, memory_monitor):
        """Test loading large datasets with performance monitoring"""
        config = {**snowflake_config, 'use_stage': True, 'batch_size': 10000}
        loader = SnowflakeLoader(config)
        table_name = 'perf_test_large_snowflake'

        try:
            with loader:
                start_time = time.time()
                result = loader.load_table(performance_test_data, table_name)
                duration = time.time() - start_time

                # Performance assertions - Snowflake should handle large loads well
                rows_per_second = result.rows_loaded / duration
                assert rows_per_second > 500, f'Snowflake throughput too low: {rows_per_second:.0f} rows/sec'
                assert duration < 120, f'Load took too long: {duration:.2f}s'

                # Record benchmark
                memory_mb = memory_monitor.get('initial_mb', 0)
                record_benchmark(
                    'large_table_loading_performance',
                    'snowflake',
                    {
                        'throughput': rows_per_second,
                        'memory_mb': memory_mb,
                        'duration': duration,
                        'dataset_size': result.rows_loaded,
                        'loading_method': 'stage' if config['use_stage'] else 'insert'
                    }
                )

                print(f"\nSnowflake Performance Metrics:")
                print(f"  Rows loaded: {result.rows_loaded:,}")
                print(f"  Duration: {duration:.2f}s")
                print(f"  Throughput: {rows_per_second:,.0f} rows/sec")
                print(f"  Loading method: {'stage' if config['use_stage'] else 'insert'}")
                print(f"  Batches processed: {result.metadata.get('batches_processed', 1)}")

        finally:
            # Cleanup
            try:
                if loader._is_connected:
                    loader.cursor.execute(f'DROP TABLE IF EXISTS {table_name}')
                    loader.connection.commit()
            except Exception:
                pass

    def test_stage_vs_insert_performance(self, snowflake_config, medium_test_table):
        """Compare performance between stage loading and insert loading"""
        results = {}
        table_base_name = 'perf_stage_vs_insert'

        # Test both loading methods
        for use_stage in [True, False]:
            method_name = 'stage' if use_stage else 'insert'
            table_name = f'{table_base_name}_{method_name}'
            config = {**snowflake_config, 'use_stage': use_stage, 'batch_size': 5000}
            loader = SnowflakeLoader(config)

            try:
                with loader:
                    start_time = time.time()
                    result = loader.load_table(medium_test_table, table_name)
                    duration = time.time() - start_time

                    throughput = result.rows_loaded / duration
                    results[method_name] = {
                        'throughput': throughput,
                        'duration': duration,
                        'rows_loaded': result.rows_loaded,
                        'success': result.success
                    }

                    assert result.success, f'{method_name} loading failed: {result.error}'
                    assert throughput > 200, f'{method_name} throughput too low: {throughput:.0f} rows/sec'

            finally:
                # Cleanup
                try:
                    if loader._is_connected:
                        loader.cursor.execute(f'DROP TABLE IF EXISTS {table_name}')
                        loader.connection.commit()
                except Exception:
                    pass

        # Compare results
        stage_throughput = results['stage']['throughput']
        insert_throughput = results['insert']['throughput']

        print(f"\nSnowflake Loading Method Comparison:")
        print(f"  Stage loading: {stage_throughput:,.0f} rows/sec")
        print(f"  Insert loading: {insert_throughput:,.0f} rows/sec")
        print(f"  Stage vs Insert ratio: {stage_throughput/insert_throughput:.2f}x")

        # Stage loading should generally be faster for larger datasets
        if medium_test_table.num_rows > 1000:
            assert stage_throughput >= insert_throughput * 0.5, "Stage loading significantly slower than expected"

    def test_batch_size_performance_scaling(self, snowflake_config, performance_test_data):
        """Test performance scaling with different batch sizes"""
        batch_sizes = [1000, 5000, 10000, 25000]
        results = {}
        table_base_name = 'perf_batch_scaling'

        for batch_size in batch_sizes:
            table_name = f'{table_base_name}_{batch_size}'
            config = {**snowflake_config, 'use_stage': True, 'batch_size': batch_size}
            loader = SnowflakeLoader(config)

            try:
                with loader:
                    start_time = time.time()
                    result = loader.load_table(performance_test_data, table_name)
                    duration = time.time() - start_time

                    throughput = result.rows_loaded / duration
                    results[batch_size] = {
                        'throughput': throughput,
                        'duration': duration,
                        'batches_processed': result.metadata.get('batches_processed', 1)
                    }

                    assert result.success, f'Batch size {batch_size} failed: {result.error}'

            finally:
                # Cleanup
                try:
                    if loader._is_connected:
                        loader.cursor.execute(f'DROP TABLE IF EXISTS {table_name}')
                        loader.connection.commit()
                except Exception:
                    pass

        # Find optimal batch size
        best_batch_size = max(results.keys(), key=lambda x: results[x]['throughput'])
        best_throughput = results[best_batch_size]['throughput']

        print(f"\nSnowflake Batch Size Performance:")
        for batch_size, metrics in results.items():
            print(f"  {batch_size:,} rows/batch: {metrics['throughput']:,.0f} rows/sec ({metrics['batches_processed']} batches)")
        print(f"  Optimal batch size: {best_batch_size:,} rows ({best_throughput:,.0f} rows/sec)")

        # All batch sizes should achieve reasonable performance
        for batch_size, metrics in results.items():
            assert metrics['throughput'] > 100, f'Batch size {batch_size} too slow: {metrics["throughput"]:.0f} rows/sec'

    def test_concurrent_loading_performance(self, snowflake_config, medium_test_table):
        """Test performance with concurrent batch loading"""
        import concurrent.futures
        from src.nozzle.loaders.base import LoadMode

        config = {**snowflake_config, 'use_stage': True, 'batch_size': 2000}
        table_name = 'perf_concurrent_test'

        # Split data into chunks for concurrent processing
        num_chunks = 4
        chunk_size = medium_test_table.num_rows // num_chunks
        chunks = []

        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = (i + 1) * chunk_size if i < num_chunks - 1 else medium_test_table.num_rows
            chunk = medium_test_table.slice(start_idx, end_idx - start_idx)
            chunks.append(chunk)

        def load_chunk(chunk_data, chunk_id):
            loader = SnowflakeLoader(config)
            chunk_table_name = f'{table_name}_{chunk_id}'

            try:
                with loader:
                    start_time = time.time()
                    result = loader.load_table(chunk_data, chunk_table_name)
                    duration = time.time() - start_time

                    return {
                        'chunk_id': chunk_id,
                        'rows_loaded': result.rows_loaded,
                        'duration': duration,
                        'throughput': result.rows_loaded / duration,
                        'success': result.success
                    }
            finally:
                # Cleanup
                try:
                    if loader._is_connected:
                        loader.cursor.execute(f'DROP TABLE IF EXISTS {chunk_table_name}')
                        loader.connection.commit()
                except Exception:
                    pass

        # Test concurrent loading
        start_time = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_chunks) as executor:
            futures = [executor.submit(load_chunk, chunk, i) for i, chunk in enumerate(chunks)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]

        total_duration = time.time() - start_time
        total_rows = sum(r['rows_loaded'] for r in results)
        overall_throughput = total_rows / total_duration

        print(f"\nSnowflake Concurrent Loading Performance:")
        print(f"  Chunks processed: {len(results)}")
        print(f"  Total rows: {total_rows:,}")
        print(f"  Total duration: {total_duration:.2f}s")
        print(f"  Overall throughput: {overall_throughput:,.0f} rows/sec")

        # All chunks should succeed
        for result in results:
            assert result['success'], f"Chunk {result['chunk_id']} failed"
            assert result['throughput'] > 50, f"Chunk {result['chunk_id']} too slow: {result['throughput']:.0f} rows/sec"

        # Concurrent loading should be reasonably efficient
        assert overall_throughput > 200, f'Concurrent loading too slow: {overall_throughput:.0f} rows/sec'


@pytest.mark.performance
class TestCrossLoaderPerformance:
    """Performance comparison tests across all loaders"""

    def test_throughput_comparison(self, postgresql_config, redis_config, snowflake_config, delta_basic_config, medium_test_table):
        """Compare throughput across all loaders with medium dataset"""
        results = {}

        # Test PostgreSQL
        pg_loader = PostgreSQLLoader(postgresql_config)
        with pg_loader:
            start_time = time.time()
            result = pg_loader.load_table(medium_test_table, 'throughput_test')
            duration = time.time() - start_time
            results['postgresql'] = result.rows_loaded / duration
            with pg_loader.pool.getconn() as conn:
                try:
                    with conn.cursor() as cur:
                        cur.execute('DROP TABLE IF EXISTS throughput_test')
                        conn.commit()
                finally:
                    pg_loader.pool.putconn(conn)

        # Test Redis
        redis_config_perf = {**redis_config, 'data_structure': 'hash', 'pipeline_size': 1000}
        redis_loader = RedisLoader(redis_config_perf)
        with redis_loader:
            start_time = time.time()
            result = redis_loader.load_table(medium_test_table, 'throughput_test')
            duration = time.time() - start_time
            results['redis'] = result.rows_loaded / duration
            redis_loader.redis_client.flushdb()

        # Test Snowflake
        try:
            snowflake_config_perf = {**snowflake_config, 'use_stage': True, 'batch_size': 5000}
            snowflake_loader = SnowflakeLoader(snowflake_config_perf)
            with snowflake_loader:
                start_time = time.time()
                result = snowflake_loader.load_table(medium_test_table, 'throughput_test')
                duration = time.time() - start_time
                results['snowflake'] = result.rows_loaded / duration

                # Cleanup
                try:
                    snowflake_loader.cursor.execute('DROP TABLE IF EXISTS throughput_test')
                    snowflake_loader.connection.commit()
                except Exception:
                    pass
        except Exception as e:
            print(f"Snowflake test skipped: {e}")
            results['snowflake'] = 0

        # Test Delta Lake
        try:
            from src.nozzle.loaders.implementations.deltalake_loader import DeltaLakeLoader

            delta_loader = DeltaLakeLoader(delta_basic_config)
            with delta_loader:
                start_time = time.time()
                result = delta_loader.load_table(medium_test_table, 'throughput_test')
                duration = time.time() - start_time
                results['delta_lake'] = result.rows_loaded / duration
        except ImportError:
            results['delta_lake'] = 0

        # All loaders should achieve minimum throughput
        for loader_name, throughput in results.items():
            if throughput > 0:  # Skip if loader not available
                assert throughput > 100, f'{loader_name} throughput too low: {throughput:.0f} rows/sec'

                # Record benchmark for cross-loader comparison
                record_benchmark(
                    'throughput_comparison',
                    loader_name,
                    {
                        'throughput': throughput,
                        'memory_mb': 0,  # Not measured in this test
                        'duration': 0,  # Not measured separately
                        'dataset_size': medium_test_table.num_rows,
                    },
                )

        print('\nThroughput comparison (rows/sec):')
        for loader_name, throughput in results.items():
            if throughput > 0:
                print(f'  {loader_name}: {throughput:.0f}')

    def test_memory_usage_comparison(self, postgresql_config, redis_config, snowflake_config, small_test_table):
        """Compare memory usage patterns across loaders"""
        try:
            import psutil
        except ImportError:
            pytest.skip('psutil not available for memory monitoring')

        process = psutil.Process()
        results = {}

        # Test PostgreSQL memory usage
        initial_memory = process.memory_info().rss
        pg_loader = PostgreSQLLoader(postgresql_config)
        with pg_loader:
            pg_loader.load_table(small_test_table, 'memory_test')
            peak_memory = process.memory_info().rss
            results['postgresql'] = (peak_memory - initial_memory) / 1024 / 1024
            with pg_loader.pool.getconn() as conn:
                try:
                    with conn.cursor() as cur:
                        cur.execute('DROP TABLE IF EXISTS memory_test')
                        conn.commit()
                finally:
                    pg_loader.pool.putconn(conn)

        # Test Redis memory usage
        initial_memory = process.memory_info().rss
        redis_config_mem = {**redis_config, 'data_structure': 'hash'}
        redis_loader = RedisLoader(redis_config_mem)
        with redis_loader:
            redis_loader.load_table(small_test_table, 'memory_test')
            peak_memory = process.memory_info().rss
            results['redis'] = (peak_memory - initial_memory) / 1024 / 1024
            redis_loader.redis_client.flushdb()

        # Test Snowflake memory usage
        try:
            initial_memory = process.memory_info().rss
            snowflake_config_mem = {**snowflake_config, 'use_stage': True}
            snowflake_loader = SnowflakeLoader(snowflake_config_mem)
            with snowflake_loader:
                snowflake_loader.load_table(small_test_table, 'memory_test')
                peak_memory = process.memory_info().rss
                results['snowflake'] = (peak_memory - initial_memory) / 1024 / 1024

                # Cleanup
                try:
                    snowflake_loader.cursor.execute('DROP TABLE IF EXISTS memory_test')
                    snowflake_loader.connection.commit()
                except Exception:
                    pass
        except Exception as e:
            print(f"Snowflake memory test skipped: {e}")
            results['snowflake'] = 0

        # Memory usage should be reasonable (< 100MB for small dataset)
        print(f"\nMemory usage comparison (MB):")
        for loader_name, memory_mb in results.items():
            if memory_mb > 0:  # Skip if loader not available
                print(f"  {loader_name}: {memory_mb:.1f}MB")
                assert memory_mb < 100, f'{loader_name} using too much memory: {memory_mb:.1f}MB'



@pytest.mark.performance
@pytest.mark.iceberg
class TestIcebergPerformance:
    """Performance tests for Apache Iceberg loader"""

    def test_large_file_write_performance(self, iceberg_basic_config, performance_test_data, memory_monitor):
        """Test Iceberg write performance for large files"""
        try:
            from src.nozzle.loaders.implementations.iceberg_loader import ICEBERG_AVAILABLE, IcebergLoader
            # Skip all tests if iceberg is not available
            if not ICEBERG_AVAILABLE:
                pytest.skip('Apache Iceberg not available', allow_module_level=True)
        except ImportError:
            pytest.skip('nozzle modules not available', allow_module_level=True)

        loader = IcebergLoader(iceberg_basic_config)

        with loader:
            start_time = time.time()
            result = loader.load_table(performance_test_data, 'large_perf_test')
            duration = time.time() - start_time

            # Performance assertions - Iceberg should be fast due to zero-copy Arrow
            rows_per_second = result.rows_loaded / duration
            assert rows_per_second > 10000, f'Iceberg throughput too low: {rows_per_second:.0f} rows/sec'
            assert duration < 20, f'Write took too long: {duration:.2f}s'

            # Record benchmark
            memory_mb = memory_monitor.get('initial_mb', 0)
            record_benchmark('large_file_write_performance', 'iceberg', {
                'throughput': rows_per_second,
                'memory_mb': memory_mb,
                'duration': duration,
                'dataset_size': result.rows_loaded
            })
