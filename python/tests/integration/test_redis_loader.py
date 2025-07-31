# tests/integration/test_redis_loader.py
"""
Integration tests for Redis loader implementation.
These tests require a running Redis instance.
"""

import json
import time
from datetime import datetime, timedelta

import pyarrow as pa
import pytest

try:
    from src.nozzle.loaders.base import LoadMode
    from src.nozzle.loaders.implementations.redis_loader import RedisLoader
except ImportError:
    pytest.skip('nozzle modules not available', allow_module_level=True)




@pytest.fixture
def small_test_data():
    """Create small test data for quick tests"""
    data = {'id': [1, 2, 3, 4, 5], 'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'], 'score': [100, 200, 150, 300, 250], 'active': [True, False, True, False, True], 'created_at': [datetime.now() - timedelta(days=i) for i in range(5)]}
    return pa.Table.from_pydict(data)


@pytest.fixture
def comprehensive_test_data():
    """Create comprehensive test data with various data types"""
    base_date = datetime(2024, 1, 1)

    data = {'id': list(range(1000)), 'user_id': [f'user_{i % 100}' for i in range(1000)], 'score': [i * 10 for i in range(1000)], 'text_field': [f'text_{i}' for i in range(1000)], 'float_field': [i * 0.123 for i in range(1000)], 'bool_field': [i % 2 == 0 for i in range(1000)], 'timestamp': [(base_date + timedelta(days=i // 10, hours=i % 24)).timestamp() for i in range(1000)], 'binary_field': [f'binary_{i}'.encode() for i in range(1000)], 'json_data': [json.dumps({'index': i, 'value': f'val_{i}'}) for i in range(1000)], 'nullable_field': [i if i % 10 != 0 else None for i in range(1000)]}

    return pa.Table.from_pydict(data)


@pytest.fixture
def cleanup_redis(redis_config):
    """Cleanup Redis data after tests"""
    keys_to_clean = []
    patterns_to_clean = []

    yield (keys_to_clean, patterns_to_clean)

    # Cleanup
    try:
        import redis

        r = redis.Redis(host=redis_config['host'], port=redis_config['port'], db=redis_config['db'], password=redis_config['password'])

        # Delete specific keys
        for key in keys_to_clean:
            r.delete(key)

        # Delete keys matching patterns
        for pattern in patterns_to_clean:
            for key in r.scan_iter(match=pattern, count=1000):
                r.delete(key)

        r.close()
    except Exception:
        pass


@pytest.mark.integration
@pytest.mark.redis
class TestRedisLoaderIntegration:
    """Integration tests for Redis loader"""

    def test_loader_connection(self, redis_config):
        """Test basic connection to Redis"""
        loader = RedisLoader(redis_config)

        # Test connection
        loader.connect()
        assert loader._is_connected == True
        assert loader.redis_client is not None
        assert loader.connection_pool is not None

        # Test health check
        health = loader.health_check()
        assert health['healthy'] == True
        assert 'redis_version' in health

        # Test disconnection
        loader.disconnect()
        assert loader._is_connected == False
        assert loader.redis_client is None

    def test_context_manager(self, redis_config, small_test_data, cleanup_redis):
        """Test context manager functionality"""
        keys_to_clean, patterns_to_clean = cleanup_redis
        patterns_to_clean.append('test_context:*')

        loader = RedisLoader({**redis_config, 'data_structure': 'hash'})

        with loader:
            assert loader._is_connected == True

            result = loader.load_table(small_test_data, 'test_context')
            assert result.success == True

        # Should be disconnected after context
        assert loader._is_connected == False

    def test_hash_storage(self, redis_config, small_test_data, cleanup_redis):
        """Test hash data structure storage"""
        keys_to_clean, patterns_to_clean = cleanup_redis
        patterns_to_clean.append('test_hash:*')

        config = {**redis_config, 'data_structure': 'hash', 'key_pattern': 'test_hash:{id}'}
        loader = RedisLoader(config)

        with loader:
            # Test loading
            result = loader.load_table(small_test_data, 'test_hash')

            assert result.success == True
            assert result.rows_loaded == 5
            assert result.metadata['data_structure'] == 'hash'

            # Verify data was stored
            for i in range(5):
                key = f'test_hash:{i + 1}'
                assert loader.redis_client.exists(key)

                # Check specific fields
                name = loader.redis_client.hget(key, 'name')
                assert name.decode() == ['Alice', 'Bob', 'Charlie', 'David', 'Eve'][i]

                score = loader.redis_client.hget(key, 'score')
                assert int(score.decode()) == [100, 200, 150, 300, 250][i]

    def test_string_storage(self, redis_config, small_test_data, cleanup_redis):
        """Test string (JSON) data structure storage"""
        keys_to_clean, patterns_to_clean = cleanup_redis
        patterns_to_clean.append('test_string:*')

        config = {**redis_config, 'data_structure': 'string', 'key_pattern': 'test_string:{id}'}
        loader = RedisLoader(config)

        with loader:
            result = loader.load_table(small_test_data, 'test_string')

            assert result.success == True
            assert result.rows_loaded == 5

            # Verify data was stored as JSON
            for i in range(5):
                key = f'test_string:{i + 1}'
                assert loader.redis_client.exists(key)

                # Parse JSON data
                json_data = json.loads(loader.redis_client.get(key))
                assert json_data['name'] == ['Alice', 'Bob', 'Charlie', 'David', 'Eve'][i]
                assert json_data['score'] == [100, 200, 150, 300, 250][i]

    def test_stream_storage(self, redis_config, small_test_data, cleanup_redis):
        """Test stream data structure storage"""
        keys_to_clean, patterns_to_clean = cleanup_redis
        keys_to_clean.append('test_stream:stream')

        config = {**redis_config, 'data_structure': 'stream'}
        loader = RedisLoader(config)

        with loader:
            result = loader.load_table(small_test_data, 'test_stream')

            assert result.success == True
            assert result.rows_loaded == 5

            # Verify stream was created
            stream_key = 'test_stream:stream'
            assert loader.redis_client.exists(stream_key)

            # Check stream length
            info = loader.redis_client.xinfo_stream(stream_key)
            assert info['length'] == 5

    def test_set_storage(self, redis_config, small_test_data, cleanup_redis):
        """Test set data structure storage"""
        keys_to_clean, patterns_to_clean = cleanup_redis
        keys_to_clean.append('test_set:set')

        config = {**redis_config, 'data_structure': 'set', 'unique_field': 'name'}
        loader = RedisLoader(config)

        with loader:
            result = loader.load_table(small_test_data, 'test_set')

            assert result.success == True
            assert result.rows_loaded == 5

            # Verify set was created
            set_key = 'test_set:set'
            assert loader.redis_client.exists(set_key)
            assert loader.redis_client.scard(set_key) == 5

            # Check members
            members = loader.redis_client.smembers(set_key)
            names = {m.decode() for m in members}
            assert names == {'Alice', 'Bob', 'Charlie', 'David', 'Eve'}

    def test_sorted_set_storage(self, redis_config, small_test_data, cleanup_redis):
        """Test sorted set data structure storage"""
        keys_to_clean, patterns_to_clean = cleanup_redis
        keys_to_clean.append('test_zset:zset')

        config = {**redis_config, 'data_structure': 'sorted_set', 'score_field': 'score'}
        loader = RedisLoader(config)

        with loader:
            result = loader.load_table(small_test_data, 'test_zset')

            assert result.success == True
            assert result.rows_loaded == 5

            # Verify sorted set was created
            zset_key = 'test_zset:zset'
            assert loader.redis_client.exists(zset_key)
            assert loader.redis_client.zcard(zset_key) == 5

            # Check score ordering
            members_with_scores = loader.redis_client.zrange(zset_key, 0, -1, withscores=True)
            scores = [score for _, score in members_with_scores]
            assert scores == [100.0, 150.0, 200.0, 250.0, 300.0]  # Should be sorted

    def test_list_storage(self, redis_config, small_test_data, cleanup_redis):
        """Test list data structure storage"""
        keys_to_clean, patterns_to_clean = cleanup_redis
        keys_to_clean.append('test_list:list')

        config = {**redis_config, 'data_structure': 'list'}
        loader = RedisLoader(config)

        with loader:
            result = loader.load_table(small_test_data, 'test_list')

            assert result.success == True
            assert result.rows_loaded == 5

            # Verify list was created
            list_key = 'test_list:list'
            assert loader.redis_client.exists(list_key)
            assert loader.redis_client.llen(list_key) == 5

    def test_append_mode(self, redis_config, small_test_data, cleanup_redis):
        """Test append mode functionality"""
        keys_to_clean, patterns_to_clean = cleanup_redis
        patterns_to_clean.append('test_append:*')

        config = {**redis_config, 'data_structure': 'hash', 'key_pattern': 'test_append:{id}'}
        loader = RedisLoader(config)

        with loader:
            # Initial load
            result = loader.load_table(small_test_data, 'test_append', mode=LoadMode.APPEND)
            assert result.success == True
            assert result.rows_loaded == 5

            # Append more data (with different IDs)
            # Convert to pydict, modify, and create new table
            new_data_dict = small_test_data.to_pydict()
            new_data_dict['id'] = [6, 7, 8, 9, 10]
            new_table = pa.Table.from_pydict(new_data_dict)

            result = loader.load_table(new_table, 'test_append', mode=LoadMode.APPEND)
            assert result.success == True
            assert result.rows_loaded == 5

            # Verify all keys exist
            for i in range(1, 11):
                key = f'test_append:{i}'
                assert loader.redis_client.exists(key)

    def test_overwrite_mode(self, redis_config, small_test_data, cleanup_redis):
        """Test overwrite mode functionality"""
        keys_to_clean, patterns_to_clean = cleanup_redis
        patterns_to_clean.append('test_overwrite:*')

        config = {**redis_config, 'data_structure': 'hash', 'key_pattern': 'test_overwrite:{id}'}
        loader = RedisLoader(config)

        with loader:
            # Initial load
            result = loader.load_table(small_test_data, 'test_overwrite', mode=LoadMode.OVERWRITE)
            assert result.success == True
            assert result.rows_loaded == 5

            # Overwrite with less data
            new_data = small_test_data.slice(0, 3)
            result = loader.load_table(new_data, 'test_overwrite', mode=LoadMode.OVERWRITE)
            assert result.success == True
            assert result.rows_loaded == 3

            # Verify old keys were deleted
            assert not loader.redis_client.exists('test_overwrite:4')
            assert not loader.redis_client.exists('test_overwrite:5')

    def test_batch_loading(self, redis_config, comprehensive_test_data, cleanup_redis):
        """Test batch loading functionality"""
        keys_to_clean, patterns_to_clean = cleanup_redis
        patterns_to_clean.append('test_batch:*')

        config = {**redis_config, 'data_structure': 'hash', 'key_pattern': 'test_batch:{id}', 'batch_size': 250}
        loader = RedisLoader(config)

        with loader:
            # Test loading individual batches
            batches = comprehensive_test_data.to_batches(max_chunksize=250)
            total_rows = 0

            for i, batch in enumerate(batches):
                mode = LoadMode.OVERWRITE if i == 0 else LoadMode.APPEND
                result = loader.load_batch(batch, 'test_batch', mode=mode)

                assert result.success == True
                assert result.rows_loaded == batch.num_rows
                total_rows += batch.num_rows

            assert total_rows == 1000

    def test_ttl_functionality(self, redis_config, small_test_data, cleanup_redis):
        """Test TTL (time-to-live) functionality"""
        keys_to_clean, patterns_to_clean = cleanup_redis
        patterns_to_clean.append('test_ttl:*')

        config = {
            **redis_config,
            'data_structure': 'hash',
            'key_pattern': 'test_ttl:{id}',
            'ttl': 2,  # 2 seconds TTL
        }
        loader = RedisLoader(config)

        with loader:
            result = loader.load_table(small_test_data, 'test_ttl')
            assert result.success == True

            # Check keys exist
            key = 'test_ttl:1'
            assert loader.redis_client.exists(key)

            # Check TTL is set
            ttl = loader.redis_client.ttl(key)
            assert ttl > 0 and ttl <= 2

            # Wait for expiration
            time.sleep(3)
            assert not loader.redis_client.exists(key)

    def test_null_value_handling(self, redis_config, null_test_data, cleanup_redis):
        """Test comprehensive null value handling across all data types"""
        keys_to_clean, patterns_to_clean = cleanup_redis
        patterns_to_clean.append('test_nulls:*')

        config = {**redis_config, 'data_structure': 'hash', 'key_pattern': 'test_nulls:{id}'}
        loader = RedisLoader(config)

        with loader:
            result = loader.load_table(null_test_data, 'test_nulls')
            assert result.success == True
            assert result.rows_loaded == 10

            # Verify null values were handled correctly (skipped in hash fields)
            for i in range(1, 11):
                key = f'test_nulls:{i}'
                assert loader.redis_client.exists(key)

                # Check that null fields are not stored (Redis hash skips null values)
                fields = loader.redis_client.hkeys(key)
                field_names = {f.decode() if isinstance(f, bytes) else f for f in fields}

                # Based on our null test data pattern, verify expected fields
                # text_field pattern: ['a', 'b', None, 'd', 'e', None, 'g', 'h', None, 'j'] 
                # So ids 3, 6, 9 should have None (indices 2, 5, 8)
                if i in [3, 6, 9]:  # text_field is None for these IDs
                    assert 'text_field' not in field_names
                else:
                    assert 'text_field' in field_names, f"Expected text_field for id={i}, but got fields: {field_names}"

                # int_field pattern: [1, None, 3, 4, None, 6, 7, None, 9, 10]
                # So ids 2, 5, 8 should have None (indices 1, 4, 7)
                if i in [2, 5, 8]:  # int_field is None for these IDs
                    assert 'int_field' not in field_names
                else:
                    assert 'int_field' in field_names

                # float_field pattern: [1.1, 2.2, None, 4.4, 5.5, None, 7.7, 8.8, None, 10.0]
                # So ids 3, 6, 9 should have None (indices 2, 5, 8)
                if i in [3, 6, 9]:  # float_field is None for these IDs
                    assert 'float_field' not in field_names
                else:
                    assert 'float_field' in field_names

                # Verify non-null values are intact  
                if 'text_field' in field_names:
                    text_val = loader.redis_client.hget(key, 'text_field')
                    expected_chars = ['a', 'b', None, 'd', 'e', None, 'g', 'h', None, 'j']
                    expected_char = expected_chars[i - 1]  # Convert id to index
                    assert text_val.decode() == expected_char

                if 'int_field' in field_names:
                    int_val = loader.redis_client.hget(key, 'int_field')
                    expected_ints = [1, None, 3, 4, None, 6, 7, None, 9, 10]
                    expected_int = expected_ints[i - 1]  # Convert id to index
                    assert int(int_val.decode()) == expected_int

    def test_null_value_handling_string_structure(self, redis_config, null_test_data, cleanup_redis):
        """Test null value handling with string (JSON) data structure"""
        keys_to_clean, patterns_to_clean = cleanup_redis
        patterns_to_clean.append('test_json_nulls:*')

        config = {**redis_config, 'data_structure': 'string', 'key_pattern': 'test_json_nulls:{id}'}
        loader = RedisLoader(config)

        with loader:
            result = loader.load_table(null_test_data, 'test_json_nulls')
            assert result.success == True
            assert result.rows_loaded == 10

            for i in range(1, 11):
                key = f'test_json_nulls:{i}'
                assert loader.redis_client.exists(key)

                json_str = loader.redis_client.get(key)
                json_data = json.loads(json_str)

                if i in [3, 6, 9]:
                    assert 'text_field' not in json_data
                else:
                    expected_chars = ['a', 'b', None, 'd', 'e', None, 'g', 'h', None, 'j']
                    expected_char = expected_chars[i - 1]
                    assert json_data['text_field'] == expected_char

                if i in [2, 5, 8]:
                    assert 'int_field' not in json_data
                else:
                    expected_ints = [1, None, 3, 4, None, 6, 7, None, 9, 10]
                    expected_int = expected_ints[i - 1]
                    assert json_data['int_field'] == expected_int

    def test_binary_data_handling(self, redis_config, cleanup_redis):
        """Test binary data handling"""
        keys_to_clean, patterns_to_clean = cleanup_redis
        patterns_to_clean.append('test_binary:*')

        # Create data with binary columns
        data = {'id': [1, 2, 3], 'binary_data': [b'hello', b'world', b'\x00\x01\x02\x03'], 'text_data': ['a', 'b', 'c']}
        table = pa.Table.from_pydict(data)

        config = {**redis_config, 'data_structure': 'hash', 'key_pattern': 'test_binary:{id}'}
        loader = RedisLoader(config)

        with loader:
            result = loader.load_table(table, 'test_binary')
            assert result.success == True
            assert result.rows_loaded == 3

            # Verify binary data was stored correctly
            assert loader.redis_client.hget('test_binary:1', 'binary_data') == b'hello'
            assert loader.redis_client.hget('test_binary:2', 'binary_data') == b'world'
            assert loader.redis_client.hget('test_binary:3', 'binary_data') == b'\x00\x01\x02\x03'

    def test_comprehensive_stats(self, redis_config, small_test_data, cleanup_redis):
        """Test comprehensive statistics functionality"""
        keys_to_clean, patterns_to_clean = cleanup_redis
        patterns_to_clean.append('test_stats:*')

        config = {**redis_config, 'data_structure': 'hash', 'key_pattern': 'test_stats:{id}'}
        loader = RedisLoader(config)

        with loader:
            result = loader.load_table(small_test_data, 'test_stats')
            assert result.success == True

            # Get stats
            stats = loader.get_comprehensive_stats('test_stats')
            assert 'table_name' in stats
            assert stats['data_structure'] == 'hash'
            assert stats['key_count'] == 5
            assert 'estimated_memory_bytes' in stats
            assert 'estimated_memory_mb' in stats

    def test_error_handling(self, redis_config, small_test_data):
        """Test error handling scenarios"""
        # Test with invalid configuration
        invalid_config = {**redis_config, 'host': 'invalid-host-that-does-not-exist', 'socket_connect_timeout': 1}
        loader = RedisLoader(invalid_config)

        with pytest.raises(Exception):
            loader.connect()

    def test_key_pattern_generation(self, redis_config, cleanup_redis):
        """Test various key pattern generations"""
        keys_to_clean, patterns_to_clean = cleanup_redis
        patterns_to_clean.append('complex:*')

        # Create data with multiple fields for complex key
        data = {'user_id': ['u1', 'u2', 'u3'], 'session_id': ['s1', 's2', 's3'], 'timestamp': [100, 200, 300]}
        table = pa.Table.from_pydict(data)

        config = {**redis_config, 'data_structure': 'hash', 'key_pattern': 'complex:{user_id}:{session_id}:{timestamp}'}
        loader = RedisLoader(config)

        with loader:
            result = loader.load_table(table, 'complex')
            assert result.success == True

            # Verify complex keys were created
            assert loader.redis_client.exists('complex:u1:s1:100')
            assert loader.redis_client.exists('complex:u2:s2:200')
            assert loader.redis_client.exists('complex:u3:s3:300')

    def test_performance_metrics(self, redis_config, comprehensive_test_data, cleanup_redis):
        """Test performance metrics in results"""
        keys_to_clean, patterns_to_clean = cleanup_redis
        patterns_to_clean.append('test_perf:*')

        config = {**redis_config, 'data_structure': 'hash', 'key_pattern': 'test_perf:{id}', 'batch_size': 100, 'pipeline_size': 500}
        loader = RedisLoader(config)

        with loader:
            start_time = time.time()
            result = loader.load_table(comprehensive_test_data, 'test_perf')
            end_time = time.time()

            assert result.success == True
            assert result.duration > 0
            assert result.duration <= (end_time - start_time)

            # Check metadata contains performance info
            assert 'ops_per_second' in result.metadata
            assert result.metadata['ops_per_second'] > 0
            assert 'batches_processed' in result.metadata
            assert 'avg_batch_size' in result.metadata


@pytest.mark.integration
@pytest.mark.redis
@pytest.mark.slow
class TestRedisLoaderPerformance:
    """Performance tests for Redis loader"""

    def test_large_data_loading(self, redis_config, cleanup_redis):
        """Test loading large datasets"""
        keys_to_clean, patterns_to_clean = cleanup_redis
        patterns_to_clean.append('test_large:*')

        # Create large dataset
        large_data = {'id': list(range(10000)), 'value': [i * 0.123 for i in range(10000)], 'category': [f'category_{i % 100}' for i in range(10000)], 'description': [f'This is a longer text description for row {i}' for i in range(10000)], 'active': [i % 2 == 0 for i in range(10000)]}
        large_table = pa.Table.from_pydict(large_data)

        config = {**redis_config, 'data_structure': 'hash', 'key_pattern': 'test_large:{id}', 'batch_size': 1000, 'pipeline_size': 1000}
        loader = RedisLoader(config)

        with loader:
            result = loader.load_table(large_table, 'test_large')

            assert result.success == True
            assert result.rows_loaded == 10000
            assert result.duration < 30  # Should complete within 30 seconds

            # Verify performance metrics
            assert result.metadata['ops_per_second'] > 100  # Should handle >100 ops/sec

    def test_data_structure_performance_comparison(self, redis_config, cleanup_redis):
        """Compare performance across different data structures"""
        keys_to_clean, patterns_to_clean = cleanup_redis

        # Create test data
        data = {'id': list(range(1000)), 'score': list(range(1000)), 'data': [f'value_{i}' for i in range(1000)]}
        table = pa.Table.from_pydict(data)

        structures = ['hash', 'string', 'set', 'sorted_set', 'list']
        results = {}

        for structure in structures:
            patterns_to_clean.append(f'perf_{structure}:*')
            keys_to_clean.append(f'perf_{structure}:{structure}')

            config = {**redis_config, 'data_structure': structure, 'key_pattern': f'perf_{structure}:{{id}}', 'score_field': 'score' if structure == 'sorted_set' else None}
            loader = RedisLoader(config)

            with loader:
                result = loader.load_table(table, f'perf_{structure}')
                results[structure] = result.metadata.get('ops_per_second', 0)

        # All structures should perform reasonably well
        for structure, ops_per_sec in results.items():
            assert ops_per_sec > 50, f'{structure} performance too low: {ops_per_sec} ops/sec'
