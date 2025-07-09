# tests/unit/test_redis_loader.py
"""
Unit tests for Redis loader implementation.
"""

import pytest
from unittest.mock import Mock, patch
import redis

try:
    from src.nozzle.loaders.implementations.redis_loader import RedisLoader, RedisDataStructure, RedisConnectionConfig
    from src.nozzle.loaders.base import LoadResult, LoadMode
except ImportError:
    pytest.skip("nozzle modules not available", allow_module_level=True)

@pytest.mark.unit
class TestRedisLoader:
    """Unit tests for Redis loader"""

    @pytest.fixture
    def mock_config(self):
        """Mock Redis configuration"""
        return {
            'host': 'localhost',
            'port': 6379,
            'db': 0,
            'password': 'testpass',
            'data_structure': 'hash',
            'key_pattern': '{table}:{id}',
            'batch_size': 1000,
            'ttl': 3600,
            'pipeline_size': 500
        }

    @pytest.fixture
    def mock_redis_client(self):
        """Mock Redis client"""
        client = Mock()
        client.ping.return_value = True  # Redis ping returns True in redis-py
        client.info.return_value = {
            'redis_version': '7.0.0',
            'used_memory_human': '1.2M',
            'connected_clients': 1,
            'total_commands_processed': 100,
            'keyspace_hits': 50,
            'keyspace_misses': 10,
            'uptime_in_seconds': 3600
        }
        client.config_get.return_value = {
            'maxmemory': '1GB',
            'maxmemory-policy': 'allkeys-lru'
        }

        # Mock pipeline
        pipeline = Mock()
        pipeline.command_stack = []
        pipeline.execute.return_value = []
        client.pipeline.return_value = pipeline

        return client

    @pytest.fixture
    def test_batch(self, small_test_table):
        """Create a test RecordBatch"""
        return small_test_table.to_batches()[0]

    def test_init(self, mock_config):
        """Test loader initialization"""
        loader = RedisLoader(mock_config)

        assert loader.config == mock_config
        assert loader.redis_client is None
        assert loader.connection_pool is None
        assert loader.data_structure == RedisDataStructure.HASH
        assert loader.key_pattern == '{table}:{id}'
        assert loader.batch_size == 1000
        assert loader.ttl == 3600
        assert loader.pipeline_size == 500
        assert not loader._is_connected

    def test_init_with_defaults(self):
        """Test loader initialization with default values"""
        config = {'host': 'localhost'}
        loader = RedisLoader(config)

        assert loader.data_structure == RedisDataStructure.HASH
        assert loader.key_pattern == '{table}:{id}'
        assert loader.batch_size == 1000
        assert loader.ttl is None
        assert loader.pipeline_size == 1000

    @patch('src.nozzle.loaders.implementations.redis_loader.redis.ConnectionPool')
    @patch('src.nozzle.loaders.implementations.redis_loader.redis.Redis')
    def test_connect_success(self, mock_redis_class, mock_pool_class, mock_config, mock_redis_client):
        """Test successful connection"""
        mock_pool = Mock()
        mock_pool_class.return_value = mock_pool
        mock_redis_class.return_value = mock_redis_client

        loader = RedisLoader(mock_config)
        loader.connect()

        assert loader._is_connected
        assert loader.redis_client == mock_redis_client
        assert loader.connection_pool == mock_pool
        # The connect method calls info() to test connection and get server info
        mock_redis_client.info.assert_called_once()
        # ping() is not called in connect() - only in health_check()
        mock_redis_client.ping.assert_not_called()

    @patch('src.nozzle.loaders.implementations.redis_loader.redis.ConnectionPool')
    def test_connect_failure(self, mock_pool_class, mock_config):
        """Test connection failure"""
        mock_pool_class.side_effect = redis.ConnectionError("Connection failed")

        loader = RedisLoader(mock_config)

        with pytest.raises(redis.ConnectionError):
            loader.connect()

        assert not loader._is_connected
        assert loader.redis_client is None

    def test_disconnect(self, mock_config):
        """Test disconnection"""
        loader = RedisLoader(mock_config)
        mock_client = Mock()
        mock_pool = Mock()
        loader.redis_client = mock_client
        loader.connection_pool = mock_pool
        loader._is_connected = True

        loader.disconnect()

        mock_pool.disconnect.assert_called_once()
        mock_client.close.assert_called_once()
        assert loader.redis_client is None
        assert loader.connection_pool is None
        assert not loader._is_connected

    def test_generate_redis_key(self, mock_config):
        """Test Redis key generation"""
        loader = RedisLoader(mock_config)

        # Test with simple data that matches the actual implementation
        data_dict = {'id': [123], 'user_id': ['user_456']}
        key = loader._generate_key_optimized(data_dict, 0, 'users')
        assert key == 'users:123'

        # Test with user_id in pattern
        loader.key_pattern = '{table}:{user_id}'
        key = loader._generate_key_optimized(data_dict, 0, 'users')
        assert key == 'users:user_456'

    def test_generate_redis_key_fallback(self, mock_config):
        """Test Redis key generation with fallback behavior"""
        loader = RedisLoader(mock_config)

        # Test when field doesn't exist
        data_dict = {'id': [123], 'user_id': ['user_456']}
        loader.key_pattern = '{table}:{nonexistent_field}'
        key = loader._generate_key_optimized(data_dict, 0, 'users')
        assert key == 'users:{nonexistent_field}'

    def test_load_as_hashes_optimized(self, mock_config):
        """Test loading data as Redis hashes"""
        loader = RedisLoader(mock_config)
        loader.data_structure = RedisDataStructure.HASH

        mock_client = Mock()
        mock_pipeline = Mock()
        mock_client.pipeline.return_value = mock_pipeline
        loader.redis_client = mock_client

        data_dict = {
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'score': [85.5, 92.0, 78.5]
        }

        rows_loaded = loader._load_as_hashes_optimized(data_dict, 3, 'users')

        assert rows_loaded == 3
        mock_client.pipeline.assert_called_once()
        mock_pipeline.execute.assert_called()

    def test_load_as_strings_optimized(self, mock_config):
        """Test loading data as Redis strings (JSON)"""
        loader = RedisLoader(mock_config)
        loader.data_structure = RedisDataStructure.STRING

        # Mock redis client and pipeline
        mock_client = Mock()
        mock_pipeline = Mock()
        mock_client.pipeline.return_value = mock_pipeline
        loader.redis_client = mock_client

        data_dict = {
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'active': [True, False]
        }

        rows_loaded = loader._load_as_strings_optimized(data_dict, 2, 'users')

        assert rows_loaded == 2
        # Verify pipeline was used
        mock_client.pipeline.assert_called_once()

    def test_load_batch_optimized(self, mock_config, test_batch):
        """Test optimized batch loading"""
        loader = RedisLoader(mock_config)

        mock_client = Mock()
        mock_pipeline = Mock()
        mock_pipeline.command_stack = []
        mock_client.pipeline.return_value = mock_pipeline
        loader.redis_client = mock_client

        # Mock the specific implementation method for HASH
        loader._load_as_hashes_optimized = Mock(return_value=test_batch.num_rows)

        rows_processed = loader._load_batch_optimized(test_batch, 'test_table')

        assert rows_processed == test_batch.num_rows
        # Verify the correct method was called based on data structure (HASH)
        loader._load_as_hashes_optimized.assert_called_once()

        # Verify the method was called with the correct arguments
        call_args = loader._load_as_hashes_optimized.call_args
        data_dict = call_args[0][0]  # First argument should be data_dict
        num_rows = call_args[0][1]    # Second argument should be num_rows
        table_name = call_args[0][2]  # Third argument should be table_name

        assert isinstance(data_dict, dict)
        assert num_rows == test_batch.num_rows
        assert table_name == 'test_table'

    def test_load_batch_success(self, mock_config, test_batch):
        """Test successful batch loading"""
        loader = RedisLoader(mock_config)

        # Mock Redis client - but don't mock pipeline at this level
        mock_client = Mock()
        loader.redis_client = mock_client

        # Mock the batch processing method that actually exists
        loader._load_batch_optimized = Mock(return_value=test_batch.num_rows)

        result = loader.load_batch(test_batch, 'test_table')

        assert result.success
        assert result.rows_loaded == test_batch.num_rows
        assert result.table_name == 'test_table'
        assert result.loader_type == 'redis'
        assert result.duration > 0
        assert result.metadata['data_structure'] == 'hash'

        # Verify the batch processing method was called
        loader._load_batch_optimized.assert_called_once_with(test_batch, 'test_table')

        # The pipeline execution happens inside _load_batch_optimized and its sub-methods,
        # not directly in load_batch(), so we don't verify pipeline.execute() here

    def test_load_batch_overwrite_mode(self, mock_config, test_batch):
        """Test batch loading with overwrite mode"""
        loader = RedisLoader(mock_config)

        # Mock Redis client and pipeline
        mock_client = Mock()
        mock_pipeline = Mock()
        mock_pipeline.command_stack = []
        mock_client.pipeline.return_value = mock_pipeline
        loader.redis_client = mock_client

        # Mock methods that exist in the actual implementation
        loader._clear_data = Mock()
        loader._load_batch_optimized = Mock(return_value=test_batch.num_rows)

        result = loader.load_batch(test_batch, 'test_table', mode=LoadMode.OVERWRITE)

        assert result.success
        loader._clear_data.assert_called_once_with('test_table')

    def test_load_batch_failure(self, mock_config, test_batch):
        """Test batch loading failure"""
        loader = RedisLoader(mock_config)

        # Mock Redis client that raises exception
        mock_client = Mock()
        mock_client.pipeline.side_effect = redis.ConnectionError("Connection failed")
        loader.redis_client = mock_client

        result = loader.load_batch(test_batch, 'test_table')

        assert not result.success
        assert result.rows_loaded == 0
        assert result.error == "Connection failed"

    def test_clear_data(self, mock_config):
        """Test clearing data for overwrite mode"""
        loader = RedisLoader(mock_config)
        loader.data_structure = RedisDataStructure.HASH

        # Mock Redis client
        mock_client = Mock()
        # Mock scan_iter returning keys
        mock_client.scan_iter.return_value = ['users:1', 'users:2', 'users:3']
        loader.redis_client = mock_client

        loader._clear_data('users')

        # For HASH structure, it should use scan_iter to find keys and delete them one by one
        mock_client.scan_iter.assert_called_once()
        # Verify individual deletes were called (not bulk delete)
        assert mock_client.delete.call_count == 3
        mock_client.delete.assert_any_call('users:1')
        mock_client.delete.assert_any_call('users:2')
        mock_client.delete.assert_any_call('users:3')

    def test_health_check(self, mock_config):
        """Test health check functionality"""
        loader = RedisLoader(mock_config)

        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_client.info.return_value = {
            'redis_version': '7.0.0',
            'used_memory_human': '1.5M',
            'connected_clients': 2,
            'total_commands_processed': 1000,
            'keyspace_hits': 800,
            'keyspace_misses': 200,
            'uptime_in_seconds': 7200
        }
        mock_client.config_get.return_value = {
            'maxmemory': '2GB',
            'maxmemory-policy': 'allkeys-lru'
        }
        mock_client.set.return_value = True
        mock_client.get.return_value = b'test_value'
        mock_client.delete.return_value = True

        loader.redis_client = mock_client

        health = loader.health_check()

        assert health['healthy'] is True
        assert health['redis_version'] == '7.0.0'
        assert health['hit_rate'] == 80.0  # 800/(800+200) * 100

    def test_clear_data_collection_structure(self, mock_config):
        """Test clearing data for collection-based structures"""
        loader = RedisLoader(mock_config)
        loader.data_structure = RedisDataStructure.STREAM  # Collection-based structure

        # Mock Redis client
        mock_client = Mock()
        mock_client.exists.return_value = True
        loader.redis_client = mock_client

        loader._clear_data('events')

        # For collection structures, it should delete the main collection key
        collection_key = 'events:stream'
        mock_client.exists.assert_called_once_with(collection_key)
        mock_client.delete.assert_called_once_with(collection_key)

    def test_get_comprehensive_stats(self, mock_config):
        """Test getting comprehensive statistics"""
        loader = RedisLoader(mock_config)
        loader.data_structure = RedisDataStructure.HASH

        mock_client = Mock()
        mock_client.scan_iter.return_value = ['users:1', 'users:2', 'users:3']
        mock_client.hkeys.return_value = [b'id', b'name', b'email']
        mock_client.memory_usage.return_value = 1024
        loader.redis_client = mock_client

        stats = loader.get_comprehensive_stats('users')

        assert stats['table_name'] == 'users'
        assert stats['data_structure'] == 'hash'
        assert stats['key_count'] == 3
        assert 'sample_fields' in stats
        assert 'estimated_memory_mb' in stats
        assert 'connection_info' in stats

    def test_context_manager(self, mock_config):
        """Test context manager functionality"""
        loader = RedisLoader(mock_config)

        # Mock connect and disconnect
        loader.connect = Mock()
        loader.disconnect = Mock()

        with loader:
            pass

        loader.connect.assert_called_once()
        loader.disconnect.assert_called_once()

@pytest.mark.unit
class TestRedisDataStructures:
    """Test different Redis data structures"""

    @pytest.fixture
    def loader_configs(self):
        """Different loader configurations for each data structure"""
        return {
            'hash': {'host': 'localhost', 'data_structure': 'hash'},
            'string': {'host': 'localhost', 'data_structure': 'string'},
            'stream': {'host': 'localhost', 'data_structure': 'stream'},
            'set': {'host': 'localhost', 'data_structure': 'set'},
            'sorted_set': {'host': 'localhost', 'data_structure': 'sorted_set'},
            'list': {'host': 'localhost', 'data_structure': 'list'},
            'json': {'host': 'localhost', 'data_structure': 'json'}
        }

    def test_hash_structure_initialization(self, loader_configs):
        """Test hash data structure initialization"""
        loader = RedisLoader(loader_configs['hash'])
        assert loader.data_structure == RedisDataStructure.HASH

    def test_string_structure_initialization(self, loader_configs):
        """Test string data structure initialization"""
        loader = RedisLoader(loader_configs['string'])
        assert loader.data_structure == RedisDataStructure.STRING

    def test_stream_structure_initialization(self, loader_configs):
        """Test stream data structure initialization"""
        loader = RedisLoader(loader_configs['stream'])
        assert loader.data_structure == RedisDataStructure.STREAM

    def test_set_structure_initialization(self, loader_configs):
        """Test set data structure initialization"""
        loader = RedisLoader(loader_configs['set'])
        assert loader.data_structure == RedisDataStructure.SET

    def test_sorted_set_structure_initialization(self, loader_configs):
        """Test sorted set data structure initialization"""
        loader = RedisLoader(loader_configs['sorted_set'])
        assert loader.data_structure == RedisDataStructure.SORTED_SET

    def test_list_structure_initialization(self, loader_configs):
        """Test list data structure initialization"""
        loader = RedisLoader(loader_configs['list'])
        assert loader.data_structure == RedisDataStructure.LIST

    def test_json_structure_initialization(self, loader_configs):
        """Test JSON data structure initialization"""
        loader = RedisLoader(loader_configs['json'])
        assert loader.data_structure == RedisDataStructure.JSON

    def test_unsupported_structure(self, loader_configs):
        """Test unsupported data structure"""
        config = loader_configs['hash'].copy()
        config['data_structure'] = 'unsupported'

        with pytest.raises(ValueError, match="'unsupported' is not a valid RedisDataStructure"):
            loader = RedisLoader(config)
