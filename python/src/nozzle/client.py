from google.protobuf.any_pb2 import Any
from pyarrow import flight
import pyarrow as pa
from . import FlightSql_pb2
from .loaders.registry import create_loader, get_available_loaders
from .loaders.base import LoadResult, LoadConfig, LoadMode
from .config.connection_manager import ConnectionManager
import logging
from typing import Dict, Optional, Union, Iterator, List, Any as Anytype


class QueryBuilder:
    """Chainable query builder for data loading operations"""

    def __init__(self, client: 'Client', query: str):
        self.client = client
        self.query = query
        self._result_cache = None
        self.logger = logging.getLogger(__name__)

    def load(self, connection: str, destination: str, config: Dict[str, Any] = None, **kwargs) -> Union[LoadResult, Iterator[LoadResult]]:
        """
        Load query results to specified destination

        Args:
            loader: Name of the loader (e.g., 'postgresql', 'redis')
            destination: Target destination (table name, key, path, etc.)
            connection: Named connection or connection name for auto-discovery
            config: Inline configuration dict (alternative to connection)
            **kwargs: Additional loader-specific options including:
                - read_all: bool = False (if True, loads entire table at once; if False, streams batch by batch)
                - batch_size: int = 10000 (size of each batch for streaming)

        Returns:
            - If read_all=True: Single LoadResult with operation details
            - If read_all=False (default): Iterator of LoadResults, one per batch
        """
        # Default to streaming (read_all=False) for memory efficiency
        kwargs.setdefault('read_all', False)

        return self.client.query_and_load(query=self.query, destination=destination, connection_name=connection, config=config, **kwargs)

    def stream(self) -> Iterator[pa.RecordBatch]:
        """Stream query results as Arrow batches"""
        self.logger.debug(f'Starting stream for query: {self.query[:50]}...')
        return self.client.get_sql(self.query, read_all=False)

    def to_arrow(self) -> pa.Table:
        """Get query results as Arrow table"""
        if self._result_cache is None:
            self.logger.debug(f'Executing query for Arrow table: {self.query[:50]}...')
            self._result_cache = self.client.get_sql(self.query, read_all=True)
        return self._result_cache

    def get_sql(self, read_all: bool = False):
        """Backward compatibility with existing method"""
        return self.client.get_sql(self.query, read_all=read_all)

    def __repr__(self):
        return f"QueryBuilder(query='{self.query[:50]}{'...' if len(self.query) > 50 else ''}')"


class Client:
    """Enhanced Flight SQL client with data loading capabilities"""

    def __init__(self, url):
        self.conn = flight.connect(url)
        self.connection_manager = ConnectionManager()
        self.logger = logging.getLogger(__name__)

    def sql(self, query: str) -> QueryBuilder:
        """
        Create a chainable query builder

        Args:
            query: SQL query string

        Returns:
            QueryBuilder instance for chaining operations
        """
        return QueryBuilder(self, query)

    def configure_connection(self, name: str, loader: str, config: Dict[str, Any]) -> None:
        """Configure a named connection for reuse"""
        self.connection_manager.add_connection(name, loader, config)

    def list_connections(self) -> Dict[str, str]:
        """List all configured connections"""
        return self.connection_manager.list_connections()

    def get_available_loaders(self) -> List[str]:
        """Get list of available data loaders"""
        return get_available_loaders()

    # Existing methods for backward compatibility
    def get_sql(self, query, read_all=False):
        """Execute SQL query and return Arrow data"""
        # Create a CommandStatementQuery message
        command_query = FlightSql_pb2.CommandStatementQuery()
        command_query.query = query

        # Wrap the CommandStatementQuery in an Any type
        any_command = Any()
        any_command.Pack(command_query)
        cmd = any_command.SerializeToString()

        flight_descriptor = flight.FlightDescriptor.for_command(cmd)
        info = self.conn.get_flight_info(flight_descriptor)
        reader = self.conn.do_get(info.endpoints[0].ticket)

        if read_all:
            return reader.read_all()
        else:
            return self._batch_generator(reader)

    def _batch_generator(self, reader):
        """Generate batches from Flight reader"""
        while True:
            try:
                chunk = reader.read_chunk()
                yield chunk.data
            except StopIteration:
                break

    def query_and_load(self, query: str, destination: str, connection_name: str, config: Optional[Dict[str, Any]] = None, **kwargs) -> Union[LoadResult, Iterator[LoadResult]]:
        """
        Execute query and load results directly into target system

        Args:
            query: SQL query to execute
            destination: Target destination name (table name, key, path, etc.)
            connection_name: Named connection (which specifies both loader type and config)
            config: Inline configuration dict (alternative to named connection)
            **kwargs: Additional load options including:
                - read_all: bool = False (default streams batch by batch for memory efficiency)
                - batch_size: int = 10000 (size of each batch for streaming)
                - mode: str = 'append' (loading mode)
                - create_table: bool = True (whether to create target table)

        Returns:
            - If read_all=False (default): Iterator of LoadResults, one per batch (memory efficient)
            - If read_all=True: Single LoadResult with operation details (loads entire table)
        """
        # Get connection configuration and determine loader type
        if connection_name:
            try:
                connection_info = self.connection_manager.get_connection_info(connection_name)
                loader_config = connection_info['config']
                loader_type = connection_info['loader']
            except ValueError as e:
                self.logger.error(f'Connection error: {e}')
                raise
        elif config:
            # For inline config, we need to determine the loader type
            # This is a fallback for advanced usage
            loader_type = config.pop('loader_type', None)
            if not loader_type:
                raise ValueError("When using inline config, 'loader_type' must be specified")
            loader_config = config
        else:
            raise ValueError('Either connection_name or config must be provided')

        # Extract load options from kwargs - streaming is now the default
        read_all = kwargs.pop('read_all', False)  # Default to streaming
        load_config = LoadConfig(batch_size=kwargs.pop('batch_size', 10000), mode=LoadMode(kwargs.pop('mode', 'append')), create_table=kwargs.pop('create_table', True), schema_evolution=kwargs.pop('schema_evolution', False), **{k: v for k, v in kwargs.items() if k in ['max_retries', 'retry_delay']})

        if read_all:
            self.logger.info(f'Loading entire query result to {loader_type}:{destination}')
        else:
            self.logger.info(f'Streaming query results to {loader_type}:{destination} (batch_size={load_config.batch_size})')

        # Get the data and load
        if read_all:
            table = self.get_sql(query, read_all=True)
            return self._load_table(table, loader_type, destination, loader_config, load_config)
        else:
            batch_stream = self.get_sql(query, read_all=False)
            return self._load_stream(batch_stream, loader_type, destination, loader_config, load_config)

    def _load_table(self, table: pa.Table, loader: str, table_name: str, config: Dict[str, Any], load_config: LoadConfig) -> LoadResult:
        """Load a complete Arrow Table"""
        try:
            loader_instance = create_loader(loader, config)

            with loader_instance:
                return loader_instance.load_table(table, table_name, **load_config.__dict__)
        except Exception as e:
            self.logger.error(f'Failed to load table: {e}')
            return LoadResult(rows_loaded=0, duration=0.0, table_name=table_name, loader_type=loader, success=False, error=str(e))

    def _load_stream(self, batch_stream: Iterator[pa.RecordBatch], loader: str, table_name: str, config: Dict[str, Any], load_config: LoadConfig) -> Iterator[LoadResult]:
        """Load from a stream of batches"""
        try:
            loader_instance = create_loader(loader, config)

            with loader_instance:
                yield from loader_instance.load_stream(batch_stream, table_name, **load_config.__dict__)
        except Exception as e:
            self.logger.error(f'Failed to load stream: {e}')
            yield LoadResult(rows_loaded=0, duration=0.0, table_name=table_name, loader_type=loader, success=False, error=str(e))
