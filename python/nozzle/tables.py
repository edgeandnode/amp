from typing import Dict, Union, Callable, List, Tuple
import pyarrow as pa
import datafusion
from datafusion import DataFrame, SessionContext
from pathlib import Path
import pathlib
from .table_registry_dynamic import DynamicTableRegistry, reload_table_registry, SessionManager

class Tables:
    _instance = None
    # Initialize DataFusion session context from table_registry.py if function exists, otherwise create an empty session context
    _ctx: datafusion.SessionContext = SessionManager.initialize_session()
    _registry_file: Path = Path("table_registry.json")
    _registry_py_file: Path = Path(__file__).parent / "table_registry.py"
    _field_descriptions: Dict[str, Dict[str, str]] = {}
    _table_descriptions: Dict[str, str] = {}
    _parquet_files: Dict[str, List[Path]] = {}
    _block_ranges: Dict[str, Tuple[int, int]] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Tables, cls).__new__(cls)
            # cls._instance._load_registry()
        return cls._instance

    def register_table(self, table_name: str, parquet_file: pathlib.Path, schema: pa.Schema, table_description: str = '', field_descriptions: Dict[str, str] = {}, min_block: int = None, max_block: int = None):
        if table_name not in self._ctx.tables():
            self._ctx.register_parquet(table_name, str(parquet_file), schema=schema)
        
        self._field_descriptions[table_name] = field_descriptions
        self._table_descriptions[table_name] = table_description
        
        if table_name not in self._parquet_files:
            self._parquet_files[table_name] = []
        if parquet_file not in self._parquet_files[table_name]:
            self._parquet_files[table_name].append(parquet_file)
        
        try:
            existing_table = DynamicTableRegistry.get_table_by_name(table_name)
        except ValueError:
            existing_table = None
        if existing_table:
            min_block = min(existing_table.min_block, min_block) if min_block is not None else existing_table.min_block
            max_block = max(existing_table.max_block, max_block) if max_block is not None else existing_table.max_block
        
        # Use DynamicTableRegistry to register or update the table
        DynamicTableRegistry.register_table(
            table_name=table_name,
            schema=schema,
            description=table_description,
            parquet_files=self._parquet_files[table_name],
            min_block=min_block,
            max_block=max_block
        )
        reload_table_registry()
    
    def _update_view_metadata(self, view_name: str, query: Union[str, Callable]):
        self._ctx.sql(f"CREATE OR REPLACE VIEW {view_name} AS {query if isinstance(query, str) else 'SELECT 1'}")
        reload_table_registry()

    def execute_query(self, query: Union[str, Callable[[SessionContext], DataFrame]], view_name: str) -> pa.Table:
        if isinstance(query, str):
            df = self._ctx.sql(query)
        elif callable(query):
            df = query(self._ctx)
        else:
            raise ValueError("Query must be either a SQL string or a callable that operates on a SessionContext")
        batches = df.collect_partitioned()
        result = self._ctx.create_dataframe(batches)
        result = result.to_arrow_table()
        return result
