from typing import Dict, Union, Callable, List, Set, Optional
import pyarrow as pa
import datafusion
from datafusion import DataFrame, SessionContext
from pathlib import Path
import json
from datetime import datetime
from dataclasses import dataclass, field
import pathlib
from .table_registry import table_registry

class Tables:
    _instance = None
    # Initialize DataFusion session context from table_registry.py if function exists, otherwise create an empty session context
    _ctx: datafusion.SessionContext = table_registry.initialize_session() if hasattr(table_registry, 'initialize_session') else SessionContext()
    _registry_file: Path = Path("table_registry.json")
    _registry_py_file: Path = Path(__file__).parent / "table_registry.py"
    _field_descriptions: Dict[str, Dict[str, str]] = {}
    _table_descriptions: Dict[str, str] = {}
    _parquet_files: Dict[str, List[Path]] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Tables, cls).__new__(cls)
            # cls._instance._load_registry()
        return cls._instance

    def register_table(self,  table_name: str, parquet_file: pathlib.Path, schema: pa.Schema, table_description: str = '', field_descriptions: Dict[str, str] = {}):
        if table_name not in self._ctx.tables():
            self._ctx.register_parquet(table_name, parquet_file, schema=schema, skip_metadata=False)
        # self._save_registry()
        self._field_descriptions[table_name] = field_descriptions
        self._table_descriptions[table_name] = table_description
        if table_name not in self._parquet_files:
            self._parquet_files[table_name] = []
        self._parquet_files[table_name].append(parquet_file)
        self._generate_registry_py()
    
    def _update_view_metadata(self, view_name: str, query: Union[str, Callable]):
        self._ctx.sql(f"CREATE OR REPLACE VIEW {view_name} AS {query if isinstance(query, str) else 'SELECT 1'}")
        # self._save_registry()
        self._generate_registry_py()

    def execute_query(self, query: Union[str, Callable[[SessionContext], DataFrame]], view_name: str) -> pa.Table:
        if isinstance(query, str):
            result = self._ctx.sql(query)
        elif callable(query):
            df = query(self._ctx)
            schema = df.schema()
            print('DF SCHEMA ', schema)
            batches = df.collect_partitioned()
            result = self._ctx.create_dataframe(batches)
            result = result.to_arrow_table()
        else:
            raise ValueError("Query must be either a SQL string or a callable that operates on a SessionContext")
        
        return result

    def _save_registry(self):
        for table_name in self._ctx.tables():
            schema = self._ctx.table(table_name).schema()
            # print datafusion catalog including all tables and schemas
            #print(self._ctx.tables())
            #print(self._ctx.catalog().names())
            #print(self._ctx.catalog().database('public'))
            #print(self._ctx.catalog().database('public').table('preprocessed_event_approval_20700000_20800000'))
            #print(self._ctx.catalog().database('public').table('preprocessed_event_approval_20700000_20800000').schema)

    def _load_registry(self):
        if self._registry_file.exists():
            with open(self._registry_file, 'r') as f:
                catalog_data = json.load(f)
            for table_name, table_info in catalog_data["tables"].items():
                schema = pa.Schema.from_json(table_info["schema"])
                self.register_table(table_name, schema, table_info["description"])
        self._generate_registry_py()

    def _generate_registry_py(self):
        registry_content = [
            "from typing import Dict",
            "from .table import Table",
            "from dataclasses import dataclass",
            "import pyarrow as pa",
            "from datafusion import SessionContext",
            "from pathlib import Path",
            "",
            "class RegisteredTable(Table):",
            "    pass",
            "",
            "@dataclass",
            "class TableRegistry:",
            ""
        ]

        # Preserve existing tables from the current table_registry
        existing_tables = {name: getattr(table_registry, name) for name in dir(table_registry) 
                           if isinstance(getattr(table_registry, name), property)}

        for table_name in self._ctx.tables():
            # skip tables that have no parquet files/local data
            if self._parquet_files.get(table_name, []) == []:
                continue
            parquet_files_str = ', '.join([f"Path('{str(file)}')" for file in self._parquet_files.get(table_name, [])])
            if table_name in existing_tables:
                table_info = existing_tables[table_name]
                table_info['parquet_files_str'] = parquet_files_str
            else:
                schema = self._ctx.table(table_name).schema()
                field_descriptions = self._field_descriptions.get(table_name, {})
                table_description = self._table_descriptions.get(table_name, '')
                parquet_files_str = ', '.join([f"Path('{str(file)}')" for file in self._parquet_files.get(table_name, [])])
                print('field descriptions ', field_descriptions)
                table_info = {
                    'name': table_name,
                    'schema': schema,
                    'description': table_description,
                    'parquet_files_str': parquet_files_str
                }
            
            registry_content.extend([
                f"    @property",
                f"    def {table_name}(self) -> Dict[str, any]:",
                f"        return {{",
                f"            'name': '{table_info['name']}',",
                f"            'schema': pa.schema([",
            ])
            for field in table_info['schema']:
                field_type = self._get_field_type_string(field.type)
                description = field_descriptions.get(field.name, '')
                registry_content.append(f"                pa.field('{field.name}', {field_type}, metadata={{'description': '{description}'}}),")
            registry_content.extend([
                "            ]),",
                f"            'description': '{table_info['description']}',",
                f"            'parquet_files': [{table_info['parquet_files_str']}],",
                f"        }}",
                "",
            ])

        registry_content.extend([
            "",
            "table_registry = TableRegistry()",
            "",
             "def initialize_session() -> SessionContext:",
            "    ctx = SessionContext()",
            "    for table_info in [getattr(table_registry, attr) for attr in dir(table_registry) if isinstance(getattr(table_registry, attr), property)]:",
            "        for file in table_info['parquet_files']:",
            "            ctx.register_parquet(table_info['name'], str(file), schema=table_info['schema'])",
            "    return ctx",
        ])

        with open(self._registry_py_file, 'w') as f:
            f.write('\n'.join(registry_content))

    def _get_field_type_string(self, pa_type):
        if pa.types.is_timestamp(pa_type):
            time_unit = pa_type.unit
            tz = pa_type.tz
            if tz:
                return f"pa.timestamp('{time_unit}', tz='{tz}')"
            else:
                return f"pa.timestamp('{time_unit}')"
        elif pa.types.is_decimal(pa_type):
            return f"pa.decimal128({pa_type.precision}, {pa_type.scale})"
        elif pa.types.is_float64(pa_type):
            return "pa.float64()"
        elif pa.types.is_float32(pa_type):
            return "pa.float32()"
        else:
            return f"pa.{str(pa_type)}()"

