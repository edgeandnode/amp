import pyarrow as pa
from typing import List
from pathlib import Path
from .registry_manager import RegistryManager
from datafusion import SessionContext
from .table_registry import RegisteredTable, TableRegistry
import json

class DynamicTableRegistry:
    REGISTRY_DIR = Path(__file__).parent / "registries"
    REGISTRY_FILE = REGISTRY_DIR / "table_registry.json"

    @classmethod
    def get_registry(cls):
        if not cls.REGISTRY_FILE.exists():
            cls.REGISTRY_DIR.mkdir(parents=True, exist_ok=True)
            cls.save_registry({})
        
        with cls.REGISTRY_FILE.open('r') as f:
            return json.load(f)

    @classmethod
    def save_registry(cls, registry_data):
        cls.REGISTRY_DIR.mkdir(parents=True, exist_ok=True)
        with cls.REGISTRY_FILE.open('w') as f:
            json.dump(registry_data, f, indent=2)

    @classmethod
    def register_table(cls, table_name: str, schema: pa.Schema, description: str, parquet_files: List[Path], min_block: int, max_block: int):
        table_data = {
            table_name: {
                "name": table_name,
                "schema": cls._schema_to_dict(schema),
                "description": description,
                "parquet_files": [str(file) for file in parquet_files],
                "min_block": min_block,
                "max_block": max_block,
                "columns": {field.name: cls._field_to_dict(field) for field in schema}
            }
        }
        RegistryManager.update_registry("table", table_data)
        reload_table_registry()

    @classmethod
    def _field_to_dict(cls, field: pa.Field) -> dict:
        return {
            "name": field.name,
            "type": cls._get_field_type_string(field.type),
            "metadata": field.metadata
        }

    @classmethod
    def _schema_to_dict(cls, schema: pa.Schema) -> List[dict]:
        return [
            {
                "name": field.name,
                "type": cls._get_field_type_string(field.type),
                "metadata": field.metadata
            }
            for field in schema
        ]

    @classmethod
    def _get_field_type_string(cls, pa_type):
        if pa.types.is_timestamp(pa_type):
            time_unit = pa_type.unit
            tz = pa_type.tz
            if tz:
                return f"pa.timestamp('{time_unit}', tz='{tz}')"
            else:
                return f"pa.timestamp('{time_unit}')"
        elif pa.types.is_decimal(pa_type):
            if pa_type.precision > 38 or pa_type.scale > 38:
                return f"pa.decimal256({pa_type.precision}, {pa_type.scale})"
            else:
                return f"pa.decimal128({pa_type.precision}, {pa_type.scale})"
        elif pa.types.is_float64(pa_type):
            return "pa.float64()"
        elif pa.types.is_float32(pa_type):
            return "pa.float32()"
        else:
            return f"pa.{str(pa_type)}()"

    @classmethod
    def get_table_by_name(cls, name: str):
        return RegistryManager.get_registry_item("table", name)
    
class SessionManager:
    @staticmethod
    def initialize_session() -> SessionContext:
        ctx = SessionContext()
        for table_name in TableRegistry.list_tables():
            table = TableRegistry.get_table(table_name)
            for parquet_file in table.parquet_files:
                ctx.register_parquet(table.name, str(parquet_file))
        return ctx

dynamic_table_registry = DynamicTableRegistry()

def reload_table_registry():
    RegistryManager.reload_registry("table")
    SessionManager.initialize_session()
    