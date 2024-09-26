from dataclasses import dataclass
from typing import List
from pathlib import Path
import pyarrow as pa

@dataclass
class RegisteredTable():
    name: str
    schema: pa.Schema
    description: str
    parquet_files: List[Path]
    min_block: int
    max_block: int