from dataclasses import dataclass, field
from typing import List, Set
import pyarrow as pa
from pathlib import Path

@dataclass
class Table:
    name: str
    schema: pa.Schema
    files: List[Path] = field(default_factory=list)
    dependencies: Set[str] = field(default_factory=set)
    associated_tables: Set[str] = field(default_factory=set)
    dependent_views: Set[str] = field(default_factory=set)
    start_block: int = None
    end_block: int = None
    last_updated: str = None  # ISO format timestamp