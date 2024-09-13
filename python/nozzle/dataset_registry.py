from dataclasses import dataclass, field
from typing import Dict, List
import json
from pathlib import Path

@dataclass
class Table:
    name: str
    columns: List[str]
    start_block: int
    end_block: int

@dataclass
class Dataset:
    name: str
    tables: Dict[str, Table] = field(default_factory=dict)

    def add_table(self, table: Table):
        self.tables[table.name] = table

class DatasetRegistry:
    _instance = None
    _datasets: Dict[str, Dataset] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatasetRegistry, cls).__new__(cls)
        return cls._instance

    def register_dataset(self, dataset: Dataset):
        self._datasets[dataset.name] = dataset

    def get_dataset(self, name: str) -> Dataset:
        return self._datasets.get(name)

    def save_to_disk(self):
        data = {name: {"tables": {t.name: t.__dict__ for t in dataset.tables.values()}} 
                for name, dataset in self._datasets.items()}
        with open('dataset_registry.json', 'w') as f:
            json.dump(data, f)

    @classmethod
    def load_from_disk(cls):
        if Path('dataset_registry.json').exists():
            with open('dataset_registry.json', 'r') as f:
                data = json.load(f)
            registry = cls()
            for dataset_name, dataset_data in data.items():
                dataset = Dataset(dataset_name)
                for table_name, table_data in dataset_data['tables'].items():
                    table = Table(**table_data)
                    dataset.add_table(table)
                registry.register_dataset(dataset)
            return registry
        return cls()