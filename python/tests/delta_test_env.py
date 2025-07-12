# tests/delta_test_env.py
"""
Delta Lake test environment setup and management.
"""

import os
import tempfile
import shutil
from pathlib import Path
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class DeltaTestEnvironment:
    """Manages Delta Lake test environment with multiple scenarios"""

    def __init__(self, base_path: Optional[str] = None):
        if base_path:
            self.base_path = Path(base_path)
        else:
            # Create temporary directory for testing
            self.base_path = Path(tempfile.mkdtemp(prefix='delta_test_'))

        self.base_path.mkdir(parents=True, exist_ok=True)
        self.temp_tables = []
        self.scenarios = {}
        self._setup_scenarios()

    def _setup_scenarios(self):
        """Setup different test scenarios"""

        # Basic scenario - simple table for basic tests
        self.scenarios['basic'] = {'table_path': str(self.base_path / 'basic_table'), 'partition_by': ['year', 'month'], 'optimize_after_write': True, 'vacuum_after_write': False, 'schema_evolution': True, 'merge_schema': True, 'storage_options': {}}

        # Partitioned scenario - heavy partitioning
        self.scenarios['partitioned'] = {'table_path': str(self.base_path / 'partitioned_table'), 'partition_by': ['year', 'month', 'day'], 'optimize_after_write': True, 'vacuum_after_write': True, 'schema_evolution': True, 'merge_schema': True, 'storage_options': {}}

        # Blockchain scenario - optimized for blockchain data
        self.scenarios['blockchain'] = {
            'table_path': str(self.base_path / 'blockchain_table'),
            'partition_by': ['year', 'month'],
            'optimize_after_write': True,
            'vacuum_after_write': False,
            'schema_evolution': True,
            'merge_schema': True,
            'file_size_hint': 128 * 1024 * 1024,  # 128MB files
            'max_rows_per_file': 100000,
            'storage_options': {},
        }

        # Large data scenario - optimized for large datasets
        self.scenarios['large_data'] = {
            'table_path': str(self.base_path / 'large_data_table'),
            'partition_by': ['year', 'month'],
            'optimize_after_write': True,
            'vacuum_after_write': False,
            'schema_evolution': True,
            'merge_schema': True,
            'file_size_hint': 256 * 1024 * 1024,  # 256MB files
            'max_rows_per_file': 500000,
            'storage_options': {},
        }

        # Schema evolution scenario
        self.scenarios['schema_evolution'] = {
            'table_path': str(self.base_path / 'schema_evolution_table'),
            'partition_by': ['year', 'month'],
            'optimize_after_write': False,  # Don't optimize during schema changes
            'vacuum_after_write': False,
            'schema_evolution': True,
            'merge_schema': True,
            'storage_options': {},
        }

        # Temporary scenario - for one-off tests
        self.scenarios['temp'] = {'table_path': str(self.base_path / 'temp_table'), 'optimize_after_write': False, 'vacuum_after_write': False, 'schema_evolution': True, 'merge_schema': True, 'storage_options': {}}

    def get_scenario_config(self, scenario: str) -> Dict[str, Any]:
        """Get configuration for a specific scenario"""
        if scenario not in self.scenarios:
            raise ValueError(f"Scenario '{scenario}' not found. Available: {list(self.scenarios.keys())}")

        return self.scenarios[scenario].copy()

    def create_temp_table_path(self, prefix: str = 'test_') -> str:
        """Create a temporary table path"""
        temp_dir = tempfile.mkdtemp(prefix=prefix, dir=self.base_path)
        self.temp_tables.append(temp_dir)
        return temp_dir

    def cleanup_temp_tables(self):
        """Clean up temporary tables"""
        for temp_dir in self.temp_tables:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
        self.temp_tables.clear()

    def cleanup(self):
        """Clean up the entire test environment"""
        self.cleanup_temp_tables()
        if self.base_path.exists():
            shutil.rmtree(self.base_path)


# Global test environment instance
_test_env = None


def get_test_env() -> DeltaTestEnvironment:
    """Get or create the global test environment"""
    global _test_env
    if _test_env is None:
        _test_env = DeltaTestEnvironment()
    return _test_env


def setup_test_env(clean: bool = True) -> Dict[str, Any]:
    """Setup the global test environment"""
    env = get_test_env()
    return {'status': 'ready', 'base_path': str(env.base_path)}


def cleanup_test_env():
    """Cleanup the global test environment"""
    global _test_env
    if _test_env is not None:
        _test_env.cleanup()
        _test_env = None
