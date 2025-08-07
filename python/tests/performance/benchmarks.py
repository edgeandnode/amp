# tests/performance/benchmarks.py
"""
Performance baseline management and regression detection.
"""

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict


@dataclass
class PerformanceBenchmark:
    """Performance benchmark data structure"""

    test_name: str
    loader_type: str
    throughput_rows_per_sec: float
    memory_mb: float
    duration_seconds: float
    dataset_size: int
    timestamp: str
    git_commit: str = ''
    environment: str = 'local'


class BenchmarkManager:
    """Manage performance benchmarks and regression detection"""

    def __init__(self, benchmarks_file: str = 'performance_benchmarks.json'):
        self.benchmarks_file = Path(benchmarks_file)
        self.benchmarks = self._load_benchmarks()

    def _load_benchmarks(self) -> Dict[str, PerformanceBenchmark]:
        """Load existing benchmarks from file"""
        if not self.benchmarks_file.exists():
            return {}

        try:
            with open(self.benchmarks_file) as f:
                data = json.load(f)
                return {key: PerformanceBenchmark(**value) for key, value in data.items()}
        except Exception:
            return {}

    def save_benchmark(self, benchmark: PerformanceBenchmark):
        """Save a new benchmark result"""
        key = f'{benchmark.loader_type}_{benchmark.test_name}'
        self.benchmarks[key] = benchmark

        # Save to file
        with open(self.benchmarks_file, 'w') as f:
            json.dump({k: asdict(v) for k, v in self.benchmarks.items()}, f, indent=2)

    def check_regression(self, current: PerformanceBenchmark) -> Dict[str, Any]:
        """Check if current performance represents a regression"""
        key = f'{current.loader_type}_{current.test_name}'
        baseline = self.benchmarks.get(key)

        if not baseline:
            return {'is_regression': False, 'reason': 'No baseline found'}

        # Check throughput regression (5% slower)
        throughput_ratio = current.throughput_rows_per_sec / baseline.throughput_rows_per_sec
        if throughput_ratio < 0.95:
            return {'is_regression': True, 'metric': 'throughput', 'current': current.throughput_rows_per_sec, 'baseline': baseline.throughput_rows_per_sec, 'regression_pct': (1 - throughput_ratio) * 100}

        # Check memory regression (10% more memory)
        if baseline.memory_mb > 0:  # Only check if baseline has memory data
            memory_ratio = current.memory_mb / baseline.memory_mb
            if memory_ratio > 1.10:
                return {'is_regression': True, 'metric': 'memory', 'current': current.memory_mb, 'baseline': baseline.memory_mb, 'regression_pct': (memory_ratio - 1) * 100}

        # Check duration regression (10% slower)
        if baseline.duration_seconds > 0:  # Only check if baseline has duration data
            duration_ratio = current.duration_seconds / baseline.duration_seconds
            if duration_ratio > 1.10:
                return {'is_regression': True, 'metric': 'duration', 'current': current.duration_seconds, 'baseline': baseline.duration_seconds, 'regression_pct': (duration_ratio - 1) * 100}

        return {'is_regression': False}


def get_git_commit() -> str:
    """Get current git commit hash"""
    try:
        import subprocess

        result = subprocess.run(['git', 'rev-parse', 'HEAD'], capture_output=True, text=True)
        return result.stdout.strip()[:8]
    except Exception:
        return 'unknown'


def record_benchmark(test_name: str, loader_type: str, result_data: Dict[str, Any]):
    """Record a benchmark result"""
    benchmark = PerformanceBenchmark(test_name=test_name, loader_type=loader_type, throughput_rows_per_sec=result_data.get('throughput', 0), memory_mb=result_data.get('memory_mb', 0), duration_seconds=result_data.get('duration', 0), dataset_size=result_data.get('dataset_size', 0), timestamp=datetime.now().isoformat(), git_commit=get_git_commit(), environment=os.getenv('PERF_ENV', 'local'))

    manager = BenchmarkManager()
    regression_check = manager.check_regression(benchmark)

    if regression_check['is_regression']:
        print('\n⚠️  PERFORMANCE REGRESSION DETECTED:')
        print(f'   Test: {test_name}')
        print(f'   Loader: {loader_type}')
        print(f'   Metric: {regression_check["metric"]}')
        print(f'   Regression: {regression_check["regression_pct"]:.1f}%')
        print(f'   Current: {regression_check["current"]}')
        print(f'   Baseline: {regression_check["baseline"]}')

    manager.save_benchmark(benchmark)
    return regression_check
