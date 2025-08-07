# tests/fixtures/test_data.py
"""
Test data generation utilities for data loader tests.
"""

import random
from datetime import date, datetime, timedelta
from decimal import Decimal

import pyarrow as pa


def create_test_arrow_table(num_rows: int = 1000, include_nulls: bool = True, include_complex_types: bool = True) -> pa.Table:
    """
    Create a comprehensive test Arrow table with various data types.

    Args:
        num_rows: Number of rows to generate
        include_nulls: Whether to include null values
        include_complex_types: Whether to include complex types (JSON, binary, etc.)

    Returns:
        Arrow Table with test data
    """

    data = {
        # Basic types
        'id': list(range(num_rows)),
        'name': [f'user_{i:04d}' for i in range(num_rows)],
        'age': [20 + (i % 60) for i in range(num_rows)],
        'score': [round(random.random() * 100, 2) for _ in range(num_rows)],
        'active': [i % 2 == 0 for i in range(num_rows)],
        # Date/time types
        'created_date': [date.today() - timedelta(days=i) for i in range(num_rows)],
        'updated_at': [datetime.now() - timedelta(hours=i) for i in range(num_rows)],
    }

    # Add nullable columns
    if include_nulls:
        data.update(
            {
                'nullable_int': [i if i % 10 != 0 else None for i in range(num_rows)],
                'nullable_string': [f'opt_{i}' if i % 7 != 0 else None for i in range(num_rows)],
                'nullable_float': [i * 0.1 if i % 5 != 0 else None for i in range(num_rows)],
            }
        )

    # Add complex types
    if include_complex_types:
        data.update(
            {
                'decimal_price': [Decimal(f'{i * 0.99:.2f}') for i in range(num_rows)],
                'json_metadata': [f'{{"id": {i}, "type": "test"}}' for i in range(num_rows)],
                'binary_data': [f'data_{i}'.encode('utf-8') for i in range(num_rows)],
                'category': random.choices(['A', 'B', 'C', 'D'], k=num_rows),
            }
        )

    # Create Arrow table directly
    return pa.Table.from_pydict(data)


def create_large_test_table(num_rows: int = 100000) -> pa.Table:
    """Create a large test table for performance testing"""

    # Generate in chunks to avoid memory issues
    chunk_size = 10000
    chunks = []

    for start in range(0, num_rows, chunk_size):
        end = min(start + chunk_size, num_rows)
        chunk_data = {
            'id': list(range(start, end)),
            'name': [f'user_{i:06d}' for i in range(start, end)],
            'value': [random.random() * 1000 for _ in range(start, end)],
            'timestamp': [datetime.now() for _ in range(start, end)],
            'category': random.choices(['X', 'Y', 'Z'], k=(end - start)),
        }

        chunk_table = pa.Table.from_pydict(chunk_data)
        chunks.append(chunk_table)

    return pa.concat_tables(chunks)


def create_streaming_test_data(num_batches: int = 10, batch_size: int = 1000) -> list[pa.RecordBatch]:
    """Create streaming test data as a list of RecordBatches"""

    batches = []
    for batch_num in range(num_batches):
        start_id = batch_num * batch_size
        data = {'batch_id': [batch_num] * batch_size, 'row_id': list(range(start_id, start_id + batch_size)), 'value': [random.random() for _ in range(batch_size)], 'timestamp': [datetime.now() for _ in range(batch_size)]}

        table = pa.Table.from_pydict(data)
        batches.append(table.to_batches()[0])

    return batches
