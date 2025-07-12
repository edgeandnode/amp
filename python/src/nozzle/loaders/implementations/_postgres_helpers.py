"""Helper functions for PostgreSQL loader to reduce code duplication"""

import io
from typing import Any, List, Tuple, Union

import pyarrow as pa
from pyarrow import csv


def prepare_csv_data(data: Union[pa.RecordBatch, pa.Table]) -> Tuple[io.StringIO, List[str]]:
    """
    Convert Arrow data to CSV format optimized for PostgreSQL COPY.

    Args:
        data: Arrow RecordBatch or Table

    Returns:
        Tuple of (csv_buffer, column_names)
    """
    output = io.BytesIO()

    # Create CSV write options optimized for PostgreSQL COPY
    try:
        csv_write_options = csv.WriteOptions(
            include_header=False,
            delimiter='\t',
            null_string='\\N',  # PostgreSQL null representation
            quoting_style='none',  # No quotes for better performance
        )
    except TypeError:
        # Fallback for older PyArrow versions
        try:
            csv_write_options = csv.WriteOptions(include_header=False, delimiter='\t')
        except TypeError:
            # Ultimate fallback
            csv_write_options = csv.WriteOptions()

    # Write Arrow data directly to CSV format (zero-copy operation)
    csv.write_csv(data, output, write_options=csv_write_options)

    # Convert to StringIO for psycopg2 compatibility
    csv_data = output.getvalue().decode('utf-8')

    # Handle null values manually if WriteOptions didn't support null_string
    if '\\N' not in csv_data and 'None' in csv_data:
        csv_data = csv_data.replace('None', '\\N')

    csv_buffer = io.StringIO(csv_data)

    # Get column names from Arrow schema
    column_names = [field.name for field in data.schema]

    return csv_buffer, column_names


def prepare_insert_data(data: Union[pa.RecordBatch, pa.Table]) -> Tuple[str, List[Tuple[Any, ...]]]:
    """
    Prepare data for INSERT operations (used for binary data).

    Args:
        data: Arrow RecordBatch or Table

    Returns:
        Tuple of (insert_sql_template, rows_data)
    """
    # Convert Arrow data to Python objects
    data_dict = data.to_pydict()

    # Get column names
    column_names = [field.name for field in data.schema]

    # Create INSERT statement template
    placeholders = ', '.join(['%s'] * len(column_names))
    insert_sql = f'({", ".join(column_names)}) VALUES ({placeholders})'

    # Prepare data for insertion
    rows = []
    for i in range(data.num_rows):
        row = []
        for col_name in column_names:
            value = data_dict[col_name][i]
            # Binary data is already in the correct format (bytes)
            row.append(value)
        rows.append(tuple(row))

    return insert_sql, rows


def has_binary_columns(schema: pa.Schema) -> bool:
    """Check if schema contains any binary column types."""
    return any(pa.types.is_binary(field.type) or pa.types.is_large_binary(field.type) or pa.types.is_fixed_size_binary(field.type) for field in schema)
