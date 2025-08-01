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
    # Note: Modern PyArrow versions don't support null_string parameter in WriteOptions
    try:
        csv_write_options = csv.WriteOptions(
            include_header=False,
            delimiter='\t',
            quoting_style='needed',  # Quote only when needed for complex data
        )
    except TypeError:
        # Fallback for older PyArrow versions
        csv_write_options = csv.WriteOptions()

    # Write Arrow data directly to CSV format (zero-copy operation)
    csv.write_csv(data, output, write_options=csv_write_options)

    # Convert to StringIO for psycopg2 compatibility
    csv_data = output.getvalue().decode('utf-8')

    # Fix null handling: PyArrow outputs empty strings for nulls, but PostgreSQL COPY expects \N
    # We need to identify null positions from the original Arrow data and replace empty fields
    csv_data = _fix_null_values_in_csv(csv_data, data)

    csv_buffer = io.StringIO(csv_data)

    # Get column names from Arrow schema
    column_names = [field.name for field in data.schema]

    return csv_buffer, column_names


def _fix_null_values_in_csv(csv_data: str, data: Union[pa.RecordBatch, pa.Table]) -> str:
    """
    Fix null values in CSV data by replacing empty fields with \\N where nulls exist in Arrow data.
    
    This is necessary because PyArrow's CSV writer doesn't properly handle null_string parameter
    and outputs empty strings for null values, but PostgreSQL COPY expects \\N.
    """
    
    lines = csv_data.strip().split('\n')
    if not lines:
        return csv_data

    null_masks = []
    for i in range(data.num_columns):
        column = data.column(i)
        # Create boolean mask where True indicates null values
        null_mask = pa.compute.is_null(column).to_pylist()
        null_masks.append(null_mask)

    fixed_lines = []
    for row_idx, line in enumerate(lines):
        if not line.strip():  # Skip empty lines
            continue
            
        fields = line.split('\t')
        
        # Replace empty fields with \N where we know there are nulls
        for col_idx, field in enumerate(fields):
            if col_idx < len(null_masks) and row_idx < len(null_masks[col_idx]):
                if null_masks[col_idx][row_idx]:
                    fields[col_idx] = '\\N'
                elif field == '' and not null_masks[col_idx][row_idx]:
                    # This is an empty string value, not a null - keep it as is
                    # But we might need to handle this case differently depending on data type
                    pass
        
        fixed_lines.append('\t'.join(fields))
    
    return '\n'.join(fixed_lines) + '\n'


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
