# The Graph Nozzles: Blockchain Data Transformation Framework

## Table of Contents
1. [Introduction](#introduction)
2. [Getting Started](#getting-started)
3. [Data Directory Structure and Table Registration](#data-directory-structure-and-table-registration)
4. [Onboarding a New Contract](#onboarding-a-new-contract)
5. [Creating Views](#creating-views)
6. [Testing Views](#testing-views)
7. [Iterating on Views](#iterating-on-views)
8. [Composing Views](#composing-views)
9. [Best Practices](#best-practices)
10. [Performance Testing](#performance-testing)

## Introduction

Nozzle is a powerful framework for analyzing Ethereum blockchain data. It allows you to easily onboard contracts, create views for specific data analysis tasks, and compose these views for complex analyses.

## Getting Started

1. Clone the repository:
   ```bash
   git clone https://github.com/your-repo/nozzle.git
   cd nozzle
   ```

2. Install Poetry (if not already installed):
   ```bash
   curl -sSL https://install.python-poetry.org | python3 -
   ```

3. Install dependencies and activate the virtual environment:
   ```bash
   poetry install
   poetry shell
   ```

## Data Directory Structure and Table Registration

Nozzle organizes data in a structured directory hierarchy and automatically registers tables based on this structure. Understanding this system is crucial for efficient data management and querying.

### Directory Structure

The data is organized as follows:

```
nozzle_data/
├── ViewName1/
│   ├── EventName1/
│   │   ├── blocks_1000000_1999999.parquet
│   │   ├── blocks_2000000_2999999.parquet
│   │   └── ...
│   └── EventName2/
│       ├── blocks_1000000_1999999.parquet
│       └── ...
└── ViewName2/
    └── ...
```

- Each view has its own directory.
- Within each view directory, there are subdirectories for each event.
- Each event directory contains Parquet files, each covering a specific block range.

### Table Registration

When a view is initialized or refreshed:

1. The system scans the appropriate directories based on the view name and specified events.
2. For each Parquet file found:
   - A table is registered in the DataFusion context.
   - The table name follows the format: `preprocessed_{event_name}_{start_block}_{end_block}`.
   - These tables are stored in the view's `_event_tables` dictionary.

### Accessing Tables in Queries

When writing queries in your view's `get_query` method:

1. Use `self._event_tables[event_name]` to get a list of all registered tables for a specific event.
2. Each item in this list is a tuple: `(table_name, start_block, end_block)`.
3. You can construct queries that span multiple block ranges by UNIONing these tables.

Example:

```python
def get_query(self) -> str:
    transfer_tables = " UNION ALL ".join([
        f"SELECT * FROM {table_name}"
        for table_name, _, _ in self._event_tables['Transfer']
    ])
    
    return f"""
    SELECT COUNT(*) as transfer_count
    FROM ({transfer_tables})
    WHERE block_num BETWEEN {self.start_block} AND {self.end_block}
    """
```

This structure allows for efficient querying across large block ranges while maintaining manageable file sizes and enabling parallel processing.

## Onboarding a New Contract

1. Identify the contract you want to onboard (address, chain, name).
2. Add the contract to the `Contracts` class:

   ```python
   from nozzle.contracts import Contracts
   from nozzle.chains import Chain

   Contracts.add_contract(
       name="MyContract",
       address="0x1234567890123456789012345678901234567890",
       chain=Chain.ETHEREUM
   )
   ```

3. The ABI will be automatically fetched and cached. You can now access the contract and its events:

   ```python
   my_contract = Contracts.get_contract("MyContract")
   transfer_event = my_contract.events.Transfer
   ```

## Creating Views

1. Create a new file in the `nozzle/views` directory, e.g., `my_view.py`.
2. Define your view class, inheriting from `View`:

   ```python
   from nozzle.view import View
   from nozzle.contracts import Contracts

   class MyView(View):
       def __init__(self, start_block=None, end_block=None, force_refresh=False):
           my_contract = Contracts.get_contract("MyContract")
           super().__init__(
               events=[my_contract.events.Transfer],
               start_block=start_block,
               end_block=end_block,
               force_refresh=force_refresh
           )

       def get_query(self) -> str:
           return f"""
           SELECT COUNT(*) as transfer_count
           FROM {self._event_tables['Transfer'][0]}
           """
   ```

3. Implement the `get_query` method to define your analysis logic.

## Testing Views

1. Create a test file in the `tests` directory, e.g., `test_my_view.py`.
2. Write unit tests for your view:

   ```python
   import unittest
   from nozzle.views.my_view import MyView

   class TestMyView(unittest.TestCase):
       def test_my_view(self):
           view = MyView(start_block=1000000, end_block=1001000)
           result = view.execute()
           self.assertIsNotNone(result)
           self.assertTrue(len(result) > 0)
   ```

3. Run the tests:
   ```bash
   python -m unittest discover tests
   ```

## Iterating on Views

1. Start with a basic query in your view's `get_query` method.
2. Run the view and analyze the results.
3. Refine the query based on your findings.
4. Add new parameters to the view's `__init__` method if needed.
5. Update tests to cover new functionality.
6. Repeat steps 2-5 until you're satisfied with the results.

## Composing Views

1. Create a new view that combines data from multiple existing views:

   ```python
   from nozzle.view import View
   from nozzle.views.my_view import MyView
   from nozzle.views.other_view import OtherView

   class CompositeView(View):
       def __init__(self, start_block=None, end_block=None, force_refresh=False):
           self.my_view = MyView(start_block, end_block, force_refresh)
           self.other_view = OtherView(start_block, end_block, force_refresh)
           super().__init__(
               events=self.my_view.events + self.other_view.events,
               start_block=start_block,
               end_block=end_block,
               force_refresh=force_refresh
           )

       def get_query(self) -> str:
           my_view_query = self.my_view.get_query()
           other_view_query = self.other_view.get_query()
           return f"""
           WITH my_view_results AS ({my_view_query}),
                other_view_results AS ({other_view_query})
           SELECT *
           FROM my_view_results
           JOIN other_view_results ON my_view_results.block_num = other_view_results.block_num
           """
   ```

2. Ensure that the composite view handles all necessary events and data preprocessing.

## Best Practices

1. Naming Conventions:
   - Use descriptive names for views, e.g., `TokenTransferView`, `LiquidityPoolView`.
   - Name query columns clearly, e.g., `daily_volume`, `unique_users`.

2. Code Organization:
   - Keep each view in a separate file.
   - Use a consistent structure for view classes.

3. Documentation:
   - Add docstrings to your views explaining their purpose and parameters.
   - Comment complex SQL queries for clarity.

4. Error Handling:
   - Implement proper error handling in views, especially for edge cases.

5. Version Control:
   - Commit frequently with clear, descriptive commit messages.
   - Use feature branches for significant changes.

## Performance Testing

1. Use the `--force-refresh` flag to test query performance with fresh data:
   ```bash
   python -m nozzle.views.my_view --force-refresh
   ```

2. Monitor query execution time and resource usage.

3. For large queries, use EXPLAIN ANALYZE to understand query plans:
   ```sql
   EXPLAIN ANALYZE
   Your complex query here
   ```

4. Optimize queries by:
   - Adding appropriate indexes
   - Simplifying complex joins
   - Using CTEs for better readability and potential performance gains
   - Considering materialized views for frequently accessed data

5. Use the `process_query` function's built-in timing to measure performance improvements:
   ```python
   result = view.execute()
   print(f"Query execution time: {result.execution_time} seconds")
   ```

6. For very large datasets, consider using sampling techniques to test query logic before running on the full dataset.

Remember to always test performance improvements with realistic data volumes to ensure they provide actual benefits in production scenarios.