# Nozzle: Local Blockchain Data Analysis Framework

## Table of Contents
1. [Introduction](#introduction)
2. [Value Proposition](#value-proposition)
3. [Getting Started](#getting-started)
   - [Installation](#installation)
   - [Adding Contracts](#adding-contracts)
   - [Preprocessing Data](#preprocessing-data)
   - [Creating and Running Views](#creating-and-running-views)
   - [Exploring Data in Notebooks](#exploring-data-in-notebooks)
   - [Using the Data Catalog Visualization](#using-the-data-catalog-visualization)
4. [Advanced Usage](#advanced-usage)
5. [Best Practices](#best-practices)

## Introduction

Nozzle is a powerful, user-friendly framework for blockchain data transformation. The local python client enables developers, researchers, and analysts to easily onboard smart contracts, preprocess on-chain data, create custom views, make arbitrary joins and transformations, and perform complex analyses—all on their local machine.

## Value Proposition

Nozzle's engineering design offers several key advantages:

1. **Local-First Development**: Run and test queries on your local machine using the same query engine that serves production queries, ensuring compatibility while allowing for fast iteration.
2. **Modular Architecture**: Easily extend the library of existing nozzle views with new data sources, views, and analysis techniques.
3. **Efficient Data Management**: Automatic preprocessing of event data and local caching to minimize redundant computations.
4. **Intuitive CLI Tools**: Simple commands for adding contracts, preprocessing data, and creating views.
5. **Integrated Visualization**: Built-in data catalog for easy exploration of available datasets and their relationships.
6. **Seamless Integration**: Works well with popular data science tools like Jupyter notebooks, dataframe and machine learning libraries in a local environment.

## Getting Started

### Installation

1. Install python dependencies and create a virtual environment:
   ```bash
   poetry install
   poetry shell
   cd examples
   ```

### Adding Contracts

To add a new contract for analysis:

```bash
# DAI
python3 -m nozzle.commands add_contract --address 0x6b175474e89094c44da98b954eedeac495271d0f --chain ethereum
# LUSD
python3 -m nozzle.commands add_contract --address 0x5f98805a4e8be255a32880fdec7f6728c6568ba0 --chain ethereum
#WETH
python3 -m nozzle.commands add_contract --address 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 --chain ethereum
```

This command does a few things behind the scenes: 
1. fetches the contract's ABI and stores it locally for future use
2. Prompts users to enter description of the contract's events and event parameters, which are used to populate a local data catalog
3. It adds metadata about the contract and its events to the registries (for contract, event, and event parameters), which allows for easy autocompletion and introspection of the data model.

### Preprocessing Data

Preprocess event data for a specific contract and block range:

```bash
# Run event preprocessing for these contracts to pull data into local tables to work with
python3 -m nozzle.commands run_event_preprocessing --contract-name Dai --events Approval,Transfer --start-block 17000000 --end-block 18000000

python3 -m nozzle.commands run_event_preprocessing --contract-name LUSDToken --events Approval,Transfer --start-block 17000000 --end-block 18000000

python3 -m nozzle.commands run_event_preprocessing --contract-name WETH9 --events Approval,Transfer --start-block 17000000 --end-block 17010000
```

This command fetches the specified events from a remote Nozzle server and stores them locally in Parquet files for efficient querying. It registers the table and associated parquet files in a registry so that it can be easily reloaded into a datafusion SessionContext for querying across sessions.

### Creating and Running Your Own Views

1. Create a new view:
   ```bash
   # Codegen to create a new nozzle view file
   python -m nozzle.commands create_new_view --name daily_lusd_transfers --description "LUSD transfers at the wallet level by day"
   ```

2. Edit the created view file (`daily_lusd_transfers.py`) to implement your analysis logic.
```python
from nozzle.view import View
from datafusion import SessionContext, DataFrame, functions as f, lit, col
import pyarrow as pa
import argparse
from nozzle.table_registry import TableRegistry

class daily_lusd_transfers_view(View):
    def __init__(self, start_block, end_block):
        super().__init__(
            description='Daily LUSD transfers by wallet',
            input_tables=[TableRegistry.preprocessed_event_lusdtoken_transfer],  # Add your input tables here from TableRegistry
            start_block=start_block,
            end_block=end_block
        )

    def query(self) -> str:
        # Implement your query here. You can use SQL or DataFrame operations.
        # Example SQL query:
        def df_operations(ctx: SessionContext) -> DataFrame:
            return ctx.table(TableRegistry.preprocessed_event_lusdtoken_transfer.name)\
                .aggregate([
                    col(TableRegistry.preprocessed_event_lusdtoken_transfer.columns.from_).alias('sender'),
                    f.date_trunc(lit('day'), col(TableRegistry.preprocessed_event_lusdtoken_transfer.columns.timestamp)).alias('day')
                ], [
                    f.count().alias('transfer_count'),
                    f.sum(col(TableRegistry.preprocessed_event_lusdtoken_transfer.columns.value_) / pow(10, 17)).alias('total_daily_lusd_transfer_value')
                ]).sort(f.order_by(col('total_daily_lusd_transfer_value'), ascending=False))
        return df_operations

    def schema(self) -> pa.lib.Schema:
        # Define the output schema of your view
        return pa.schema([
            pa.field('sender', pa.string(), metadata={'description': 'Sender of the transfer'}),
            pa.field('day', pa.timestamp('ns', tz='UTC'), metadata={'description': 'Day of the transfer'}),
            pa.field('transfer_count', pa.int64(), metadata={'description': 'Number of transfers'}),
            pa.field('total_daily_lusd_transfer_value', pa.float64(), metadata={'description': 'Total value of the transfers'}),
            # Add more fields as needed
        ])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Daily LUSD transfers by wallet')
    parser.add_argument('--start-block', type=int, required=True, help='Start block number')
    parser.add_argument('--end-block', type=int, required=True, help='End block number')
    args = parser.parse_args()

    view = daily_lusd_transfers_view(start_block=args.start_block, end_block=args.end_block)
    result = view.execute()
    print(result.to_pandas().head(10))

# Available registered tables:
# preprocessed_event_dai_approval, preprocessed_event_dai_transfer, preprocessed_event_lusdtoken_approval, preprocessed_event_lusdtoken_transfer
# To run the view, use: python3 daily_lusd_transfers.py --start_block <start_block> --end_block <end_block>
```

3. Run the view:
   ```bash
   # Run the view
   python3 daily_lusd_transfers.py --start-block 17000000 --end-block 18000000
   ```

### Exploring Data in Notebooks

1. Start a Jupyter notebook:
   ```bash
   jupyter notebook
   ```

2. In your notebook, you can import and use Nozzle views:
   ```python
   from nozzle.views.daily_lusd_transfers import daily_lusd_transfers_view
   from nozzle.table_registry import TableRegistry
   
   view = daily_lusd_transfers_view(start_block=17000000, end_block=18000000)
   result = view.execute()

   # You can also use the TableRegistry autocomplete to view all available tables and columns
   TableRegistry.daily_lusd_transfers_view.columns
   
   # Analyze the results
   import polars as pl
   df_pl = result.to_polars()
   df_pl.head()

   import pandas as pd
   df_pd = result.to_pandas()
   df_pd.head()
   ```

### Using the Data Catalog Visualization

Launch the data catalog visualization:

```bash
python3 -m nozzle.graph_view
```
![Local Nozzle Data Catalog Graph View](local_nozzle_data_catalog.png)
This opens a web interface where you can explore:
- Available contracts and their events
- Existing views and their dependencies
- Data schemas and types
