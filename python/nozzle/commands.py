import click
from .contracts import Contracts
from .contract_registry import ContractRegistry
from .chains import Chain
from .abi import ABI
import os
from pathlib import Path
import requests
from .event_parameter_registry import EventParameterRegistry, EventParameter
from .client import Client
import pyarrow.parquet as pq
from .client import process_query
from .event import Event, EventParameter
from .table_registry_dynamic import DynamicTableRegistry, reload_table_registry
import pyarrow as pa
from .registry_manager import RegistryManager
from .address import EVMAddress
from .contract import Contract
from .util import get_pyarrow_type
import logging

VIEW_DIR = Path("examples")
DATA_DIR = Path("data")
NOZZLE_URL = "grpc://34.122.177.97:80"
BASE_FIREHOSE_TABLE = "eth_firehose.logs"



@click.group()
def cli():
    """Command line tools for the nozzle project."""
    pass

@cli.command('add_contract')
@click.option('--address', prompt='Contract address', help='Address of the contract')
@click.option('--chain', prompt='Chain', type=click.Choice(['ethereum', 'arbitrum']), help='Chain of the contract')
@click.option('--name', default=None, help='Name of the contract (optional). Will only be used if the contract is not found in the API, otherwise contract name will be used.')
@click.option('--abi-path', default=None, help='Path to local ABI file (optional)')
@click.option('--force-refresh', default=False, is_flag=True, help='Force refresh of ABI from API')
def add_contract(name, address, chain, abi_path, force_refresh):
    """Add a new contract to the registry."""
    try:
        chain = Chain.from_name(chain)
        if 'name' in requests.get(f"{chain.contract_api_url}{address}").json():
            contract_name = requests.get(f"{chain.contract_api_url}{address}").json()['name']
            name = contract_name
        else:
            contract_name = name
        if force_refresh:
            # Remove stored ABI if it exists
            storage_path = ABI._get_storage_path(chain, contract_name, address)
            if os.path.exists(storage_path):
                os.remove(storage_path)
                click.echo(f"Removed stored ABI for {name}")
        
        contract = Contracts.add_contract(address, chain, name, abi_path, force_refresh)
        click.echo(f"Contract {name} added successfully.")

        event_params = prompt_for_event_descriptions(contract)
        existing_registry = RegistryManager.get_registry("event_parameter")

        for event_name, params in event_params.items():
            full_event_name = f"{contract_name}.{event_name}"
            if full_event_name not in existing_registry:
                existing_registry[full_event_name] = {}
            for param_name, param in params.items():
                if param_name not in existing_registry[full_event_name]:
                    existing_registry[full_event_name][param_name] = {}
                existing_registry[full_event_name][param_name]["description"] = param.description

        RegistryManager.update_registry("event_parameter", existing_registry)

        # Update the event parameter registry
        click.echo("Event parameter registry updated successfully.")


    except ValueError as e:
        click.echo(f"Error adding contract: {str(e)}", err=True)


def prompt_for_event_descriptions(contract):
    """Prompt the user for descriptions of each event and its parameters in the contract."""
    event_params = {}
    event_descriptions = {}
    for event_name in dir(contract.events):
        if not event_name.startswith('_'):  # Skip any private attributes
            event = getattr(contract.events, event_name)
            if isinstance(event, Event):  # Make sure it's an actual event
                click.echo(f"\nEvent: {event_name}")
                event_descriptions[event_name] = click.prompt("  Enter description for this event", default='')
                event_params[event_name] = {}
                for param in event.abi_event['inputs']:
                    param_description = click.prompt(f"  Enter description for parameter '{param['name']}' ({param['type']})", default='')
                    event_params[event_name][param['name']] = EventParameter(
                        name=param['name'],
                        abi_type=param['type'],
                        pyarrow_type=get_pyarrow_type(param['type']),
                        indexed=param['indexed'],
                        description=param_description
                    )
    # Update the event descriptions in the registry
    existing_registry = RegistryManager.get_registry("event")
    for event_name, description in event_descriptions.items():
        if contract.name in existing_registry:
            if event_name not in existing_registry[contract.name]:
                existing_registry[contract.name][event_name] = {}
            existing_registry[contract.name][event_name]['description'] = description
    RegistryManager.update_registry("event", existing_registry)
    return event_params


@cli.command('run_event_preprocessing')
@click.option('--contract-name', type=str, help='Name of the contract')
@click.option('--events', type=str, help='List of events to preprocess')
@click.option('--start-block', type=int, help='Start block')
@click.option('--end-block', type=int, help='End block')
def run_event_preprocessing(contract_name: str, events: str, start_block: int, end_block: int):
    try:
        # Get contract data from the registry
        
        contract_data = RegistryManager.get_registry_item("contract", contract_name)
        print(contract_data.__dict__)
        if contract_data is None:
            raise ValueError(f"No contract found with name '{contract_name}'")

        # Reconstruct the Contract object
        contract = Contract(
            name=contract_data.name,
            address=EVMAddress(contract_data.address),
            chain=Chain[contract_data.chain.upper()]
        )

        if events:
            event_names = events.split(',')
            events_to_preprocess = [getattr(contract.events, event_name) for event_name in event_names if hasattr(contract.events, event_name)]
        else:
            events_to_preprocess = [getattr(contract.events, event_name) for event_name in dir(contract.events) if not event_name.startswith('_') and isinstance(getattr(contract.events, event_name), Event)]

        if not events_to_preprocess:
            click.echo("No valid events found for preprocessing.")
            return

        preprocess_event_data(start_block, end_block, events_to_preprocess, force_refresh=False, run=True)
        click.echo(f"Event preprocessing completed for contract {contract_name}.")

    except ValueError as e:
        click.echo(f"Error: {str(e)}", err=True)
    except AttributeError as e:
        click.echo(f"Error accessing registry data: {str(e)}", err=True)

@cli.command('list_contracts')
def list_contracts():
    """List all available contracts."""
    contracts = Contracts.get_all_contracts()
    if not contracts:
        click.echo("No contracts available.")
    else:
        click.echo("Available contracts:")
        for contract in contracts:
            click.echo(f"  - {contract['name']}:")
            click.echo(f"    Address: {contract['address']}")
            click.echo(f"    Chain: {contract['chain']}")
            click.echo(f"    Events: {contract['event_count']}")
            click.echo()

@cli.command('contract_info')
@click.argument('name')
def contract_info(name):
    """Display detailed information about a specific contract."""
    try:
        contract = Contracts.get_contract(name)
        click.echo(f"Contract: {contract.name}")
        click.echo(f"Address: {contract.address}")
        click.echo(f"Chain: {contract.chain.value}")
        click.echo(f"Events:")
        for event_name, event in contract.events.items():
            click.echo(f"  - {event_name}: {event.signature}")
    except ValueError as e:
        click.echo(f"Error: {str(e)}", err=True)


@cli.command('create_new_view')
@click.option('--name', help='Name of the view file to create')
@click.option('--description', help='Description of the view')
def create_new_view(name, description):
    """Codegen to create a new view file."""
    with open(VIEW_DIR / f"{name}.py", "w") as f:
        f.write(f"from nozzle.view import View\n")
        f.write(f"from datafusion import SessionContext, DataFrame, functions as f, lit, col\n")
        f.write(f"import pyarrow as pa\n")
        f.write(f"import argparse\n")
        f.write(f"from nozzle.table_registry import TableRegistry\n")
        f.write(f"\n")
        f.write(f"class {name}_view(View):\n")
        f.write(f"    def __init__(self, start_block, end_block):\n")
        f.write(f"        super().__init__(\n")
        f.write(f"            description='{description}',\n")
        f.write(f"            input_tables=[],  # Add your input tables here from TableRegistry\n")
        f.write(f"            start_block=start_block,\n")
        f.write(f"            end_block=end_block\n")
        f.write(f"        )\n")
        f.write(f"\n")
        f.write(f"    def query(self) -> str:\n")
        f.write(f"        # Implement your query here. You can use SQL or DataFrame operations.\n")
        f.write(f"        # Example SQL query:\n")
        f.write(f"        return f'''\n")
        f.write(f"        SELECT *\n")
        f.write(f"        FROM your_input_table\n")
        f.write(f"        WHERE block_num BETWEEN {{self.start_block}} AND {{self.end_block}}\n")
        f.write(f"        LIMIT 10\n")
        f.write(f"        '''\n")
        f.write(f"\n")
        f.write(f"        # Example DataFrame operations:\n")
        f.write(f"        # def df_operations(ctx: SessionContext) -> DataFrame:\n")
        f.write(f"        #     return ctx.table('your_input_table')\\\n")
        f.write(f"        #         .filter((col('block_num') >= lit(self.start_block)) & (col('block_num') <= lit(self.end_block)))\\\n")
        f.write(f"        #         .select([col('column_name')])\\\n")
        f.write(f"        #         .limit(10)\n")
        f.write(f"        # return df_operations\n")
        f.write(f"\n")
        f.write(f"    def schema(self) -> pa.lib.Schema:\n")
        f.write(f"        # Define the output schema of your view\n")
        f.write(f"        return pa.schema([\n")
        f.write(f"            pa.field('column_name', pa.string(), metadata={{'description': 'Description of the column'}}),\n")
        f.write(f"            # Add more fields as needed\n")
        f.write(f"        ])\n")
        f.write(f"\n")
        f.write(f"if __name__ == '__main__':\n")
        f.write(f"    parser = argparse.ArgumentParser(description='{description}')\n")
        f.write(f"    parser.add_argument('--start-block', type=int, required=True, help='Start block number')\n")
        f.write(f"    parser.add_argument('--end-block', type=int, required=True, help='End block number')\n")
        f.write(f"    args = parser.parse_args()\n")
        f.write(f"\n")
        f.write(f"    view = {name}_view(start_block=args.start_block, end_block=args.end_block)\n")
        f.write(f"    result = view.execute()\n")
        f.write(f"    print(result.to_pandas())\n")
        f.write(f"\n")
        f.write(f"# Available registered tables:\n")
        f.write(f"# {', '.join(DynamicTableRegistry.get_registry().keys())}\n")
        f.write(f"# To run the view, use: python3 {name}.py --start_block <start_block> --end_block <end_block>")

    click.echo(f"View file '{name}.py' created successfully.")
    click.echo(f"Please edit the file to implement your query, define the output schema, and specify input tables.")
    click.echo(f"Available registered tables: {', '.join(DynamicTableRegistry.get_registry().keys())}")
    click.echo(f"To run the view, use: python3 {name}.py --start_block <start_block> --end_block <end_block>")


def preprocess_event_data(start_block: int, end_block: int, events_to_preprocess: list[Event], force_refresh: bool = False, run: bool = False):
    for event in events_to_preprocess:
        event_dir = DATA_DIR / event.contract.name / event.name
        if not event_dir.exists():
            event_dir.mkdir(parents=True)                
        output_path = event_dir / f"{start_block}_{end_block}.parquet"
        
        table_name = f"preprocessed_event_{event.contract.name.lower()}_{event.name.lower()}"
        
        try:
            existing_table = DynamicTableRegistry.get_table_by_name(table_name)
            parquet_files = existing_table.parquet_files
            min_block = existing_table.min_block
            max_block = existing_table.max_block
            schema = existing_table.schema
        except ValueError:
            parquet_files = []
            min_block = start_block
            max_block = end_block
            schema = None

        if force_refresh or run or not existing_table:
            client = Client(NOZZLE_URL)
            print(f"Preprocessing data for {event.name} events from remote server")
            query = build_event_query(event, start_block, end_block, run)
            table = process_query(client, query)
            schema = table.schema
            pq.write_table(table, output_path)
            print(f"Table {table_name} registered with {table.num_rows} rows")
            print(f"Preprocessed data for {event.contract.name} {event.name} events saved to {output_path}")
            
            parquet_files.append(output_path)
            min_block = min(min_block, start_block)
            max_block = max(max_block, end_block)

        table_description = f"Preprocessed {event.contract.name} {event.name} event data"
        
        # Always update the DynamicTableRegistry with the latest information
        DynamicTableRegistry.register_table(
            table_name=table_name,
            schema=schema,
            description=table_description,
            parquet_files=parquet_files,
            min_block=min_block,
            max_block=max_block
        )

        reload_table_registry()

def build_event_query(event: Event, start_block: int, end_block: int, run: bool = False) -> str:
    print("EventParameterRegistry structure:")
    for attr in dir(EventParameterRegistry):
        if not attr.startswith("__"):
            print(f"  {attr}:")
            contract_attr = getattr(EventParameterRegistry, attr)
            for event_attr in dir(contract_attr):
                if not event_attr.startswith("__"):
                    print(f"    {event_attr}")
    columns = [
        "block_num",
        "timestamp",
        "tx_hash",
        "log_index",
        "address",
        f"'{event.signature}' as event_signature"
    ]
    decoded_columns = []
    try:
        contract_parameters = getattr(EventParameterRegistry, event.contract.name)
        event_specific_parameters = [val for key, val in contract_parameters.__dict__.items() if key.endswith(event.name + "EventParameters")]
        for param in event_specific_parameters:
            for param_name, param_value in param.__class__.__dict__.items():
                if not param_name.startswith("__"):
                    decoded_columns.append(f"decoded.{param_value.name} as {param_value.name}")
    
    except AttributeError as e:
        logging.error(f"Error accessing event parameters: {str(e)}")
        logging.error(f"Event: {event.contract.name}.{event.name}")
        logging.error(f"EventParameterRegistry structure: {EventParameterRegistry.__dict__}")
        raise
    
    decoded_columns_str = ", ".join(decoded_columns)
    # if parameter name is reserved word in sql, we need to quote it so query runs, and add alias
    RESERVED_SQL_WORDS = set(["from", "to", "value"])
    decoded_columns_str = ", ".join([f"`{col}` as {col}" if col in RESERVED_SQL_WORDS else col for col in decoded_columns_str.split(", ")])
    columns_str = ", ".join(columns + [decoded_columns_str])
    
    return f"""
    SELECT {columns_str}
    FROM (
        SELECT *,
            evm_decode(topic1, topic2, topic3, data, '{event.signature}') as decoded
        FROM {BASE_FIREHOSE_TABLE}
        WHERE address = arrow_cast(x'{str(event.contract.address)[2:]}', 'FixedSizeBinary(20)')
        AND topic0 = evm_topic('{event.signature}')
        AND block_num BETWEEN {start_block} AND {end_block}
        {f"" if run else "LIMIT 1000"}
    )
    """

if __name__ == '__main__':
    cli()

# to run:
# project from scratch, only interact through the examples files
# add contracts
# weth
# python3 -m nozzle.commands add_contract --address 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 --chain ethereum
# dai
# python3 -m nozzle.commands add_contract --address 0x6b175474e89094c44da98b954eedeac495271d0f --chain ethereum

# create new view
# python3 -m nozzle.commands create_new_view --name example_dai_counts_sql_2 --description "Example view for dai counts"

# run event preprocessing
# python3 -m nozzle.commands run_event_preprocessing --contract_name dai --events Transfer,Approval --start_block 17500000 --end_block 17500100


# sql run
# df run
# python3 -m examples.example_weth_counts_df --run

# input table create view

# input table run

# python notebook or ipython exploratory data analysis

# open source local dev flow for sql and df and test market / see if there's interest

# open questions
# scope of presentation (local, not remote/production data service)
# goal in python client is to define the interface and local development flow, testing, iteration
# local flow --> publish flow (format, namespace, versioning, etc.) --> run flow (scheduling, queues, materialization, etc.)
# data catalog, web interface for exploring/onboarding views, contracts, events, event parameters
# TODO: scheduling, queues, materialization, versioning, etc.

# semiotic verifiability refereed game on the data sources - once only computations/checkpoint
