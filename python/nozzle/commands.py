import click
from .contracts import Contracts
from .chains import Chain
from .abi import ABI
from .util import get_all_event_strings
import os
from pathlib import Path

VIEW_DIR = Path("../examples")

@click.group()
def cli():
    """Command line tools for the nozzle project."""
    pass

@cli.command('add_contract')
@click.option('--address', prompt='Contract address', help='Address of the contract')
@click.option('--chain', prompt='Chain', type=click.Choice(['ethereum', 'arbitrum']), help='Chain of the contract')
@click.option('--name', prompt='Contract name', help='Name of the contract (optional). Will only be used if the contract is not found in the API, otherwise contract name will be used.')
@click.option('--abi-path', default=None, help='Path to local ABI file (optional)')
@click.option('--force-refresh', default=False, is_flag=True, help='Force refresh of ABI from API')
def add_contract(name, address, chain, abi_path, force_refresh):
    """Add a new contract to the registry."""
    try:
        chain = Chain.from_name(chain)
        if force_refresh:
            # Remove stored ABI if it exists
            storage_path = ABI._get_storage_path(chain, address)
            if os.path.exists(storage_path):
                os.remove(storage_path)
                click.echo(f"Removed stored ABI for {name}")
        
        Contracts.add_contract(address, chain, name, abi_path, force_refresh)
        click.echo(f"Contract {name} added successfully.")
    except ValueError as e:
        click.echo(f"Error adding contract: {str(e)}", err=True)

@cli.command('load_contracts')
def load_contracts():
    """Load all defined contracts and display their information."""
    contracts = Contracts.get_all_contracts()
    if not contracts:
        click.echo("No contracts available.")
    else:
        click.echo("Loaded contracts:")
        for contract in contracts:
            click.echo(f"  - {contract.name}:")
            click.echo(f"    Address: {contract.address}")
            click.echo(f"    Chain: {contract.chain.value}")
            click.echo(f"    Events: {len(contract.events)}")
            click.echo()

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


@cli.command('create_event_view')
@click.option('--name', help='Name of the view file to create')
@click.option('--contract_name', help='Name of the contract')
@click.option('--address', help='Address of the contract')
@click.option('--event_list', help='List of events to create views for')
@click.option('--chain', help='Chain of the contract')
@click.option('--start_block', type=int, help='Start block')
@click.option('--end_block', type=int, help='End block')
def create_event_view(name, contract_name, address, event_list, chain, start_block, end_block):
    """Codegen to create a new view file for a contract."""
    with open(VIEW_DIR / f"{name}.py", "w") as f:
        f.write(f"from nozzle.view import View\n")
        f.write(f"from nozzle.event_registry import EventRegistry\n")
        f.write(f"\n")
        f.write(f"class {name}_view(View):\n")
        f.write(f"    def __init__(self, start_block=None, end_block=None):\n")
        f.write(f"        super().__init__(\n")
        f.write(f"            events=[\n")
        # if name is provided, and event_list is not, use the contract's events
        if contract_name and not event_list and not address:
            contract = Contracts.get_contract(contract_name)
            for event in get_all_event_strings():
                if event.startswith(contract_name):
                    f.write(f"                EventRegistry.{event},\n")
        # if event_list is provided, use the provided events
        elif event_list:
            for event in event_list:
                if event in get_all_event_strings():
                    f.write(f"EventRegistry.{event},\n")
                else:
                    raise ValueError(f"Event {event} not found in EventRegistry")
        # if address is provided, use the address's events
        elif address and contract_name and chain:
            Contracts.add_contract(contract_name, address, Chain.from_name(chain))
            for event in get_all_event_strings():
                if event.startswith(contract_name):
                    f.write(f"                EventRegistry.{event},\n")
        else:
            raise ValueError("You must provide an existing contract name, existing events, or a contract address.")
        f.write(f"            ],\n")
        f.write(f"            start_block={start_block},\n")
        f.write(f"            end_block={end_block}\n")
        f.write(f"        )\n")
        f.write(f"\n")
        f.write(f"    def query(self) -> str:\n")
        f.write(f"        # User must implement a valid DataFusion SQL query here\n")
        f.write(f"        pass\n")
        f.write(f"\n")
        f.write(f"# Usage\n")
        f.write(f"{name}_view = {name}_view(start_block={start_block}, end_block={end_block})\n")
        f.write(f"result = {name}_view.execute()\n")
        f.write(f"\n")



if __name__ == '__main__':
    cli()


# to run:
# python -m nozzle.commands  add_contract --name weth --address 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 --chain ethereum --abi-path /path/to/abi.json