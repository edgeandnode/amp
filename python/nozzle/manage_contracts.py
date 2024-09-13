import click
from .contracts import Contracts
from .chains import Chain
from .abi import ABI
import os

@click.group()
def cli():
    """Manage contracts for the nozzle project."""
    pass

@cli.command()
@click.option('--name', prompt='Contract name', help='Name of the contract')
@click.option('--address', prompt='Contract address', help='Address of the contract')
@click.option('--chain', prompt='Chain', type=click.Choice(['ethereum', 'arbitrum']), help='Chain of the contract')
@click.option('--abi-path', default=None, help='Path to local ABI file (optional)')
@click.option('--force-refresh', is_flag=True, help='Force refresh of ABI from API')
def add(name, address, chain, abi_path, force_refresh):
    """Add a new contract to the registry."""
    try:
        chain_enum = Chain[chain.upper()]
        if force_refresh:
            # Remove stored ABI if it exists
            storage_path = ABI._get_storage_path(chain_enum, address)
            if os.path.exists(storage_path):
                os.remove(storage_path)
                click.echo(f"Removed stored ABI for {name}")
        
        Contracts.add_contract(name, address, chain_enum, abi_path)
        click.echo(f"Contract {name} added successfully.")
    except ValueError as e:
        click.echo(f"Error adding contract: {str(e)}", err=True)

@cli.command()
def load():
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

@cli.command()
def list():
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

@cli.command()
@click.argument('name')
def info(name):
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

if __name__ == '__main__':
    cli()