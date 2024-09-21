from typing import Dict, List, Optional
from enum import Enum, auto
from .chains import Chain
from .contract import Contract
from .event import Event
from .address import EVMAddress
from .abi import ABI
from .base_contract import BaseContract
from dataclasses import dataclass
import os
import re
import requests

class ContractAttribute:
    def __init__(self, contract: Contract):
        self.contract = contract

    def __get__(self, obj, objtype=None):
        return self.contract


class Contracts:
    contracts: Dict[str, Contract] = {}

    @classmethod
    def add_contract(cls, address: str, chain: Chain, name: Optional[str] = None, local_abi_path: Optional[str] = None, force_refresh: bool = False):
        if 'name' in requests.get(f"{chain.contract_api_url}{address}").json():
            name = requests.get(f"{chain.contract_api_url}{address}").json()['name']

        if name in cls.contracts:
            raise ValueError(f"Contract {name} already exists")
        
        abi = ABI.fetch(address, chain, name, force_refresh)

        contract = Contract(name = name, address = EVMAddress(address), chain = chain)
        contract.abi = abi
        events = ABI.extract_events(abi, contract)

        for event in events:
            contract.add_event(events[event])
        
        # Add attribute to Contracts
        cls.contracts[name] = contract
        setattr(cls, name, ContractAttribute(contract))
        # Update the EventRegistry file
        cls._update_event_registry(contract)
        # Update the ContractRegistry file
        cls._update_contract_registry(contract)

        print(f"Successfully added contract {name} with {len(events)} events")
        for event_name in events:
            print(f"  - {event_name}")

    @classmethod
    def _update_event_registry(cls, contract: Contract):
        registry_path = os.path.join(os.path.dirname(__file__), 'event_registry.py')

        events_class = cls._generate_events_class(contract)

        with open(registry_path, 'r') as f:
            content = f.readlines()
            text = ''.join(content)
        # If events_class does not appear in the file, add it
        if re.search(f'class {contract.name}Events', text) is None:
            new_content = []
            previous_line = None
            for line in content:
                if line.strip() == "class EventRegistry():":
                    new_content.append(line)
                    new_content.append(events_class)
                    new_content.append(f"    {contract.name} = {contract.name}Events()\n")
                elif line.strip() == "pass" and previous_line.strip() != 'class RegisteredEvent(Event):':
                    continue
                else:
                    new_content.append(line)
                previous_line = line
            with open(registry_path, 'w') as f:
                f.writelines(new_content)
        else:
            # Replace the existing events class with the new one
            for i, line in enumerate(content):
                if line.strip() == f"class {contract.name}Events":
                    content[i] = events_class
                    break
            with open(registry_path, 'w') as f:
                f.writelines(content)

    @classmethod
    def _update_contract_registry(cls, contract: Contract):
        registry_path = os.path.join(os.path.dirname(__file__), 'contract_registry.py')

        contract_class = cls._generate_contract_class(contract)

        with open(registry_path, 'r') as f:
            content = f.readlines()
            text = ''.join(content)

        if re.search(f'class {contract.name}Contract', text) is None:
            new_content = []
            previous_line = None
            for line in content:
                if line.strip() == "class ContractRegistry():":
                    new_content.append(line)
                    new_content.append(contract_class)
                    new_content.append(f"    {contract.name} = {contract.name}Contract\n")
                elif line.strip() == "pass" and previous_line.strip() != 'class RegisteredContract(Contract):':
                    continue
                else:
                    new_content.append(line)
                previous_line = line
            with open(registry_path, 'w') as f:
                f.writelines(new_content)
        else:
            # Replace the existing contract class with the new one
            for i, line in enumerate(content):
                if line.strip() == f"class {contract.name}Contract":
                    content[i] = contract_class
                    break
            with open(registry_path, 'w') as f:
                f.writelines(content)

    @classmethod
    def _generate_events_class(cls, contract: Contract) -> str:
        events = []
        for event_name, event in contract.events.__dict__.items():
            params = ", ".join([f"EventParameter(name='{p.name}', type='{p.type}', indexed={p.indexed})" for p in event.parameters])
            events.append(f"        {event_name} = RegisteredEvent(name='{event_name}', contract=Contracts.generate_base_contract(name='{event.contract.name}', address='{str(event.contract.address)}', chain=Chain.{event.contract.chain.name.upper()}), signature='{event.signature}', parameters=[{params}])\n")
        
        return f"""
    class {contract.name}Events(ContractEvents):
{"".join(events)}
"""

    @classmethod
    def _generate_contract_class(cls, contract: Contract) -> str:
        return f"""
    class {contract.name}Contract(RegisteredContract):
        name = '{contract.name}'
        address = EVMAddress('{contract.address}')
        chain = {contract.chain}
        events = EventRegistry.{contract.name}
"""
    
    @classmethod
    def get_all_contracts(cls) -> List[Contract]:
        return list(cls.contracts.values())

    @classmethod
    def get_contract(cls, name: str) -> Contract:
        contract = cls.contracts.get(name)
        if contract:
            return contract
        raise ValueError(f"No contract found with name '{name}'")
    
    @classmethod
    def get_events(cls, name: str) -> Dict[str, Event]:
        contract = cls.contracts.get(name)
        if contract:
            return contract.events
        raise ValueError(f"No contract found with name '{name}'")
    
    @classmethod
    def generate_base_contract(cls, name: str, address: str, chain: Chain, local_abi_path: Optional[str] = None, force_refresh: bool = False):
        if name in cls.contracts:
            raise ValueError(f"Contract {name} already exists")

        evm_address = EVMAddress(address)
        
        base_contract = BaseContract(name, evm_address, chain)

        return base_contract


# Example usage:
# Contracts.add_contract("WETH", "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", Chain.ETHEREUM)
# Contracts.add_contract("USDC", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", Chain.ETHEREUM)
# 
# # Now you can use autocompletion:
# weth_contract = Contracts.get_contract(ContractEnum.weth)
# usdc_events = Contracts.get_events(ContractEnum.usdc)
# 
# # Access specific events:
# transfer_event = weth_contract.events[ContractEvents.WETH_Transfer]
# # Access contract information from an event:
# event_contract = transfer_event.contract
# event_contract_address = event_contract.address
# event_contract_chain = event_contract.chain