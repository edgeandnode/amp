from typing import Dict, List, Optional
from enum import Enum, auto
from .chains import Chain
from .contract import Contract
from .event import Event
from .address import EVMAddress
from .abi import ABI
from .base_contract import BaseContract
from dataclasses import dataclass

class ContractAttribute:
    def __init__(self, contract: Contract):
        self.contract = contract

    def __get__(self, obj, objtype=None):
        return self.contract


class Contracts:
    contracts: Dict[str, Contract] = {}

    @classmethod
    def add_contract(cls, name: str, address: str, chain: Chain, local_abi_path: Optional[str] = None):
        name = name.lower()

        if name in cls.contracts:
            raise ValueError(f"Contract {name} already exists")
        
        abi = ABI.fetch(address, chain, name)

        evm_address = EVMAddress(address)
        
        base_contract = BaseContract(name, evm_address, chain)

        events = ABI.extract_events(abi, base_contract)

        contract = Contract(name=name, address=evm_address, chain=chain)
        contract.abi = abi

        for event in events:
            contract.add_event(events[event])
        
        # Add attribute to Contracts
        cls.contracts[name] = contract
        setattr(cls, name, ContractAttribute(contract))

        print(f"Successfully added contract {name} with {len(events)} events")
        for event_name in events:
            print(f"  - {event_name}")

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