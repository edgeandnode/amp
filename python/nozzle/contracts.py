from typing import Dict, List, Optional
from .chains import Chain
from .contract import Contract
from .event import Event
from .address import EVMAddress
from .abi import ABI
from .base_contract import BaseContract
from .registry_manager import RegistryManager
from .util import get_pyarrow_type
from .event_parameter_registry import EventParameter

class Contracts:
    contracts: Dict[str, Contract] = {}

    @classmethod
    def add_contract(cls, address: str, chain: Chain, name: Optional[str] = None, local_abi_path: Optional[str] = None, force_refresh: bool = False):
        if name in cls.contracts:
            raise ValueError(f"Contract {name} already exists")
        
        abi = ABI.fetch(address, chain, name, force_refresh)

        contract = Contract(name=name, address=EVMAddress(address), chain=chain)
        contract.abi = abi
        events = ABI.extract_events(abi, contract)

        for event in events.values():
            contract.add_event(event)
        
        cls.contracts[name] = contract
        
        cls._update_contract_registry(contract)
        cls._update_event_registry(contract)
        cls._update_event_parameter_registry(contract)

        ABI._save_abi_to_file(abi, chain, name, address)

        print(f"Successfully added contract {name} with {len(events)} events")
        for event_name in events:
            print(f"  - {event_name}")
        return contract

    @classmethod
    def _update_event_parameter_registry(cls, contract: Contract):
        parameter_data = {}
        for event_name, event in contract.events.__dict__.items():
            if isinstance(event, Event):
                event_params = {}
                for param_name, param in event.parameters.__dict__.items():
                    if isinstance(param, EventParameter):
                        event_params[param_name] = {
                            "name": param.name,
                            "type": param.abi_type,
                            "pyarrow_type": str(param.pyarrow_type),
                            "indexed": param.indexed,
                            "description": param.description
                        }
                parameter_data[f"{contract.name}.{event_name}"] = event_params
        
        RegistryManager.update_registry("event_parameter", parameter_data)

    @classmethod
    def _update_contract_registry(cls, contract: Contract):
        contract_data = {
            contract.name: {
                "name": contract.name,
                "address": str(contract.address),
                "chain": contract.chain.name
            }
        }
        RegistryManager.update_registry("contract", contract_data)

    @classmethod
    def _update_event_registry(cls, contract: Contract):
        event_data = {
            contract.name: {
                event_name: {
                    "name": event.name,
                    "contract": {
                        "name": contract.name,
                        "address": str(contract.address),
                        "chain": contract.chain.name
                    },
                    "signature": event.signature
                }
                for event_name, event in contract.events.__dict__.items()
            }
        }
        RegistryManager.update_registry("event", event_data)

    @classmethod
    def _generate_events_class(cls, contract: Contract) -> str:
        events = []
        for event_name, event in contract.events.__dict__.items():
            # params = ", ".join([f"EventParameter(name='{p.name}', type='{p.type}', indexed={p.indexed})" for p in event.parameters])
            # events.append(f"        {event_name} = RegisteredEvent(name='{event_name}', contract=Contracts.generate_base_contract(name='{event.contract.name}', address='{str(event.contract.address)}', chain=Chain.{event.contract.chain.name.upper()}), signature='{event.signature}', parameters=[{params}])\n")
            events.append(f"        {event_name} = RegisteredEvent(name='{event_name}', "
                          f"contract=generate_base_contract(name='{event.contract.name}', "
                          f"address='{str(event.contract.address)}', "
                          f"chain=Chain.{event.contract.chain.name.upper()}), "
                          f"signature='{event.signature}', "
                          f"parameters=event_parameter_registry.{contract.name}.{event_name})\n")

        return f"""
    class {contract.name}Events(ContractEvents):
{"".join(events)}
"""
    @classmethod
    def _generate_event_parameter_class(cls, contract: Contract) -> str:
        events = []
        for event_name, event in contract.events.__dict__.items():
            event_param = event_parameter_registry.get_event(contract.name, event_name)
            events.append(f"        {event_name} = EventParameter(name='{event_name}', type='event', indexed=False, description='{event_param.description}')\n")

        return f"""
    class {contract.name}EventParameters:
{"".join(events)}
    {contract.name} = {contract.name}EventParameters()
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
    
def generate_base_contract(name: str, address: str, chain: Chain, local_abi_path: Optional[str] = None, force_refresh: bool = False):

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