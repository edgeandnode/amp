from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, List

@dataclass
class ChainInfo:
    name: str
    chain_id: int
    contract_api_url: str

class Chain(Enum):
    ETHEREUM = ChainInfo("ethereum", 1, "https://eth.blockscout.com/api/v2/smart-contracts/")
    ARBITRUM = ChainInfo("arbitrum", 42161, "https://arbitrum.blockscout.com/api/v2/smart-contracts/")

    @property
    def contract_api_url(self):
        return self.value.contract_api_url

    @property
    def chain_id(self):
        return self.value.chain_id

    @property
    def name(self):
        return self.value.name

    @classmethod
    def from_chain_id(cls, chain_id: int):
        for chain in cls:
            if chain.chain_id == chain_id:
                return chain
        raise ValueError(f"No chain found for chain_id: {chain_id}")

    @classmethod
    def from_name(cls, name: str):
        for chain in cls:
            if chain.name.lower() == name.lower():
                return chain
        raise ValueError(f"No chain found with name: {name}")

@dataclass
class EventParameter:
    name: str
    type: str
    indexed: bool

@dataclass
class Event:
    name: str
    parameters: List[EventParameter]
    signature: str

@dataclass
class Contract:
    name: str
    address: str
    chain: Chain
    events: Dict[str, Event] = field(default_factory=dict)

class Contracts:
    # Add more contracts as needed

    @classmethod
    def get_all_contracts(cls):
        return [getattr(cls, attr) for attr in dir(cls) if isinstance(getattr(cls, attr), Contract)]