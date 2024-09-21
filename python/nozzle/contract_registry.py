from .base_contract import BaseContract
from .contract import Contract, ContractEvents
from .contracts import Contracts
from .chains import Chain
from .address import EVMAddress
from .event import Event, EventParameter
from dataclasses import dataclass
from .registered_event import RegisteredEvent
from .registered_contract import RegisteredContract
from .event_registry import EventRegistry

@dataclass
class ContractRegistry():

    class WETH9Contract(RegisteredContract):
        name = 'WETH9'
        address = EVMAddress('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2')
        chain = Chain.ETHEREUM
        events = EventRegistry.WETH9
    WETH9 = WETH9Contract

    class DaiContract(RegisteredContract):
        name = 'dai'
        address = EVMAddress('0x6B175474E89094C44Da98b954EedeAC495271d0F')
        chain = Chain.ETHEREUM
        events = EventRegistry.dai
    dai = DaiContract
    # New contract classes will be added here
