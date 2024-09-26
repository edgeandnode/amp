from .base_contract import BaseContract
from .contract import ContractEvents
from .chains import Chain
from .address import EVMAddress
from .registered_contract import RegisteredContract
from .event_registry import EventRegistry


class DaiContract(RegisteredContract):
    name = 'Dai'
    address = EVMAddress('0x6B175474E89094C44Da98b954EedeAC495271d0F')
    chain = Chain.ETHEREUM
    events = EventRegistry.Dai
class LUSDTokenContract(RegisteredContract):
    name = 'LUSDToken'
    address = EVMAddress('0x5f98805A4E8be255a32880FDeC7F6728C6568bA0')
    chain = Chain.ETHEREUM
    events = EventRegistry.LUSDToken
class WETH9Contract(RegisteredContract):
    name = 'WETH9'
    address = EVMAddress('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2')
    chain = Chain.ETHEREUM
    events = EventRegistry.WETH9

class ContractRegistry:
    Dai = DaiContract
    LUSDToken = LUSDTokenContract
    WETH9 = WETH9Contract