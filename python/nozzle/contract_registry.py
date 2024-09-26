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
class WETH9Contract(RegisteredContract):
    name = 'WETH9'
    address = EVMAddress('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2')
    chain = Chain.ETHEREUM
    events = EventRegistry.WETH9
class UniswapV2FactoryContract(RegisteredContract):
    name = 'UniswapV2Factory'
    address = EVMAddress('0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f')
    chain = Chain.ETHEREUM
    events = EventRegistry.UniswapV2Factory
class UniswapV2PairContract(RegisteredContract):
    name = 'UniswapV2Pair'
    address = EVMAddress('0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc')
    chain = Chain.ETHEREUM
    events = EventRegistry.UniswapV2Pair

class ContractRegistry:
    Dai = DaiContract
    WETH9 = WETH9Contract
    UniswapV2Factory = UniswapV2FactoryContract
    UniswapV2Pair = UniswapV2PairContract