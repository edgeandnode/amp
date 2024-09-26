from dataclasses import dataclass
from .base_contract import BaseContract
from .chains import Chain
from .address import EVMAddress
from .event_parameter_registry import EventParameterRegistry

@dataclass
class RegisteredEvent:
    name: str
    contract: BaseContract
    signature: str
    parameters: object
    description: str


class DaiEvents:
    Approval = RegisteredEvent(
        name='Approval',
        contract=BaseContract(name='Dai', address=EVMAddress('0x6B175474E89094C44Da98b954EedeAC495271d0F'), chain=Chain.ETHEREUM),
        signature='Approval(address indexed src, address indexed guy, uint256 wad)',
        parameters=EventParameterRegistry.Dai.ApprovalEventParameters,
        description='4wt4wD'
    )
    LogNote = RegisteredEvent(
        name='LogNote',
        contract=BaseContract(name='Dai', address=EVMAddress('0x6B175474E89094C44Da98b954EedeAC495271d0F'), chain=Chain.ETHEREUM),
        signature='LogNote(bytes4 indexed sig, address indexed usr, bytes32 indexed arg1, bytes32 indexed arg2, bytes data)',
        parameters=EventParameterRegistry.Dai.LogNoteEventParameters,
        description='hdt'
    )
    Transfer = RegisteredEvent(
        name='Transfer',
        contract=BaseContract(name='Dai', address=EVMAddress('0x6B175474E89094C44Da98b954EedeAC495271d0F'), chain=Chain.ETHEREUM),
        signature='Transfer(address indexed src, address indexed dst, uint256 wad)',
        parameters=EventParameterRegistry.Dai.TransferEventParameters,
        description='tyhede'
    )
class WETH9Events:
    Approval = RegisteredEvent(
        name='Approval',
        contract=BaseContract(name='WETH9', address=EVMAddress('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'), chain=Chain.ETHEREUM),
        signature='Approval(address indexed src, address indexed guy, uint256 wad)',
        parameters=EventParameterRegistry.WETH9.ApprovalEventParameters,
        description='shrtxfg'
    )
    Transfer = RegisteredEvent(
        name='Transfer',
        contract=BaseContract(name='WETH9', address=EVMAddress('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'), chain=Chain.ETHEREUM),
        signature='Transfer(address indexed src, address indexed dst, uint256 wad)',
        parameters=EventParameterRegistry.WETH9.TransferEventParameters,
        description='xhrthxrt'
    )
    Deposit = RegisteredEvent(
        name='Deposit',
        contract=BaseContract(name='WETH9', address=EVMAddress('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'), chain=Chain.ETHEREUM),
        signature='Deposit(address indexed dst, uint256 wad)',
        parameters=EventParameterRegistry.WETH9.DepositEventParameters,
        description='hxtrrxht'
    )
    Withdrawal = RegisteredEvent(
        name='Withdrawal',
        contract=BaseContract(name='WETH9', address=EVMAddress('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'), chain=Chain.ETHEREUM),
        signature='Withdrawal(address indexed src, uint256 wad)',
        parameters=EventParameterRegistry.WETH9.WithdrawalEventParameters,
        description='hrxtxrht'
    )
class UniswapV2FactoryEvents:
    PairCreated = RegisteredEvent(
        name='PairCreated',
        contract=BaseContract(name='UniswapV2Factory', address=EVMAddress('0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f'), chain=Chain.ETHEREUM),
        signature='PairCreated(address indexed token0, address indexed token1, address pair, uint256 )',
        parameters=EventParameterRegistry.UniswapV2Factory.PairCreatedEventParameters,
        description='Uniswap pair was created'
    )
class UniswapV2PairEvents:
    Approval = RegisteredEvent(
        name='Approval',
        contract=BaseContract(name='UniswapV2Pair', address=EVMAddress('0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc'), chain=Chain.ETHEREUM),
        signature='Approval(address indexed owner, address indexed spender, uint256 value)',
        parameters=EventParameterRegistry.UniswapV2Pair.ApprovalEventParameters,
        description='Approval of LP'
    )
    Burn = RegisteredEvent(
        name='Burn',
        contract=BaseContract(name='UniswapV2Pair', address=EVMAddress('0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc'), chain=Chain.ETHEREUM),
        signature='Burn(address indexed sender, uint256 amount0, uint256 amount1, address indexed to)',
        parameters=EventParameterRegistry.UniswapV2Pair.BurnEventParameters,
        description='burnt'
    )
    Mint = RegisteredEvent(
        name='Mint',
        contract=BaseContract(name='UniswapV2Pair', address=EVMAddress('0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc'), chain=Chain.ETHEREUM),
        signature='Mint(address indexed sender, uint256 amount0, uint256 amount1)',
        parameters=EventParameterRegistry.UniswapV2Pair.MintEventParameters,
        description='mint'
    )
    Swap = RegisteredEvent(
        name='Swap',
        contract=BaseContract(name='UniswapV2Pair', address=EVMAddress('0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc'), chain=Chain.ETHEREUM),
        signature='Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)',
        parameters=EventParameterRegistry.UniswapV2Pair.SwapEventParameters,
        description=''
    )
    Sync = RegisteredEvent(
        name='Sync',
        contract=BaseContract(name='UniswapV2Pair', address=EVMAddress('0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc'), chain=Chain.ETHEREUM),
        signature='Sync(uint112 reserve0, uint112 reserve1)',
        parameters=EventParameterRegistry.UniswapV2Pair.SyncEventParameters,
        description=''
    )
    Transfer = RegisteredEvent(
        name='Transfer',
        contract=BaseContract(name='UniswapV2Pair', address=EVMAddress('0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc'), chain=Chain.ETHEREUM),
        signature='Transfer(address indexed from, address indexed to, uint256 value)',
        parameters=EventParameterRegistry.UniswapV2Pair.TransferEventParameters,
        description=''
    )

class EventRegistry:
    Dai = DaiEvents()
    WETH9 = WETH9Events()
    UniswapV2Factory = UniswapV2FactoryEvents()
    UniswapV2Pair = UniswapV2PairEvents()