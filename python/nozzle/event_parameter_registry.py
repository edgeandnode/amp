from dataclasses import dataclass
import pyarrow as pa
from .util import get_pyarrow_type

@dataclass
class EventParameter:
    name: str
    abi_type: str
    pyarrow_type: pa.DataType
    indexed: bool
    description: str


class Dai:
    pass
class WETH9:
    pass
class UniswapV2Factory:
    pass
class UniswapV2Pair:
    pass


class DaiApprovalEventParameters():
    src = EventParameter(name='src', abi_type='address', pyarrow_type=get_pyarrow_type('address'), indexed=True, description='')
    guy = EventParameter(name='guy', abi_type='address', pyarrow_type=get_pyarrow_type('address'), indexed=True, description='')
    wad = EventParameter(name='wad', abi_type='uint256', pyarrow_type=get_pyarrow_type('uint256'), indexed=False, description='')
class DaiLogNoteEventParameters():
    sig = EventParameter(name='sig', abi_type='bytes4', pyarrow_type=get_pyarrow_type('bytes4'), indexed=True, description='')
    usr = EventParameter(name='usr', abi_type='address', pyarrow_type=get_pyarrow_type('address'), indexed=True, description='')
    arg1 = EventParameter(name='arg1', abi_type='bytes32', pyarrow_type=get_pyarrow_type('bytes32'), indexed=True, description='')
    arg2 = EventParameter(name='arg2', abi_type='bytes32', pyarrow_type=get_pyarrow_type('bytes32'), indexed=True, description='')
    data = EventParameter(name='data', abi_type='bytes', pyarrow_type=get_pyarrow_type('bytes'), indexed=False, description='')
class DaiTransferEventParameters():
    src = EventParameter(name='src', abi_type='address', pyarrow_type=get_pyarrow_type('address'), indexed=True, description='')
    dst = EventParameter(name='dst', abi_type='address', pyarrow_type=get_pyarrow_type('address'), indexed=True, description='')
    wad = EventParameter(name='wad', abi_type='uint256', pyarrow_type=get_pyarrow_type('uint256'), indexed=False, description='')
class WETH9ApprovalEventParameters():
    src = EventParameter(name='src', abi_type='address', pyarrow_type=get_pyarrow_type('address'), indexed=True, description='fgxhrxhf')
    guy = EventParameter(name='guy', abi_type='address', pyarrow_type=get_pyarrow_type('address'), indexed=True, description='rhtrht')
    wad = EventParameter(name='wad', abi_type='uint256', pyarrow_type=get_pyarrow_type('uint256'), indexed=False, description='xhrtrxh')
class WETH9TransferEventParameters():
    src = EventParameter(name='src', abi_type='address', pyarrow_type=get_pyarrow_type('address'), indexed=True, description='hxtrxhr')
    dst = EventParameter(name='dst', abi_type='address', pyarrow_type=get_pyarrow_type('address'), indexed=True, description='xhtrxrht')
    wad = EventParameter(name='wad', abi_type='uint256', pyarrow_type=get_pyarrow_type('uint256'), indexed=False, description='xhrtxhr')
class WETH9DepositEventParameters():
    dst = EventParameter(name='dst', abi_type='address', pyarrow_type=get_pyarrow_type('address'), indexed=True, description='hxrtxhrt')
    wad = EventParameter(name='wad', abi_type='uint256', pyarrow_type=get_pyarrow_type('uint256'), indexed=False, description='xhrhrxt')
class WETH9WithdrawalEventParameters():
    src = EventParameter(name='src', abi_type='address', pyarrow_type=get_pyarrow_type('address'), indexed=True, description='hxrthrxtr')
    wad = EventParameter(name='wad', abi_type='uint256', pyarrow_type=get_pyarrow_type('uint256'), indexed=False, description='xhrxhrmjc')
class UniswapV2FactoryPairCreatedEventParameters():
    token0 = EventParameter(name='token0', abi_type='address', pyarrow_type=get_pyarrow_type('address'), indexed=True, description='')
    token1 = EventParameter(name='token1', abi_type='address', pyarrow_type=get_pyarrow_type('address'), indexed=True, description='')
    pair = EventParameter(name='pair', abi_type='address', pyarrow_type=get_pyarrow_type('address'), indexed=False, description='')
    __ = EventParameter(name='__', abi_type='uint256', pyarrow_type=get_pyarrow_type('uint256'), indexed=False, description='')
class UniswapV2PairApprovalEventParameters():
    owner = EventParameter(name='owner', abi_type='address', pyarrow_type=get_pyarrow_type('address'), indexed=True, description='owner')
    spender = EventParameter(name='spender', abi_type='address', pyarrow_type=get_pyarrow_type('address'), indexed=True, description='spender')
    value = EventParameter(name='value', abi_type='uint256', pyarrow_type=get_pyarrow_type('uint256'), indexed=False, description='amount approved')
class UniswapV2PairBurnEventParameters():
    sender = EventParameter(name='sender', abi_type='address', pyarrow_type=get_pyarrow_type('address'), indexed=True, description='sender')
    amount0 = EventParameter(name='amount0', abi_type='uint256', pyarrow_type=get_pyarrow_type('uint256'), indexed=False, description='amount of token 0')
    amount1 = EventParameter(name='amount1', abi_type='uint256', pyarrow_type=get_pyarrow_type('uint256'), indexed=False, description='amount of token 1 ')
    to = EventParameter(name='to', abi_type='address', pyarrow_type=get_pyarrow_type('address'), indexed=True, description='to address')
class UniswapV2PairMintEventParameters():
    sender = EventParameter(name='sender', abi_type='address', pyarrow_type=get_pyarrow_type('address'), indexed=True, description='sender')
    amount0 = EventParameter(name='amount0', abi_type='uint256', pyarrow_type=get_pyarrow_type('uint256'), indexed=False, description='')
    amount1 = EventParameter(name='amount1', abi_type='uint256', pyarrow_type=get_pyarrow_type('uint256'), indexed=False, description='')
class UniswapV2PairSwapEventParameters():
    sender = EventParameter(name='sender', abi_type='address', pyarrow_type=get_pyarrow_type('address'), indexed=True, description='')
    amount0In = EventParameter(name='amount0In', abi_type='uint256', pyarrow_type=get_pyarrow_type('uint256'), indexed=False, description='')
    amount1In = EventParameter(name='amount1In', abi_type='uint256', pyarrow_type=get_pyarrow_type('uint256'), indexed=False, description='')
    amount0Out = EventParameter(name='amount0Out', abi_type='uint256', pyarrow_type=get_pyarrow_type('uint256'), indexed=False, description='')
    amount1Out = EventParameter(name='amount1Out', abi_type='uint256', pyarrow_type=get_pyarrow_type('uint256'), indexed=False, description='')
    to = EventParameter(name='to', abi_type='address', pyarrow_type=get_pyarrow_type('address'), indexed=True, description='')
class UniswapV2PairSyncEventParameters():
    reserve0 = EventParameter(name='reserve0', abi_type='uint112', pyarrow_type=get_pyarrow_type('uint112'), indexed=False, description='')
    reserve1 = EventParameter(name='reserve1', abi_type='uint112', pyarrow_type=get_pyarrow_type('uint112'), indexed=False, description='')
class UniswapV2PairTransferEventParameters():
    from_ = EventParameter(name='from', abi_type='address', pyarrow_type=get_pyarrow_type('address'), indexed=True, description='')
    to = EventParameter(name='to', abi_type='address', pyarrow_type=get_pyarrow_type('address'), indexed=True, description='')
    value = EventParameter(name='value', abi_type='uint256', pyarrow_type=get_pyarrow_type('uint256'), indexed=False, description='')

class EventParameterRegistry:
    Dai = Dai()
    WETH9 = WETH9()
    UniswapV2Factory = UniswapV2Factory()
    UniswapV2Pair = UniswapV2Pair()
    Dai.ApprovalEventParameters = DaiApprovalEventParameters()
    Dai.LogNoteEventParameters = DaiLogNoteEventParameters()
    Dai.TransferEventParameters = DaiTransferEventParameters()
    WETH9.ApprovalEventParameters = WETH9ApprovalEventParameters()
    WETH9.TransferEventParameters = WETH9TransferEventParameters()
    WETH9.DepositEventParameters = WETH9DepositEventParameters()
    WETH9.WithdrawalEventParameters = WETH9WithdrawalEventParameters()
    UniswapV2Factory.PairCreatedEventParameters = UniswapV2FactoryPairCreatedEventParameters()
    UniswapV2Pair.ApprovalEventParameters = UniswapV2PairApprovalEventParameters()
    UniswapV2Pair.BurnEventParameters = UniswapV2PairBurnEventParameters()
    UniswapV2Pair.MintEventParameters = UniswapV2PairMintEventParameters()
    UniswapV2Pair.SwapEventParameters = UniswapV2PairSwapEventParameters()
    UniswapV2Pair.SyncEventParameters = UniswapV2PairSyncEventParameters()
    UniswapV2Pair.TransferEventParameters = UniswapV2PairTransferEventParameters()