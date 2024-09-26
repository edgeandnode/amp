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
class LUSDToken:
    pass
class WETH9:
    pass

# If parameter is a reserved word in python, we need to modify it by adding an underscore


class DaiApprovalEventParameters():
    src = EventParameter(name="src", abi_type="address", pyarrow_type=get_pyarrow_type("address"), indexed=True, description="")
    guy = EventParameter(name="guy", abi_type="address", pyarrow_type=get_pyarrow_type("address"), indexed=True, description="")
    wad = EventParameter(name="wad", abi_type="uint256", pyarrow_type=get_pyarrow_type("uint256"), indexed=False, description="")
class DaiLogNoteEventParameters():
    sig = EventParameter(name="sig", abi_type="bytes4", pyarrow_type=get_pyarrow_type("bytes4"), indexed=True, description="")
    usr = EventParameter(name="usr", abi_type="address", pyarrow_type=get_pyarrow_type("address"), indexed=True, description="")
    arg1 = EventParameter(name="arg1", abi_type="bytes32", pyarrow_type=get_pyarrow_type("bytes32"), indexed=True, description="")
    arg2 = EventParameter(name="arg2", abi_type="bytes32", pyarrow_type=get_pyarrow_type("bytes32"), indexed=True, description="")
    data = EventParameter(name="data", abi_type="bytes", pyarrow_type=get_pyarrow_type("bytes"), indexed=False, description="")
class DaiTransferEventParameters():
    src = EventParameter(name="src", abi_type="address", pyarrow_type=get_pyarrow_type("address"), indexed=True, description="")
    dst = EventParameter(name="dst", abi_type="address", pyarrow_type=get_pyarrow_type("address"), indexed=True, description="")
    wad = EventParameter(name="wad", abi_type="uint256", pyarrow_type=get_pyarrow_type("uint256"), indexed=False, description="")
class LUSDTokenApprovalEventParameters():
    owner = EventParameter(name="owner", abi_type="address", pyarrow_type=get_pyarrow_type("address"), indexed=True, description="The LUSD owner approving spending")
    spender = EventParameter(name="spender", abi_type="address", pyarrow_type=get_pyarrow_type("address"), indexed=True, description="The address being approved to spend LUSD")
    value_ = EventParameter(name="value", abi_type="uint256", pyarrow_type=get_pyarrow_type("uint256"), indexed=False, description="The amount of LUSD approved")
class LUSDTokenBorrowerOperationsAddressChangedEventParameters():
    _newBorrowerOperationsAddress = EventParameter(name="_newBorrowerOperationsAddress", abi_type="address", pyarrow_type=get_pyarrow_type("address"), indexed=False, description="Admin function")
class LUSDTokenLUSDTokenBalanceUpdatedEventParameters():
    _user = EventParameter(name="_user", abi_type="address", pyarrow_type=get_pyarrow_type("address"), indexed=False, description="The LUSD user")
    _amount = EventParameter(name="_amount", abi_type="uint256", pyarrow_type=get_pyarrow_type("uint256"), indexed=False, description="The amount of the LUSD owner's balance")
class LUSDTokenStabilityPoolAddressChangedEventParameters():
    _newStabilityPoolAddress = EventParameter(name="_newStabilityPoolAddress", abi_type="address", pyarrow_type=get_pyarrow_type("address"), indexed=False, description="Admin function")
class LUSDTokenTransferEventParameters():
    from_ = EventParameter(name="from", abi_type="address", pyarrow_type=get_pyarrow_type("address"), indexed=True, description="The address sending the LUSD")
    to_ = EventParameter(name="to", abi_type="address", pyarrow_type=get_pyarrow_type("address"), indexed=True, description="The recipient address of the LUSD")
    value_ = EventParameter(name="value", abi_type="uint256", pyarrow_type=get_pyarrow_type("uint256"), indexed=False, description="The amount of LUSD transferred")
    from_ = EventParameter(name="from", abi_type="address", pyarrow_type=get_pyarrow_type("address"), indexed=True, description="The address sending the LUSD")
class LUSDTokenTroveManagerAddressChangedEventParameters():
    _troveManagerAddress = EventParameter(name="_troveManagerAddress", abi_type="address", pyarrow_type=get_pyarrow_type("address"), indexed=False, description="Admin function")
class WETH9ApprovalEventParameters():
    src = EventParameter(name="src", abi_type="address", pyarrow_type=get_pyarrow_type("address"), indexed=True, description="")
    guy = EventParameter(name="guy", abi_type="address", pyarrow_type=get_pyarrow_type("address"), indexed=True, description="")
    wad = EventParameter(name="wad", abi_type="uint256", pyarrow_type=get_pyarrow_type("uint256"), indexed=False, description="")
class WETH9TransferEventParameters():
    src = EventParameter(name="src", abi_type="address", pyarrow_type=get_pyarrow_type("address"), indexed=True, description="")
    dst = EventParameter(name="dst", abi_type="address", pyarrow_type=get_pyarrow_type("address"), indexed=True, description="")
    wad = EventParameter(name="wad", abi_type="uint256", pyarrow_type=get_pyarrow_type("uint256"), indexed=False, description="")
class WETH9DepositEventParameters():
    dst = EventParameter(name="dst", abi_type="address", pyarrow_type=get_pyarrow_type("address"), indexed=True, description="")
    wad = EventParameter(name="wad", abi_type="uint256", pyarrow_type=get_pyarrow_type("uint256"), indexed=False, description="")
class WETH9WithdrawalEventParameters():
    src = EventParameter(name="src", abi_type="address", pyarrow_type=get_pyarrow_type("address"), indexed=True, description="")
    wad = EventParameter(name="wad", abi_type="uint256", pyarrow_type=get_pyarrow_type("uint256"), indexed=False, description="")

class EventParameterRegistry:
    Dai = Dai()
    LUSDToken = LUSDToken()
    WETH9 = WETH9()
    Dai.ApprovalEventParameters = DaiApprovalEventParameters()
    Dai.LogNoteEventParameters = DaiLogNoteEventParameters()
    Dai.TransferEventParameters = DaiTransferEventParameters()
    LUSDToken.ApprovalEventParameters = LUSDTokenApprovalEventParameters()
    LUSDToken.BorrowerOperationsAddressChangedEventParameters = LUSDTokenBorrowerOperationsAddressChangedEventParameters()
    LUSDToken.LUSDTokenBalanceUpdatedEventParameters = LUSDTokenLUSDTokenBalanceUpdatedEventParameters()
    LUSDToken.StabilityPoolAddressChangedEventParameters = LUSDTokenStabilityPoolAddressChangedEventParameters()
    LUSDToken.TransferEventParameters = LUSDTokenTransferEventParameters()
    LUSDToken.TroveManagerAddressChangedEventParameters = LUSDTokenTroveManagerAddressChangedEventParameters()
    WETH9.ApprovalEventParameters = WETH9ApprovalEventParameters()
    WETH9.TransferEventParameters = WETH9TransferEventParameters()
    WETH9.DepositEventParameters = WETH9DepositEventParameters()
    WETH9.WithdrawalEventParameters = WETH9WithdrawalEventParameters()