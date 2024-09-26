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
        name="Approval",
        contract=BaseContract(name="Dai", address=EVMAddress("0x6B175474E89094C44Da98b954EedeAC495271d0F"), chain=Chain.ETHEREUM),
        signature="Approval(address indexed src, address indexed guy, uint256 wad)",
        parameters=EventParameterRegistry.Dai.ApprovalEventParameters,
        description="Event emitted when an address approves another address to spend their DAI"
    )
    LogNote = RegisteredEvent(
        name="LogNote",
        contract=BaseContract(name="Dai", address=EVMAddress("0x6B175474E89094C44Da98b954EedeAC495271d0F"), chain=Chain.ETHEREUM),
        signature="LogNote(bytes4 indexed sig, address indexed usr, bytes32 indexed arg1, bytes32 indexed arg2, bytes data)",
        parameters=EventParameterRegistry.Dai.LogNoteEventParameters,
        description="idk"
    )
    Transfer = RegisteredEvent(
        name="Transfer",
        contract=BaseContract(name="Dai", address=EVMAddress("0x6B175474E89094C44Da98b954EedeAC495271d0F"), chain=Chain.ETHEREUM),
        signature="Transfer(address indexed src, address indexed dst, uint256 wad)",
        parameters=EventParameterRegistry.Dai.TransferEventParameters,
        description="Emitted when DAI is transferred across addresses"
    )
class LUSDTokenEvents:
    Approval = RegisteredEvent(
        name="Approval",
        contract=BaseContract(name="LUSDToken", address=EVMAddress("0x5f98805A4E8be255a32880FDeC7F6728C6568bA0"), chain=Chain.ETHEREUM),
        signature="Approval(address indexed owner, address indexed spender, uint256 value)",
        parameters=EventParameterRegistry.LUSDToken.ApprovalEventParameters,
        description="Emitted when user approves another address to spend LUSD"
    )
    BorrowerOperationsAddressChanged = RegisteredEvent(
        name="BorrowerOperationsAddressChanged",
        contract=BaseContract(name="LUSDToken", address=EVMAddress("0x5f98805A4E8be255a32880FDeC7F6728C6568bA0"), chain=Chain.ETHEREUM),
        signature="BorrowerOperationsAddressChanged(address _newBorrowerOperationsAddress)",
        parameters=EventParameterRegistry.LUSDToken.BorrowerOperationsAddressChangedEventParameters,
        description="Admin function"
    )
    LUSDTokenBalanceUpdated = RegisteredEvent(
        name="LUSDTokenBalanceUpdated",
        contract=BaseContract(name="LUSDToken", address=EVMAddress("0x5f98805A4E8be255a32880FDeC7F6728C6568bA0"), chain=Chain.ETHEREUM),
        signature="LUSDTokenBalanceUpdated(address _user, uint256 _amount)",
        parameters=EventParameterRegistry.LUSDToken.LUSDTokenBalanceUpdatedEventParameters,
        description="Emitted when an LUSD user's balance changes"
    )
    StabilityPoolAddressChanged = RegisteredEvent(
        name="StabilityPoolAddressChanged",
        contract=BaseContract(name="LUSDToken", address=EVMAddress("0x5f98805A4E8be255a32880FDeC7F6728C6568bA0"), chain=Chain.ETHEREUM),
        signature="StabilityPoolAddressChanged(address _newStabilityPoolAddress)",
        parameters=EventParameterRegistry.LUSDToken.StabilityPoolAddressChangedEventParameters,
        description="Admin function"
    )
    Transfer = RegisteredEvent(
        name="Transfer",
        contract=BaseContract(name="LUSDToken", address=EVMAddress("0x5f98805A4E8be255a32880FDeC7F6728C6568bA0"), chain=Chain.ETHEREUM),
        signature="Transfer(address indexed from, address indexed to, uint256 value)",
        parameters=EventParameterRegistry.LUSDToken.TransferEventParameters,
        description="Transfers of LUSD from one address to another"
    )
    TroveManagerAddressChanged = RegisteredEvent(
        name="TroveManagerAddressChanged",
        contract=BaseContract(name="LUSDToken", address=EVMAddress("0x5f98805A4E8be255a32880FDeC7F6728C6568bA0"), chain=Chain.ETHEREUM),
        signature="TroveManagerAddressChanged(address _troveManagerAddress)",
        parameters=EventParameterRegistry.LUSDToken.TroveManagerAddressChangedEventParameters,
        description="Admin function"
    )
class WETH9Events:
    Approval = RegisteredEvent(
        name="Approval",
        contract=BaseContract(name="WETH9", address=EVMAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"), chain=Chain.ETHEREUM),
        signature="Approval(address indexed src, address indexed guy, uint256 wad)",
        parameters=EventParameterRegistry.WETH9.ApprovalEventParameters,
        description="Approval for WETH transfers"
    )
    Transfer = RegisteredEvent(
        name="Transfer",
        contract=BaseContract(name="WETH9", address=EVMAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"), chain=Chain.ETHEREUM),
        signature="Transfer(address indexed src, address indexed dst, uint256 wad)",
        parameters=EventParameterRegistry.WETH9.TransferEventParameters,
        description="Transfer of WETH"
    )
    Deposit = RegisteredEvent(
        name="Deposit",
        contract=BaseContract(name="WETH9", address=EVMAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"), chain=Chain.ETHEREUM),
        signature="Deposit(address indexed dst, uint256 wad)",
        parameters=EventParameterRegistry.WETH9.DepositEventParameters,
        description="Deposit of WETH"
    )
    Withdrawal = RegisteredEvent(
        name="Withdrawal",
        contract=BaseContract(name="WETH9", address=EVMAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"), chain=Chain.ETHEREUM),
        signature="Withdrawal(address indexed src, uint256 wad)",
        parameters=EventParameterRegistry.WETH9.WithdrawalEventParameters,
        description="Withdrawal of WETH"
    )

class EventRegistry:
    Dai = DaiEvents()
    LUSDToken = LUSDTokenEvents()
    WETH9 = WETH9Events()