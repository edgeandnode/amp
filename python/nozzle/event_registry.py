from .base_contract import BaseContract
from .contract import Contract, ContractEvents
from .contracts import Contracts
from .chains import Chain
from .address import EVMAddress
from .event import Event, EventParameter
from dataclasses import dataclass
from .registered_event import RegisteredEvent
from .registered_contract import RegisteredContract


@dataclass
class EventRegistry():

    class WETH9Events(ContractEvents):
        Approval = RegisteredEvent(name='Approval', contract=Contracts.generate_base_contract(name='WETH9', address='0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2', chain=Chain.ETHEREUM), signature='Approval(address indexed src, address indexed guy, uint256 wad)', parameters=[EventParameter(name='src', type='address', indexed=True), EventParameter(name='guy', type='address', indexed=True), EventParameter(name='wad', type='uint256', indexed=False)])
        Transfer = RegisteredEvent(name='Transfer', contract=Contracts.generate_base_contract(name='WETH9', address='0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2', chain=Chain.ETHEREUM), signature='Transfer(address indexed src, address indexed dst, uint256 wad)', parameters=[EventParameter(name='src', type='address', indexed=True), EventParameter(name='dst', type='address', indexed=True), EventParameter(name='wad', type='uint256', indexed=False)])
        Deposit = RegisteredEvent(name='Deposit', contract=Contracts.generate_base_contract(name='WETH9', address='0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2', chain=Chain.ETHEREUM), signature='Deposit(address indexed dst, uint256 wad)', parameters=[EventParameter(name='dst', type='address', indexed=True), EventParameter(name='wad', type='uint256', indexed=False)])
        Withdrawal = RegisteredEvent(name='Withdrawal', contract=Contracts.generate_base_contract(name='WETH9', address='0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2', chain=Chain.ETHEREUM), signature='Withdrawal(address indexed src, uint256 wad)', parameters=[EventParameter(name='src', type='address', indexed=True), EventParameter(name='wad', type='uint256', indexed=False)])

    WETH9 = WETH9Events()

    class DaiEvents(ContractEvents):
        Approval = RegisteredEvent(name='Approval', contract=Contracts.generate_base_contract(name='dai', address='0x6B175474E89094C44Da98b954EedeAC495271d0F', chain=Chain.ETHEREUM), signature='Approval(address indexed src, address indexed guy, uint256 wad)', parameters=[EventParameter(name='src', type='address', indexed=True), EventParameter(name='guy', type='address', indexed=True), EventParameter(name='wad', type='uint256', indexed=False)])
        LogNote = RegisteredEvent(name='LogNote', contract=Contracts.generate_base_contract(name='dai', address='0x6B175474E89094C44Da98b954EedeAC495271d0F', chain=Chain.ETHEREUM), signature='LogNote(bytes4 indexed sig, address indexed usr, bytes32 indexed arg1, bytes32 indexed arg2, bytes data)', parameters=[EventParameter(name='sig', type='bytes4', indexed=True), EventParameter(name='usr', type='address', indexed=True), EventParameter(name='arg1', type='bytes32', indexed=True), EventParameter(name='arg2', type='bytes32', indexed=True), EventParameter(name='data', type='bytes', indexed=False)])
        Transfer = RegisteredEvent(name='Transfer', contract=Contracts.generate_base_contract(name='dai', address='0x6B175474E89094C44Da98b954EedeAC495271d0F', chain=Chain.ETHEREUM), signature='Transfer(address indexed src, address indexed dst, uint256 wad)', parameters=[EventParameter(name='src', type='address', indexed=True), EventParameter(name='dst', type='address', indexed=True), EventParameter(name='wad', type='uint256', indexed=False)])

    dai = DaiEvents()
    # New event classes will be added here

event_registry = EventRegistry()
