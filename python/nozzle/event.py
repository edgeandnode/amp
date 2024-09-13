from enum import Enum
from dataclasses import dataclass
from typing import ClassVar, Dict, List
from typing import List
from .base_contract import BaseContract

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
    contract: BaseContract
