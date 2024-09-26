from dataclasses import dataclass, field
from typing import Dict
from .base_contract import BaseContract
from .event_parameter_registry import EventParameter
from typing import Any


@dataclass
class EventParameters:
    pass

@dataclass
class Event:
    name: str
    signature: str
    contract: BaseContract
    abi_event: Dict = field(default_factory=dict)
    parameters: Any = None
    description: str = ''

    def __post_init__(self):
        if self.abi_event and not self.parameters:
            for input_param in self.abi_event.get('inputs', []):
                param = EventParameter(
                    name=input_param['name'],
                    type=input_param['type'],
                    indexed=input_param['indexed'],
                    description=''
                )
                setattr(self.parameters, input_param['name'], param)