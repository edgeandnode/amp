from dataclasses import dataclass, field
from typing import Dict
from .event import Event
from .base_contract import BaseContract
from .event_parameter_registry import EventParameter

@dataclass
class RegisteredEvent(Event):
    name: str
    contract: BaseContract
    signature: str
    abi_event: Dict = field(default_factory=dict)
    parameters: Dict[str, EventParameter] = field(default_factory=dict)
    description: str = ""

    def __post_init__(self):
        super().__post_init__()
        if self.abi_event:
            for input_param in self.abi_event.get('inputs', []):
                param = EventParameter(
                    name=input_param['name'],
                    type=input_param['type'],
                    indexed=input_param['indexed'],
                    description=''  # This will be filled in later
                )
                self.parameters[input_param['name']] = param
                setattr(self, input_param['name'], param)

    def update_parameter_description(self, param_name: str, description: str):
        if param_name in self.parameters:
            self.parameters[param_name].description = description
            setattr(self, param_name, self.parameters[param_name])
        else:
            raise ValueError(f"Parameter {param_name} not found in event {self.name}")