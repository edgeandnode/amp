from dataclasses import dataclass, field, asdict
from typing import Optional, Dict
from pathlib import Path
import json
from .base_contract import BaseContract
from .event import Event
from .chains import Chain
from .event_parameter_registry import EventParameter
from .abi import ABI
from .registry_manager import RegistryManager
from .util import get_pyarrow_type

class ContractEvents():
    pass

@dataclass
class Contract(BaseContract):
    events: Optional[ContractEvents] = None
    _abi: Dict = field(default_factory=dict)
    force_refresh: Optional[bool] = False

    def __post_init__(self):
        if not self._abi:
            self.abi = ABI.fetch(self.address, self.chain, self.name, self.force_refresh)
        if not self.events:
            self.events = ContractEvents()
            for event in self._abi:
                if event['type'] == 'event':
                    event_obj = Event(name=event['name'], contract=self, signature=ABI.create_event_signature(event))
                    self.add_event(event_obj)

    def add_event(self, event: Event):
        if self.events is None:
            self.events = ContractEvents()
        
        if hasattr(self.events, event.name):
            print(f"Event {event.name} already exists")
        else:
            setattr(self.events, event.name, event)
            
            abi_event = next((e for e in self._abi if e['type'] == 'event' and e['name'] == event.name), None)
            if abi_event:
                event.abi_event = abi_event
                
                event_params = {}
                for input_param in abi_event['inputs']:
                    abi_type = input_param['type']
                    pyarrow_type = get_pyarrow_type(abi_type)
                    param = EventParameter(
                        name=input_param['name'],
                        abi_type=abi_type,
                        pyarrow_type=pyarrow_type,
                        indexed=input_param['indexed'],
                        description=''
                    )
                    event_params[input_param['name']] = param
                
                event.parameters = type(f"{self.name}{event.name}Parameters", (), event_params)()
                
                serializable_event_params = {
                    name: {
                        "name": param.name,
                        "type": param.abi_type,
                        "pyarrow_type": str(param.pyarrow_type),
                        "indexed": param.indexed,
                        "description": param.description
                    }
                    for name, param in event_params.items()
                }
                
                RegistryManager.update_registry("event_parameter", {
                    f"{self.name}.{event.name}": serializable_event_params
                })
    
    @property
    def abi(self) -> Dict:
        # if no abi, fetch one
        if not self._abi:
            self._fetch_abi()
        return self._abi

    @abi.setter
    def abi(self, value: Dict):
        self._abi = value
        # if abi file doesn't exist, write it
        if not (Path('abi') / self.chain.name / self.name / f"{str(self.address)}.json").exists() or self.force_refresh:
            self._write_abi()

    def _write_abi(self):
        abi_dir = Path('abi') / self.chain.name / self.name
        abi_dir.mkdir(parents=True, exist_ok=True)
        abi_file = abi_dir / f"{str(self.address)}.json"
        
        with open(abi_file, 'w') as f:
            json.dump(self._abi, f, indent=2)
        print(f"ABI written to {abi_file}")

    @classmethod
    def load(cls, name: str, address: str, chain: Chain):
        abi_file = Path('abi') / chain.value / name / f"{address}.json"
        if not abi_file.exists():
            raise FileNotFoundError(f"ABI file not found: {abi_file}")
        
        with open(abi_file, 'r') as f:
            abi = json.load(f)
        
        contract = cls(name=name, address=address, chain=chain)
        contract.abi = abi
        return contract
