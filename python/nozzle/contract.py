from dataclasses import dataclass, field
from typing import Optional, Dict
from pathlib import Path
import json
from .base_contract import BaseContract
from .event import Event
from .chains import Chain

class ContractEvents:
    pass

@dataclass
class Contract(BaseContract):
    events: Optional[ContractEvents] = None
    _abi: Dict = field(default_factory=dict)

    def add_event(self, event: Event):
        if self.events is None:
            self.events = ContractEvents()
        
        if hasattr(self.events, event.name):
            raise ValueError(f"Event {event.name} already exists")
        else:
            setattr(self.events, event.name, event)
    
    @property
    def abi(self) -> Dict:
        return self._abi

    @abi.setter
    def abi(self, value: Dict):
        self._abi = value
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
