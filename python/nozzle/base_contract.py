from dataclasses import dataclass
from .address import EVMAddress
from .chains import Chain

@dataclass
class BaseContract:
    name: str
    address: EVMAddress
    chain: Chain
