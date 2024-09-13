from eth_utils import is_address, to_checksum_address

class EVMAddress:
    def __init__(self, address: str):
        if not is_address(address):
            raise ValueError(f"Invalid EVM address: {address}")
        self._address = to_checksum_address(address)

    @property
    def address(self) -> str:
        return self._address

    def __str__(self) -> str:
        return self._address

    def __repr__(self) -> str:
        return f"EVMAddress({self._address})"

    def __eq__(self, other) -> bool:
        if isinstance(other, EVMAddress):
            return self._address == other._address
        return False

    def __hash__(self) -> int:
        return hash(self._address)