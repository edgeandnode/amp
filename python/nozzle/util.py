import json
import pyarrow as pa
import re

# Convert bytes columns to hex
def to_hex(val):
    return '0x' + val.hex() if isinstance(val, bytes) else val

# Read a JSON ABI definition from a file and parse it into a form that's
# easier to deal with and interpolate into queries
class Abi:
    def __init__(self, path):
        f = open(path)
        data = json.load(f)
        self.events = {}
        for entry in data:
            if entry["type"] == "event":
                self.events[entry["name"]] = Event(entry)

# An event from a JSON ABI
class Event:
    def __init__(self, data):
        self.name = data["name"]
        self.inputs = []
        self.names = []
        for input in data["inputs"]:
            param = input["type"]
            self.names.append(input["name"])
            if input["indexed"]:
                param += " indexed"
            param += " " + input["name"]
            self.inputs.append(param)

    def signature(self):
        sig = self.name + "(" + ",".join(self.inputs) + ")"
        return sig



def get_pyarrow_type(solidity_type: str) -> pa.DataType:
    if solidity_type.startswith(('uint', 'int')):
        bits = int(solidity_type[4:] if solidity_type.startswith('uint') else solidity_type[3:])
        if bits <= 64:
            return pa.int64()
        else:
            # For integers larger than 64 bits, use decimal
            # Precision is ceil(bits / log2(10)) and scale is 0 for integers
            precision = min(-(-bits // 3), 76)  # Cap at 76 for decimal256
            if precision <= 38:
                return pa.decimal128(precision, scale=0)
            else:
                return pa.decimal256(precision, scale=0)
    elif solidity_type == 'address':
        return pa.string()
    elif solidity_type == 'bool':
        return pa.bool_()
    elif solidity_type.startswith('bytes'):
        if solidity_type == 'bytes':
            return pa.binary()
        else:
            size = int(solidity_type[5:])
            return pa.binary(size)
    elif solidity_type == 'string':
        return pa.string()
    elif solidity_type.startswith('fixed') or solidity_type.startswith('ufixed'):
        match = re.match(r'(u?)fixed(\d+)x(\d+)', solidity_type)
        if match:
            _, bits, decimals = match.groups()
            bits, decimals = int(bits), int(decimals)
            precision = min(-(-bits // 3), 76)  # Cap at 76 for decimal256
            if precision <= 38:
                return pa.decimal128(precision, scale=min(decimals, precision))
            else:
                return pa.decimal256(precision, scale=min(decimals, precision))
    else:
        return pa.string()  # Default to string for unknown types