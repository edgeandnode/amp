import json
from nozzle.contract_registry import ContractRegistry
from nozzle.event_registry import EventRegistry

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

def get_all_event_strings():
    """ Get all even strings in the format of contract_name.event_name """
    event_strings = []
    for contract in dir(EventRegistry):
        if EventRegistry.__dict__.get(contract).__class__.__qualname__.endswith("Events"):
            for event in dir(EventRegistry.__dict__.get(contract)):
                if not event.startswith("_"):
                    event_strings.append(f"{EventRegistry.__dict__.get(contract).__getattribute__(event).contract.name}.{EventRegistry.__dict__.get(contract).__getattribute__(event).name}")
                
    return event_strings