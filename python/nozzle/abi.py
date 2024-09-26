import json
import requests
from pathlib import Path
from typing import List, Dict, Optional
from .chains import Chain
from .event import Event, EventParameter
from .base_contract import BaseContract
from .event import EventParameters
from .util import get_pyarrow_type


class ABI:
    @staticmethod
    def fetch(contract_address: str, chain: Chain, contract_name: str, force_refresh: bool = False) -> dict:
        """
        Fetch the ABI for a given contract address and save it to a JSON file.
        
        Args:
        contract_address (str): The Ethereum contract address.
        chain (Chain): The blockchain network.
        contract_name (str): The name of the contract.

        Returns:
        dict: The contract ABI as a dictionary.

        Raises:
        ValueError: If there's an error fetching the ABI.
        """
        abi_file = Path('abi') / chain.name / contract_name / f"{contract_address}.json"
        
        if abi_file.exists() and not force_refresh:
            print(f"Loading ABI from local file: {abi_file}")
            with open(abi_file, 'r') as f:
                return json.load(f)

        # If not found locally, attempt to fetch from BlockScout
        try:
            print(f"Fetching ABI from BlockScout: {chain.contract_api_url}{contract_address}")
            url = f"{chain.contract_api_url}{contract_address}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            if 'abi' in data:
                return data['abi']
            else:
                raise ValueError(f"Contract at {contract_address} is not verified or ABI not available.")
        
        except (requests.RequestException, ValueError) as e:
            raise ValueError(f"Error fetching ABI: {str(e)}")

    @staticmethod
    def _save_abi_to_file(abi: dict, chain: Chain, contract_name: str, contract_address: str):
        abi_dir = Path('abi')
        abi_dir.mkdir(exist_ok=True)
        file_path = abi_dir / chain.name / contract_name / f"{contract_address}.json"
        with open(file_path, 'w') as f:
            json.dump(abi, f, indent=2)
        print(f"ABI saved to {file_path}")

    @staticmethod
    def _load_local_abi(file_path: str) -> dict:
        with open(file_path, 'r') as f:
            abi = json.load(f)
        print(f"ABI loaded from local file: {file_path}")
        return abi

    @staticmethod
    @staticmethod
    def extract_events(abi: List[Dict], contract: BaseContract) -> Dict[str, Event]:
        """
        Extract events from the ABI.

        Args:
        abi (List[Dict]): The contract ABI.
        contract (BaseContract): The contract object.

        Returns:
        Dict[str, Event]: A dictionary of event names to Event objects.
        """
        events = {}
        for item in abi:
            if item['type'] == 'event':
                event_name = item['name']
                event_signature = ABI.create_event_signature(item)
                
                # Create the Event object
                event = Event(
                    name=event_name,
                    signature=event_signature,
                    contract=contract,
                    abi_event=item,
                    parameters=EventParameters()
                )

                # Create EventParameter objects for each input
                for input_param in item['inputs']:
                    pyarrow_type = get_pyarrow_type(input_param['type'])
                    param = EventParameter(
                        name=input_param['name'],
                        abi_type=input_param['type'],
                        pyarrow_type=pyarrow_type,
                        indexed=input_param['indexed'],
                        description='',  # Description will be added later when prompted
                    )
                    setattr(event.parameters, input_param['name'], param)

                events[event_name] = event
        return events

    @staticmethod
    def get_event_signatures(abi: List[Dict]) -> Dict[str, str]:
        """
        Get event signatures from the ABI.

        Args:
        abi (List[Dict]): The contract ABI.

        Returns:
        Dict[str, str]: A dictionary of event names to their signatures.
        """
        return {item['name']: ABI.create_event_signature(item) for item in abi if item['type'] == 'event'}

    @staticmethod
    def create_event_signature(event: Dict) -> str:
        """
        Create an event signature from an event ABI item.

        Args:
        event (Dict): The event item from the ABI.

        Returns:
        str: The event signature.
        """
        event_name = event['name']
            
        # Extract the inputs and construct the input part of the signature
        inputs = event['inputs']
        inputs_signature = ', '.join(
            f"{input['type']} {'indexed ' if input['indexed'] else ''}{input['name']}" for input in inputs
        )
        # Construct the full signature
        signature = f"{event_name}({inputs_signature})"
        return signature
    
    @staticmethod
    def _get_storage_path(chain: Chain, contract_name: str, contract_address: str) -> Path:
        return Path('abi') / chain.name / contract_name / f"{contract_address}.json"
