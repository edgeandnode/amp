from pyarrow import flight
from google.protobuf.any_pb2 import Any

from . import FlightSql_pb2

class Client:
    def __init__(self, url):
        self.conn =  flight.connect(url)

    # If `read_all` is `True`, returns a `pyarrow.Table` with the complete result set. This is a
    # convenience for small result sets that don't need to be streamed.
    # 
    # If `read_all` is `False`, returns a generator of `pyarrow.RecordBatch`. This is suitable for
    # streaming larger result sets.
    def get_sql(self, query, read_all=False):
        # Create a CommandStatementQuery message
        command_query = FlightSql_pb2.CommandStatementQuery()
        command_query.query = query


        # Wrap the CommandStatementQuery in an Any type
        any_command = Any()
        any_command.Pack(command_query)
        cmd = any_command.SerializeToString()


        flight_descriptor = flight.FlightDescriptor.for_command(cmd)
        info = self.conn.get_flight_info(flight_descriptor)
        reader = self.conn.do_get(info.endpoints[0].ticket)

        if read_all:
            return reader.read_all()
        else:
            return self._batch_generator(reader)

    def _batch_generator(self, reader):
        while True:
            try:
                chunk = reader.read_chunk()
                yield chunk.data
            except StopIteration:
                break



import os.path
import os
from decimal import Decimal
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pandas as pd
import polars as pl
import time
import hashlib
from eth_utils import to_hex
from eth_utils.crypto import keccak
from eth_utils import keccak
#from google.cloud import storage, bigquery

import binascii
import decimal
import base58
import json
import requests
from collections import defaultdict


def process_and_save_dataframe(final_df: pl.DataFrame, output_directory: str, file_name: str = 'final_df.parquet'):
    # Ensure the output directory exists and is writable
    if not os.access(output_directory, os.W_OK):
        raise OSError(f"Cannot write to the specified directory: {output_directory}")

    # Convert the defaultdict to a regular dictionary if needed
    if isinstance(final_df, defaultdict):
        indexer_dict = {key: dict(value) for key, value in final_df.items()}
        final_df = pl.DataFrame(indexer_dict)

    # Ensure the date column is in datetime format
    if 'date' in final_df.columns:
        final_df = final_df.with_column(pl.col('date').str.strptime(pl.Date, fmt='%Y-%m-%d'))

    # Define schema using pyarrow to ensure compatibility
    schema = pa.schema([
        ('date', pa.date32()),
        ('id', pa.string()),
        ('value', pa.float64())  # Define the decimal type to match BigQuery's NUMERIC type
    ])

    # Convert pandas DataFrame to pyarrow Table
    table = pa.Table.from_pandas(final_df.to_pandas(), schema=schema)

    # Construct the full file path
    file_path = os.path.join(output_directory, file_name)

    # Save the Table to a Parquet file
    pq.write_table(table, file_path)

    print(f"File saved successfully at {file_path}")
    return table

def generate_signatures_from_abi(file_path, specific_event_name=None):
    # Read the ABI file
    with open(file_path, 'r') as file:
        abi = json.load(file)
    
    # Initialize a list to store signatures
    signatures = []

    # Iterate over each item in the ABI
    for item in abi:
        # Check if the item is an event
        if item['type'] == 'event':
            # Extract the event name
            event_name = item['name']
            
            # Extract the inputs and construct the input part of the signature
            inputs = item['inputs']
            inputs_signature = ', '.join(
                f"{input['type']} {'indexed ' if input['indexed'] else ''}{input['name']}" for input in inputs
            )
            
            # Construct the full signature
            signature = f"{event_name}({inputs_signature})"
            
            # If specific_event_name is given, check if it matches the current event name
            if specific_event_name:
                if event_name == specific_event_name:
                    signatures.append(signature)
            else:
                # Add the signature to the list
                signatures.append(signature)
    print(signatures)
    return signatures

def generate_signature(json_string):
    # Parse the JSON string
    data = json.loads(json_string)
    
    # Extract the event name
    event_name = data['name']
    
    # Extract the inputs and construct the input part of the signature
    inputs = data['inputs']
    inputs_signature = ', '.join(f"{input['type']} {'indexed ' if input['indexed'] else ''}{input['name']}" for input in inputs)
    
    # Construct the full signature
    signature = f"{event_name}({inputs_signature})"
    
    return signature

def elapsed(start):
    return round(time.time() - start, 4) 

def to_hex(val):
    return '0x' + val.hex() if isinstance(val, bytes) else val

def process_query(client, query):
    print(query)
    start = time.time()

    result_stream = client.get_sql(query)
    print('time to establish stream: ', elapsed(start), 's')

    total_events = 0
    batches = []
    try:
        batch = next(result_stream)
        print('time for first batch ', elapsed(start), 's')
        total_events += batch.num_rows
        batches.append(batch)

        print('The type of batch is ', type(batch))
        
        batch_start = time.time()

        for batch in result_stream:
            total_events += batch.num_rows
            print('received batch of ', batch.num_rows, ' rows in ', elapsed(batch_start), 's')
            batch_start = time.time()
            batches.append(batch)

        table = pa.Table.from_batches(batches)

        # Convert fixedsizebinary to string
        table = pa.Table.from_pandas(table.to_pandas().map(to_hex))

        # Pretty print ten rows from the pyarrow table
        print('Columns: ', table.column_names)
        print('Table: ', table.to_string(preview_cols=5))
        return table

    except StopIteration:
        print("No more batches available in the result stream.")
        return pa.Table.from_batches(batches)

def convert_bigint_subgraph_id_to_base58(bigint_representation: int) -> str:
    # Convert the bigint to a hex string
    hex_string = hex(bigint_representation)
    
    # Check if the hex string length is even
    if len(hex_string) % 2 != 0:
        # Log a warning if the hex string length is not even and pad it to even length
        print(f'Warning: Hex string not even, hex: {hex_string}, original: {bigint_representation}. Padding it to even length')
        hex_string = '0x0' + hex_string[2:]

    # Convert the hex string to a byte array
    bytes_data = bytes.fromhex(hex_string[2:])
    
    # Convert the byte array to a Base58 string
    base58_string = base58.b58encode(bytes_data).decode('utf-8')
    
    return base58_string

def convert_to_base58(subgraph_metadata_hex):
    print(subgraph_metadata_hex)
    subgraph_metadata_hex = subgraph_metadata_hex[2:]
    # Convert hex string to bytes
    subgraph_metadata_bytes = bytes.fromhex(subgraph_metadata_hex)
    
    # Add the prefix using add_qm function
    prefixed_bytes = add_qm(subgraph_metadata_bytes)
    
    # Convert bytes to Base58
    base58_string = base58.b58encode(prefixed_bytes).decode('utf-8')
    
    return base58_string

def add_qm(byte_array):
    # Create an output bytearray with a size of 34
    out = bytearray(34)
    
    # Set the first two bytes
    out[0] = 0x12
    out[1] = 0x20
    
    # Copy the input byte array into the output starting from the third byte
    for i in range(32):
        out[i + 2] = byte_array[i]
    
    return bytes(out)

def convert_to_base58(subgraph_metadata_hex):
    print(subgraph_metadata_hex)
    subgraph_metadata_hex = subgraph_metadata_hex[2:]
    # Convert hex string to bytes
    subgraph_metadata_bytes = bytes.fromhex(subgraph_metadata_hex)
    
    # Add the prefix using add_qm function
    prefixed_bytes = add_qm(subgraph_metadata_bytes)
    
    # Convert bytes to Base58
    base58_string = base58.b58encode(prefixed_bytes).decode('utf-8')
    
    return base58_string

def get_subgraph_id(graphAccount, subgraphNumber):
    # Step 1: Remove '0x' prefix if present and convert graphAccount to bytes
    if isinstance(graphAccount, str):
        if graphAccount.startswith('0x'):
            graphAccount = graphAccount[2:]
        graphAccount = bytes.fromhex(graphAccount)
    
    # Step 2: Convert graphAccount to a hexadecimal string
    graphAccountStr = graphAccount.hex()
    
    # Step 3: Convert subgraphNumber to an integer if it's a decimal.Decimal, then to a hexadecimal string
    if isinstance(subgraphNumber, decimal.Decimal):
        subgraphNumber = int(subgraphNumber)
    
    subgraphNumberStr = hex(subgraphNumber)[2:].zfill(64)
    
    # Step 4: Concatenate the two strings
    unhashedSubgraphID = graphAccountStr + subgraphNumberStr
    
    # Step 5: Hash the concatenated string using Keccak256
    hashedId = keccak(bytes.fromhex(unhashedSubgraphID))
    
    # Step 6: Convert the hash to a BigInt
    bigIntRepresentation = int.from_bytes(hashedId, byteorder='big')
    
    return convert_bigint_subgraph_id_to_base58(bigIntRepresentation)

def export_df_to_csv(path, df):
    csv_file_path = path  # Replace with your desired file path
    df.to_csv(csv_file_path, index=False)

def fetch_ipfs_data(url):
    try:
        # Fetch data from IPFS using the provided URL
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.text
    except requests.RequestException as e:
        print(f"Error fetching IPFS data: {e}")
        return None
    
def save_json_file(data, filename):
    try:
        with open(filename, 'w') as file:
            json.dump(data, file, indent=4)
    except Exception as e:
        print(f"Error saving JSON file: {e}")

def json_to_string(value):
    return str(value) if value else ""

def process_subgraph_metadata(json_data):
    display_name= json_data['displayName']
    code_repository = json_data['codeRepository']
    website  = json_data['website']
    return display_name, code_repository, website

def get_ipfs_data(df):
    for ipfs_hash in df['metadata_ipfs_hash']:
        ipfs_url = "https://api.thegraph.com/ipfs/api/v0/cat?arg=" + ipfs_hash
        ipfs_data = fetch_ipfs_data(ipfs_url)
        if ipfs_data:
            json_data = json.loads(ipfs_data)  # Convert the fetched data to a JSON object    
        # Process the subgraph metadata
        df['display_name']= json_data['displayName']
        df['code_repository'] = json_data['codeRepository']
        df['website']  = json_data['website']

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

def load_parquet_to_bigquery(dataset_id, table_id, gcs_uri):
    """Loads a Parquet file from GCS into BigQuery."""
    client = bigquery.Client()

    table_ref = client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
    )

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_ref,
        job_config=job_config
    )

    print(f"Starting job {load_job.job_id}")

    load_job.result()  # Waits for the job to complete.

    print(f"Job finished. Loaded {load_job.output_rows} rows into {dataset_id}:{table_id}")

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