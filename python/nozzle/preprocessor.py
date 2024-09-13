import os
from typing import List, Dict, Union
from datetime import datetime
import datafusion
from datafusion import col, lit, functions as f
import pyarrow as pa
import pyarrow.parquet as pq
from .event import Event
from .view import View
from .contracts import Contracts

class ExtractHelper:
    BASE_REMOTE_DATA_TABLE = 'ethereum.firehose_logs'
    BASE_LOCAL_DATA_DIR = 'data/'

    def __init__(self, views: List[View], output_dir: str):
        """
        Initialize the ExtractHelper with a list of views and an output directory.

        Args:
        views (List[ViewBase]): A list of view instances to process.
        output_dir (str): The directory where the output Parquet file will be saved.
        """
        self.views = views
        self.output_dir = output_dir
        self.contracts_events = self._aggregate_contracts_events()
        self.range = self._determine_range()

    def _aggregate_contracts_events(self) -> Dict[str, List[str]]:
        """Aggregate all required contracts and events from the views."""
        aggregated = {}
        for view in self.views:
            for contract, events in view.contracts_events.items():
                if contract not in aggregated:
                    aggregated[contract] = set()
                aggregated[contract].update(events)
        return {k: list(v) for k, v in aggregated.items()}

    def _determine_range(self) -> Dict[str, Union[int, datetime]]:
        """Determine the overall range to query based on all views."""
        block_min, block_max = float('inf'), float('-inf')
        time_min, time_max = datetime.max, datetime.min

        for view in self.views:
            if view.range_type == 'block':
                block_min = min(block_min, view.range_value[0])
                block_max = max(block_max, view.range_value[1])
            else:  # time range
                time_min = min(time_min, view.range_value[0])
                time_max = max(time_max, view.range_value[1])

        if block_min < float('inf'):
            return {'type': 'block', 'start': block_min, 'end': block_max}
        else:
            return {'type': 'time', 'start': time_min, 'end': time_max}

    def extract_data(self, firehose_logs_path: str):
        """
        Extract data for all required contracts and events, and save to a Parquet file.

        Args:
        firehose_logs_path (str): Path to the firehose logs Parquet file.
        """
        ctx = datafusion.SessionContext()

        # Register the firehose logs table
        ctx.register_parquet("firehose_logs", firehose_logs_path)

        # Build the query
        query = ctx.table("firehose_logs")
        query = query.filter(self._build_contract_event_filter())

        # Add range filter
        if self.range['type'] == 'block':
            query = query.filter(
                (col("block_number") >= lit(self.range['start'])) &
                (col("block_number") <= lit(self.range['end']))
            )
        else:  # time range
            query = query.filter(
                (col("block_timestamp") >= lit(int(self.range['start'].timestamp()))) &
                (col("block_timestamp") <= lit(int(self.range['end'].timestamp())))
            )

        # Execute the query and get the result as a PyArrow table
        result = query.collect()
        table = pa.Table.from_pandas(result)

        # Write the result to a Parquet file
        output_path = os.path.join(self.output_dir, "extracted_data.parquet")
        pq.write_table(table, output_path)

        print(f"Data extracted and saved to {output_path}")

    def _build_contract_event_filter(self):
        """Build the filter for contracts and events."""
        filters = []
        for contract, events in self.contracts_events.items():
            contract_obj = Contracts.get_contract(contract)
            contract_filter = col("contract_address") == lit(str(contract_obj.address))
            event_signatures = [contract_obj.events[event].signature for event in events]
            event_filter = col("event_signature").isin(event_signatures)
            filters.append(contract_filter & event_filter)
        return f.any(*filters)


# Sample query of base table: 
# usdc_transfer_count_query=f"""select * from eth_firehose.logs l where l.address=arrow_cast(x'A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48', 
#   'FixedSizeBinary(20)') and l.topic0 = evm_topic('{usdc_transfer_sig}') limit 10"""

# Example of decoded query:
# usdc_transfer_count_query=f"""select evm_decode(l.topic1, l.topic2, l.topic3, l.data, '{usdc_transfer_sig}') as dec from eth_firehose.logs l where l.address=arrow_cast(x'A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48', 'FixedSizeBinary(20)') and l.topic0 = evm_topic('{usdc_transfer_sig}') limit 10"""

# Change the above query to cast each element of evm_decode to a separate column where the name of the column is the name of the field in the decoded data
# usdc_transfer_count_query=f"""select dec['from'] as from_address, dec['to'] as to_address, dec['value'] as value from (select evm_decode(l.topic1, l.topic2, l.topic3, l.data, '{usdc_transfer_sig}') as dec from eth_firehose.logs l where l.address=arrow_cast(x'A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48', 'FixedSizeBinary(20)') and l.topic0 = evm_topic('{usdc_transfer_sig}'))"""

# Create a function that takes an event and returns a query that decodes the data for any Event in the ABI
def create_decoded_query(event: Event):
    casted_columns = ''.join([f"dec['{param.name}'] as {param.name.lower()}, " for param in event.parameters])
    return f"""select {casted_columns} from (select evm_decode(l.topic1, l.topic2, l.topic3, l.data, '{event.signature}') as dec from eth_firehose.logs l where l.address=arrow_cast(x'{str(event.contract.address)}', 'FixedSizeBinary(20)') and l.topic0 = evm_topic('{event.signature}'))"""



# Columns in base table:
# ['block_hash', 'block_num', 'timestamp', 'tx_hash', 'tx_index', 'log_index', 'address', 'topic0', 'topic1', 'topic2', 'topic3', 'data']

# Schema of base table:
# Schema([('block_hash', Binary),
#         ('block_num', UInt64),
#         ('timestamp', Datetime(time_unit='ns', time_zone='UTC')),
#         ('tx_hash', Binary),
#         ('tx_index', UInt32),
#         ('log_index', UInt32),
#         ('address', Binary),
#         ('topic0', Binary),
#         ('topic1', Binary),
#         ('topic2', Binary),
#         ('topic3', Binary),
#         ('data', Binary)])