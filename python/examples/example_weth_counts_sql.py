from nozzle.view import View
from nozzle.event_registry import EventRegistry
import argparse
import pyarrow as pa

class example_weth_counts_sql(View):
    def __init__(self, start_block=None, end_block=None):
        super().__init__(
            events=[
                EventRegistry.WETH9.Approval,
                EventRegistry.WETH9.Transfer
            ],
            start_block=20700000,
            end_block=None,
            name = "example_weth_counts_view_sql",
            description = "This view counts the number of WETH approval and transfer events per day."
        )

    def query(self) -> str:
        # User must implement a valid DataFusion SQL query here
        query = f"""
        SELECT 
            'WETH' as contract,
            event_signature,
            COUNT(*) as event_count,
            COUNT(distinct(date_bin(interval '1 day', timestamp))) as num_days,
            COUNT(*) / COUNT(distinct(date_bin(interval '1 day', timestamp))) as events_per_day
        FROM (
            SELECT event_signature, timestamp FROM {self._event_tables['Transfer']}
            UNION ALL
            SELECT event_signature, timestamp FROM {self._event_tables['Approval']}
        )
        GROUP BY event_signature
        ORDER BY event_count DESC
        """
        print(query)
        return query
    
    def schema(self) -> pa.lib.Schema:
        return pa.schema([
            pa.field("contract", pa.string(), metadata={"description": "The contract address"}),
            pa.field("event_signature", pa.string(), metadata={"description": "The event signature"}),
            pa.field("event_count", pa.int64(), metadata={"description": "The number of events"}),
            pa.field("num_days", pa.int64(), metadata={"description": "The number of days"}),
            pa.field("events_per_day", pa.float64(), metadata={"description": "The number of events per day"}),
        ])
        
    

    # TODO: Add documentation requirement
    

# Usage
# example_weth_counts_view = example_weth_counts_view(start_block=20700000, end_block=None)
# result = example_weth_counts_view.execute(full_run=False)

# To do a full run, use:
# python -m nozzle.examples.example_weth_counts  --run

# To do a test run, use:
# python -m nozzle.examples.example_weth_counts

# Take a flag to do a full run or a test run from the command line
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", action="store_true")
    args = parser.parse_args()
    example_weth_counts_view = example_weth_counts_sql(start_block=20700000, end_block=None)
    result = example_weth_counts_view.execute(run=args.run)