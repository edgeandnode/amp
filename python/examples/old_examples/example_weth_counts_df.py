from nozzle.view import View
from nozzle.event_registry import EventRegistry
import argparse
import datafusion
from datafusion import SessionContext, DataFrame, functions as f, lit, col
import pyarrow as pa
from nozzle.table_registry import table_registry


class example_weth_counts_df(View):

    def __init__(self, start_block=None, end_block=None):
        super().__init__(
            description = "Example view that counts WETH events",
            events = [EventRegistry.WETH9.Transfer, EventRegistry.WETH9.Approval],
            input_tables = [table_registry.example_weth_counts_df_20700000_20800000],
            start_block=20760000,
            end_block=20780000
        )
        # Require documentation for each event parameter if events are the input
        # self.event_descriptions = {
        #     EventRegistry.WETH9.Transfer: "An event emitted when WETH is transferred from one address to another",
        #     EventRegistry.WETH9.Approval: "An event emitted when a user approves a spender to transfer their WETH"
        # }

        # # Need an event parameter registry as well
        # self.event_parameter_descriptions = {
        #     EventRegistry.WETH9.Transfer.parameters.src: {
        #         "src": "The source address that is transferring the WETH",
        #         "dst": "The destination address that is receiving the WETH",
        #         "wad": "The amount of WETH being transferred"
        #     },
        #     EventRegistry.WETH9.Approval: {
        #         "src": "The source address that is approving the spender",
        #         "guy": "The address that is being approved to transfer the WETH",
        #         "wad": "The amount of WETH being approved"
        #     }
        # }
        

        # Alternatively, you could use existing tables:
        # input_tables = [table_registry.weth_transfers, table_registry.weth_approvals]
        # TODO: Make input tables typed and hard coded
    
    def schema(self) -> pa.lib.Schema:
        return pa.schema([
            pa.field("contract", pa.string(), metadata={"description": "The contract address"}),
            pa.field("event_signature", pa.string(), metadata={"description": "The event signature"}),
            pa.field("event_count", pa.int64(), metadata={"description": "The number of events"}),
            pa.field("num_days", pa.int64(), metadata={"description": "The number of days"}),
            pa.field("events_per_day", pa.float64(), metadata={"description": "The number of events per day"}),
        ])

    def query(self) -> datafusion.DataFrame:
        # User must implement a valid DataFusion SQL query here
        sql_query = f"""
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
        # Rewrite the SQL query as datafusion DataFrame operations
        def df_operations(ctx: SessionContext) -> DataFrame:
            transfers = ctx.table(self._event_tables['Transfer']).select(
                col("event_signature"),
                col("timestamp"),
                col("wad").cast(pa.decimal256(38, 0)).alias("wad")
            )
            approvals = ctx.table(self._event_tables['Approval']).select(
                col("event_signature"),
                col("timestamp"),
                col("wad").cast(pa.decimal256(38, 0)).alias("wad")
            )
            combined = transfers.union(approvals)
            # After running this once, let's union it with the table from another run to test to see if input tables are working
            #combined = combined.union(ctx.table(table_registry.example_weth_counts_df_20700000_20800000.name))
            return (
                combined.aggregate([
                    # Create a constant column for "WETH"
                    lit("WETH").alias("contract"),
                    # Group by event_signature
                    col("event_signature")],
                    [f.count().alias("event_count"),
                    f.count(f.date_trunc(lit("day"), col("timestamp")), distinct=True).alias("num_days")]
                )
                .with_column("events_per_day", f.col("event_count").cast(pa.float64()) / f.col("num_days").cast(pa.float32()))
                .sort(f.order_by(col("event_count"), ascending=False))
            )
        return df_operations


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
    example_weth_counts_view = example_weth_counts_df(start_block=20700000, end_block=None)
    result = example_weth_counts_view.execute(run=args.run)

# To run the view, use:
# python -m nozzle.examples.example_weth_counts --run