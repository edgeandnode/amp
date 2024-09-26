from nozzle.view import View
from datafusion import SessionContext, DataFrame, functions as f, lit, col
import pyarrow as pa
import argparse
from nozzle.table_registry import TableRegistry

class daily_dai_transfer_counts_view(View):
    def __init__(self, start_block, end_block):
        super().__init__(
            description='This is a table of DAI transfer counts by day',
            input_tables=[TableRegistry.preprocessed_event_dai_transfer],  # Add your input tables here
            start_block=start_block,
            end_block=end_block
        )

    def query(self) -> str:
        # Implement your query here. You can use SQL or DataFrame operations.
        # Example SQL query:
        return f'''
        SELECT
            {TableRegistry.preprocessed_event_dai_transfer.columns.dst} as sender,
            date_bin(interval '1 day', {TableRegistry.preprocessed_event_dai_transfer.columns.timestamp}) as day,
            count(*) as transfer_count,
            avg({TableRegistry.preprocessed_event_dai_transfer.columns.wad})/pow(10,17) as avg_value
        FROM {TableRegistry.preprocessed_event_dai_transfer.name}
        GROUP BY 1, 2
        ORDER BY 4 DESC
        '''

        # Example DataFrame operations:
        # def df_operations(ctx: SessionContext) -> DataFrame:
        #     return ctx.table('your_input_table')\
        #         .filter((col('block_num') >= lit(self.start_block)) & (col('block_num') <= lit(self.end_block)))\
        #         .select([col('column_name')])\
        #         .limit(10)
        # return df_operations

    def schema(self) -> pa.lib.Schema:
        # Define the output schema of your view
        return pa.schema([
            pa.field('sender', pa.string(), metadata={'description': 'Sender of the transfer'}),
            pa.field('day', pa.timestamp('ns', tz='UTC'), metadata={'description': 'Day of the transfer'}),
            pa.field('transfer_count', pa.int64(), metadata={'description': 'Number of transfers'}),
            pa.field('avg_value', pa.float64(), metadata={'description': 'Average value of the transfer'}),
            # Add more fields as needed
        ])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='This is a table of DAI transfer counts by day')
    parser.add_argument('--start-block', type=int, required=True, help='Start block number')
    parser.add_argument('--end-block', type=int, required=True, help='End block number')
    args = parser.parse_args()

    view = daily_dai_transfer_counts_view(start_block=args.start_block, end_block=args.end_block)
    result = view.execute()
    print(result.to_pandas())

# Available registered tables:
# preprocessed_event_dai_approval, preprocessed_event_dai_transfer
# To run the view, use: python3 daily_dai_transfer_counts.py --start-block <start_block> --end-block <end_block>