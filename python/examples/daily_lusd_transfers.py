from nozzle.view import View
from datafusion import SessionContext, DataFrame, functions as f, lit, col
import pyarrow as pa
import argparse
from nozzle.table_registry import TableRegistry

class daily_lusd_transfers_view(View):
    def __init__(self, start_block, end_block):
        super().__init__(
            description='Daily LUSD transfers by wallet',
            input_tables=[TableRegistry.preprocessed_event_lusdtoken_transfer],  # Add your input tables here from TableRegistry
            start_block=start_block,
            end_block=end_block
        )

    def query(self) -> str:
        # Implement your query here. You can use SQL or DataFrame operations.
        # Example SQL query:
        def df_operations(ctx: SessionContext) -> DataFrame:
            return ctx.table(TableRegistry.preprocessed_event_lusdtoken_transfer.name)\
                .aggregate([
                    col(TableRegistry.preprocessed_event_lusdtoken_transfer.columns.from_).alias('sender'),
                    f.date_trunc(lit('day'), col(TableRegistry.preprocessed_event_lusdtoken_transfer.columns.timestamp)).alias('day')
                ], [
                    f.count().alias('transfer_count'),
                    f.sum(col(TableRegistry.preprocessed_event_lusdtoken_transfer.columns.value_) / pow(10, 17)).alias('total_daily_lusd_transfer_value')
                ]).sort(f.order_by(col('total_daily_lusd_transfer_value'), ascending=False))
        return df_operations

    def schema(self) -> pa.lib.Schema:
        # Define the output schema of your view
        return pa.schema([
            pa.field('sender', pa.string(), metadata={'description': 'Sender of the transfer'}),
            pa.field('day', pa.timestamp('ns', tz='UTC'), metadata={'description': 'Day of the transfer'}),
            pa.field('transfer_count', pa.int64(), metadata={'description': 'Number of transfers'}),
            pa.field('total_daily_lusd_transfer_value', pa.float64(), metadata={'description': 'Total value of the transfers'}),
            # Add more fields as needed
        ])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Daily LUSD transfers by wallet')
    parser.add_argument('--start-block', type=int, required=True, help='Start block number')
    parser.add_argument('--end-block', type=int, required=True, help='End block number')
    args = parser.parse_args()

    view = daily_lusd_transfers_view(start_block=args.start_block, end_block=args.end_block)
    result = view.execute()
    print(result.to_pandas().head(10))

# Available registered tables:
# preprocessed_event_dai_approval, preprocessed_event_dai_transfer, preprocessed_event_lusdtoken_approval, preprocessed_event_lusdtoken_transfer
# To run the view, use: python3 daily_lusd_transfers.py --start_block <start_block> --end_block <end_block>