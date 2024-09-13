from nozzle.contracts import Contracts
from nozzle.view import View
from nozzle.chains import Chain
from nozzle.dataset_registry import DatasetRegistry
from nozzle.view_dag import ViewDAG
from nozzle.scheduler import Scheduler
from nozzle.schedule import Schedule

class ExampleView(View):
    def __init__(self, start_block=None, end_block=None):
        # Add weth contract
        Contracts.add_contract(
            name="weth",
            address="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            chain=Chain.ETHEREUM
        )
        weth_contract = Contracts.get_contract("weth")
        
        super().__init__(
            events=[
                weth_contract.events.Transfer,
                weth_contract.events.Approval
            ],
            start_block=20700000,
            end_block=20736586
        )

    def get_query(self) -> str:
        weth_address = Contracts.get_contract("weth").address
        print(self._event_tables['Transfer'])
        return f"""
        SELECT 
            'WETH' as contract,
            event_signature,
            COUNT(*) as event_count
        FROM (
            SELECT event_signature FROM {self._event_tables['Transfer']}
            UNION ALL
            SELECT event_signature FROM {self._event_tables['Approval']}
        )
        GROUP BY event_signature
        ORDER BY event_count DESC
        """

# Usage
example_view = ExampleView(start_block=20730000, end_block=20736586)
result = example_view.execute()

# to run:
# python -m nozzle.example_views.example

# Initialize
registry = DatasetRegistry.load_from_disk()

# Run view
example_view = ExampleView(start_block=20730000, end_block=20736586)
result = example_view.execute()

# Save registry
registry.save_to_disk()

# Set up the DAG
dag = ViewDAG()
dag.add_view("weth_transfer", WETHTransferView)
dag.add_view("weth_approval", WETHApprovalView)
dag.add_view("weth_summary", WETHSummaryView, dependencies=["weth_transfer", "weth_approval"])

# Set up the scheduler
scheduler = Scheduler()
scheduler.add_dag("weth_analysis", dag)
scheduler.schedule_dag("weth_analysis")

# Run the scheduler
scheduler.run()