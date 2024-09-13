import schedule
import time
from typing import Dict
from .view_dag import ViewDAG
from .dataset_registry import DatasetRegistry

class Scheduler:
    def __init__(self):
        self.dags: Dict[str, ViewDAG] = {}

    def add_dag(self, name: str, dag: ViewDAG):
        self.dags[name] = dag

    def _run_dag(self, dag_name: str):
        print(f"Running DAG: {dag_name}")
        dag = self.dags[dag_name]
        registry = DatasetRegistry.load_from_disk()
        
        for view_name, view_class in dag.views.items():
            view_instance = view_class()
            if view_instance.schedule:
                result = view_instance.execute()
                print(f"Executed view: {view_name}")
                print(f"Result: {result}")

        registry.save_to_disk()

    def schedule_dag(self, dag_name: str):
        dag = self.dags[dag_name]
        for view_name, view_class in dag.views.items():
            view_instance = view_class()
            if view_instance.schedule:
                schedule.every(view_instance.schedule.interval).do(self._run_dag, dag_name)

    def run(self):
        while True:
            schedule.run_pending()
            time.sleep(1)