from typing import Dict, List, Type, Optional
from nozzle.view import View
from nozzle.dataset_registry import DatasetRegistry

class ViewDAG:
    def __init__(self):
        self.views: Dict[str, Type[View]] = {}
        self.dependencies: Dict[str, List[str]] = {}

    def add_view(self, name: str, view_class: Type[View], dependencies: List[str] = None):
        self.views[name] = view_class
        self.dependencies[name] = dependencies or []

    def get_execution_order(self) -> List[str]:
        visited = set()
        execution_order = []

        def dfs(node):
            if node in visited:
                return
            visited.add(node)
            for dep in self.dependencies[node]:
                if dep not in visited:
                    dfs(dep)
            execution_order.append(node)

        for node in self.views:
            dfs(node)

        return execution_order

    def run(self, start_block: int, end_block: int, force_refresh: bool = False):
        registry = DatasetRegistry.load_from_disk()
        execution_order = self.get_execution_order()

        results = {}
        for view_name in execution_order:
            view_class = self.views[view_name]
            view_instance = view_class(start_block=start_block, end_block=end_block)
            view_instance.force_refresh = force_refresh
            results[view_name] = view_instance.execute()
            print(f"Executed view: {view_name}")

        registry.save_to_disk()
        return results