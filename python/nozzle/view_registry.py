import pkgutil
import importlib
import inspect
from nozzle.subview import Subview

def initialize_subviews():
    package = 'nozzle'
    for _, module_name, _ in pkgutil.iter_modules([package]):
        module = importlib.import_module(f"{package}.{module_name}")
        for name, obj in inspect.getmembers(module):
            if inspect.isclass(obj) and issubclass(obj, Subview) and obj is not Subview:
                obj_instance = obj(name="example", materialized=True, output_type=None, file_output_location=None, table_output_location=None)
                print(f"Initialized {obj.__name__} with query plan: {obj_instance.get_query_plan()}")

if __name__ == "__main__":
    initialize_subviews()