from nozzle.table_registry import TableRegistry
from nozzle.view import View
import os 
from pathlib import Path
import re
from collections import defaultdict, deque
import glob
from concurrent.futures import ThreadPoolExecutor, as_completed

def remove_curly_braces(sql_query: str) -> str:
    return sql_query.replace("{", "").replace("}", "")

def extract_upstream_tables(input_string):
    return re.findall(r'\{([^}]*)\}', input_string)

def get_sql_file_paths(directory):
    sql_file_paths = {}
    for file_path in glob.glob(os.path.join(directory, '**/*.sql'), recursive=True):
        model_name = os.path.splitext(os.path.basename(file_path))[0]
        sql_file_paths[model_name] = file_path
    return sql_file_paths

def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

def topological_sort(dependencies):
    graph = defaultdict(set)
    in_degree = defaultdict(int)

    for view, deps in dependencies.items():
        for dep in deps:
            graph[dep].add(view)  
            in_degree[view] += 1   
    for view in dependencies:
        if view not in in_degree:
            in_degree[view] = 0

    queue = deque([view for view in in_degree if in_degree[view] == 0])
    execution_order = []

    while queue:
        current_view = queue.popleft()
        execution_order.append(current_view)

        for dependent in graph[current_view]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    if len(execution_order) != len(dependencies):
        raise ValueError("A cycle was detected in the dependencies.")

    return execution_order

def load_views_from_sql(directory: str):
    views = defaultdict(View)
    dependencies = defaultdict(set)
    
    sql_file_paths = get_sql_file_paths(directory)

    sql_queries = {model: read_sql_file(path) for model, path in sql_file_paths.items() if path is not None}

    for model_name, sql in sql_queries.items():
        upstream_models = extract_upstream_tables(sql)
        dependencies[model_name].update(upstream_models)  


    for model_name, sql in sql_queries.items():
        upstream_models = list(dependencies[model_name])
        # Dynamically create a class object for each model
        query = remove_curly_braces(sql)
        DynamicView = type(model_name, (View,), {
            'query': lambda self, query=query: query,  # Capture the query in a default argument
        })
        
        view = DynamicView(
            description=model_name, 
            input_tables=[],
            start_block=0,
            end_block=100
            )  

        views[model_name] = view
    
    execution_order = topological_sort(dependencies)
    existing_tables = set(TableRegistry.list_tables())
    for view_name in execution_order:
        if view_name not in existing_tables:
            print(f'''Processing the view {view_name}''')
            views[view_name].execute()

def load_views_from_sql_parallel(directory: str):
    # TODO: DEBUG the error during parallel execution
    views = defaultdict(View)
    dependencies = defaultdict(set)
    
    sql_file_paths = get_sql_file_paths(directory)

    sql_queries = {model: read_sql_file(path) for model, path in sql_file_paths.items() if path is not None}

    for model_name, sql in sql_queries.items():
        upstream_models = extract_upstream_tables(sql)
        dependencies[model_name].update(upstream_models)  


    for model_name, sql in sql_queries.items():
        upstream_models = list(dependencies[model_name])
        # Dynamically create a class object for each model
        query = remove_curly_braces(sql)
        DynamicView = type(model_name, (View,), {
            'query': lambda self, query=query: query,  # Capture the query in a default argument
        })
        
        view = DynamicView(
            description=model_name, 
            input_tables=[],
            start_block=0,
            end_block=100
            )  

        views[model_name] = view
    
    execution_order = topological_sort(dependencies)
    existing_tables = set(TableRegistry.list_tables())
    # Execute views in parallel
    with ThreadPoolExecutor() as executor:
        future_to_view = {executor.submit(views[view_name].execute): view_name for view_name in execution_order if view_name not in existing_tables}
        
        for future in as_completed(future_to_view):
            view_name = future_to_view[future]
            try:
                future.result()  # This will raise an exception if the execution failed
                print(f"Successfully processed the view {view_name}")
            except Exception as e:
                print(f"Error processing view {view_name}: {e}")
