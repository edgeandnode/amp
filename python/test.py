# run_preprocess.py
from nozzle.dag import load_views_from_sql
from nozzle.dag import load_views_from_sql_parallel
import time
import os   

base_path = os.path.dirname(os.path.abspath(__file__))
aux_path = os.path.join(base_path, 'auxiliary')
core_table_path = os.path.join(base_path, '1_core_tables')

# Measure run time for load_views_from_sql
start_time = time.time()
load_views_from_sql(core_table_path)
end_time = time.time()
print(f"Runtime of load_views_from_sql: {end_time - start_time:.2f} seconds")
# Runtime of load_views_from_sql: 5.93 seconds

# Measure runtime for load_views_from_sql_parallel
start_time = time.time()
load_views_from_sql_parallel(core_table_path)
end_time = time.time()
print(f"Runtime of load_views_from_sql_parallel: {end_time - start_time:.2f} seconds")
