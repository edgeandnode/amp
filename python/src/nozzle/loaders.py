import os

import adbc_driver_postgresql.dbapi
import pyarrow


class PSQLClient:
    def __init__(self, uri):
        connection_uri = os.environ['ADBC_POSTGRESQL_TEST_URI'] if not uri else uri
        self.conn = adbc_driver_postgresql.dbapi.connect(connection_uri)

    def create_and_load(self, table_name: str, arrow_table: pyarrow.Table, overwrite: bool):
        to_overwrite = False if not overwrite else overwrite
        with self.conn.cursor() as cur:
            if to_overwrite:
                cur.execute(f"DROP TABLE IF EXISTS {table_name}")
            cur.adbc_ingest(table_name, arrow_table, mode='create')
        self.conn.commit()

