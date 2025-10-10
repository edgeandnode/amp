import marimo

__generated_with = "0.14.16"
app = marimo.App(width="full")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Setup""")
    return


@app.cell
def _():
    import marimo as mo

    from amp.client import Client
    return Client, mo


@app.cell
def _(Client):
    client = Client('grpc://127.0.0.1')
    client.configure_connection('my_pg', 'postgresql', {'host': 'localhost', 'database': 'loaders_testing', 'user': 'username', 'password': 'pass', 'port': '5432'})
    client.configure_connection('my_redis', 'redis', {'host': 'localhost', 'port': 6379, 'password': 'mypassword'})
    return (client,)


@app.cell
def _(client):
    client.get_available_loaders()
    return


@app.cell
def _(client):
    client.list_connections()
    return


@app.cell
def _(client):
    client.connection_manager.get_connection_info('my_pg')
    return


@app.cell
def _(client):
    client.connection_manager.get_connection_info('my_redis')
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Arrow""")
    return


@app.cell
def _(client):
    test_query = client.sql('select * from eth_firehose.logs limit 10')
    return (test_query,)


@app.cell
def _(test_query):
    arrow_table = test_query.to_arrow()
    return (arrow_table,)


@app.cell
def _(arrow_table):
    arrow_table
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Postgres""")
    return


app._unparsable_cell(
    r"""
    psql_load_results = client.sql('select * from eth_firehose.logs limit 5000')
        .load(
            'my_pg',
            'logs',
            create_table=True,
        )
    """,
    name='_',
)


@app.cell
def _(psql_load_results):
    for p_result in psql_load_results:
        print(p_result)
    return (p_result,)


@app.cell(hide_code=True)
def _():
    # Redis
    return


@app.cell
def _(client):
    redis_load_results = client.sql('select * from eth_firehose.logs limit 5000').load(
        'my_redis',
        'logs',
        create_table=True,
    )
    return (redis_load_results,)


@app.cell
def _(redis_load_results):
    for r_result in redis_load_results:
        print(r_result)
    return (r_result,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Delta Lake""")
    return


@app.cell
def _(client):
    # Configure Delta Lake connection
    client.configure_connection(name='local_delta_logs', loader='deltalake', config={'table_path': './data/logs', 'partition_by': ['block_num'], 'optimize_after_write': True})
    return


@app.cell
def _(client):
    # Use chaining interface
    result = client.sql('select *  from eth_firehose.logs limit 5000').load('local_delta', './data/logs')

    # Check results
    if hasattr(result, '__iter__'):
        # Streaming mode
        for batch_result in result:
            print(f'Batch: {batch_result.rows_loaded} rows')
    else:
        # Single result
        print(f'Total: {result.rows_loaded} rows')
    return batch_result, result


@app.cell
def _():
    import deltalake
    return (deltalake,)


@app.cell
def _(deltalake):
    dt = deltalake.DeltaTable('./data/logs')
    dt.metadata()
    return (dt,)


@app.cell
def _(dt):
    a = dt.to_pyarrow_dataset()
    return (a,)


@app.cell
def _(a):
    a.head(10)
    return


@app.cell
def _(a):
    a.count_rows()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Iceberg""")
    return


@app.cell
def _(client):
    # Configure Iceberg with local SQLite catalog
    client.configure_connection(
        'my_iceberg',
        'iceberg',
        {
            'catalog_config': {
                'type': 'sql',
                'uri': 'sqlite:///./.data/iceberg/catalog.db',
                'warehouse': './.data/iceberg/warehouse'
            },
            'namespace': 'amp_test',
            'create_namespace': True,
            'create_table': True,
            'schema_evolution': True,
            'batch_size': 5000
        }
    )
    return


@app.cell
def _(client):
    # Load data to Iceberg table
    iceberg_load_results = client.sql('select * from eth_firehose.logs limit 5000').load(
        'my_iceberg',
        'test_logs',
        create_table=True,
    )
    return (iceberg_load_results,)


@app.cell
def _(iceberg_load_results):
    # Check the Iceberg loading results
    for i_result in iceberg_load_results:
        print(f'Iceberg batch: {i_result.rows_loaded} rows loaded, execution time: {i_result.duration:.2f}s')
        if hasattr(i_result, 'metadata') and i_result.metadata:
            print(f'Table location: {i_result.metadata.get("table_location", "N/A")}')
            print(f'Schema: {i_result.metadata.get("schema_summary", "N/A")}')
    return


@app.cell
def _():
    # Query the Iceberg table
    from pyiceberg.catalog import load_catalog

    iceberg_catalog = load_catalog(
        type='sql',
        uri='sqlite:///./.data/iceberg/catalog.db',
        warehouse='./.data/iceberg/warehouse'
    )

    iceberg_table = iceberg_catalog.load_table('amp_test.test_logs')
    result_table = iceberg_table.scan().to_arrow()

    print(f"Rows: {len(result_table)}")
    result_table.slice(0, 10)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# LMDB""")
    return


@app.cell
def _(client):
    # Configure LMDB connection
    client.configure_connection(name='lmdb_logs', loader='lmdb', config={
        'db_path': './data/lmdb_logs',
        'composite_key_columns': ['block_num', 'log_index'],
        'map_size': 10 * 1024**3,
        'create_if_missing': True,
        'transaction_size': 10000,
        'writemap': True,
        'sync': False,
        'readahead': False,
        'max_readers': 8
    })
    return


@app.cell
def _(client):
    lmdb_load_result = client.sql('select * from eth_firehose.logs where block_num > 0 and block_num < 5000000').load('lmdb_logs', 'logs')
    return (lmdb_load_result,)


@app.cell
def _(lmdb_load_result):
    lmdb_load_result
    return


@app.cell
def _(batch_result, lmdb_load_result):
    for lmdb_batch_result in lmdb_load_result:
            print(f'Batch: {batch_result.rows_loaded} rows')
    return (lmdb_batch_result,)


@app.cell
def _():
    import lmdb
    import pyarrow as pa
    return lmdb, pa


@app.cell
def _(lmdb):
    env = lmdb.open('./data/lmdb_logs', readonly=True)
    return (env,)


@app.cell
def _(env):
    env.stat()
    return


@app.cell
def _(env):
    env.info()
    return


@app.cell
def _(env):
    with env.begin() as txn:
       myList = [ key for key, _ in txn.cursor() ]
       print(myList)
       print(len(myList))
    return myList, txn


@app.cell
def _(env, pa):
    with env.begin() as open_txn:
        key = b'9582984:6'
        value = open_txn.get(key)
        print(value)

        if value:
            reader = pa.ipc.open_stream(value)
            batch = reader.read_next_batch()

            print(batch)
    return batch, key, open_txn, reader, value


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
