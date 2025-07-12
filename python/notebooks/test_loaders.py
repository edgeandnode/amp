import marimo

__generated_with = '0.11.31'
app = marimo.App(width='full')


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Setup""")
    return


@app.cell
def _():
    import marimo as mo

    from nozzle.client import Client

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
    delta_load_results = client.sql('select * from eth_firehose.logs limit 5000').load(
        'my_redis',
        'logs',
        create_table=True,
    )
    return (delta_load_results,)


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
    dt = deltalake.DeltaTable('./data/flight_results')
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


@app.cell
def _():
    return


if __name__ == '__main__':
    app.run()
