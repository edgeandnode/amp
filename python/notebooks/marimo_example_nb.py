import marimo

__generated_with = '0.11.31'
app = marimo.App(width='medium')


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _():
    from nozzle.client import Client
    from nozzle.util import to_hex

    return Client, to_hex


@app.cell
def _(Client):
    client = Client('grpc://34.27.238.174:80')
    return (client,)


@app.cell
def _(client):
    df = client.get_sql('select * from eth_firehose.logs limit 1', read_all=True).to_pandas()
    df
    return (df,)


@app.cell
def _(client):
    block_count = client.get_sql('select count(*) block_count from eth_firehose.blocks', read_all=True)
    return (block_count,)


@app.cell
def _(block_count):
    block_count
    return


@app.cell
def _():
    return


if __name__ == '__main__':
    app.run()
