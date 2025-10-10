import marimo

__generated_with = '0.11.31'
app = marimo.App(width='medium')


@app.cell
def _():
    import altair as alt
    import marimo as mo

    return alt, mo


@app.cell
def _():
    from amp.client import Client
    from amp.util import to_hex

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
def _(client):
    blocks_per_hour = client.get_sql("select date_bin(interval '1 day', timestamp) as day, count(*) as blocks_per_day from eth_firehose.blocks group by date_bin(interval '1 day', timestamp) having count(*) > 1", read_all=True)
    blocks_df = blocks_per_hour.to_pandas()
    return blocks_df, blocks_per_hour


@app.cell
def _(alt, blocks_df, mo):
    day_range_min_1 = blocks_df['day'].min()
    day_range_max_1 = blocks_df['day'].max()

    chart = mo.ui.altair_chart(
        alt.Chart(blocks_df)
        .mark_point()
        .encode(
            alt.X('day', scale=alt.Scale(domain=(day_range_min_1, day_range_max_1))),
            y='blocks_per_day',
            color='blocks_per_day',
        )
    )
    return chart, day_range_max_1, day_range_min_1


@app.cell
def _(chart, mo):
    mo.vstack([chart, mo.ui.table(chart.value)])
    return


@app.cell
def _():
    return


if __name__ == '__main__':
    app.run()
