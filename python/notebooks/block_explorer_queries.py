import marimo

__generated_with = "0.11.31"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        # Block explorer queries
        Testing the queries that would be used in a block explorer application
        """
    )
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    from nozzle.client import Client
    return (Client,)


@app.cell
def _(Client):
    client = Client('grpc://34.27.238.174:80')
    return (client,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Blocks queries""")
    return


@app.cell
def _(client):
    blocks_columns = client.get_sql(f"""
        select 
            *
        from eth_firehose.blocks 
        LIMIT 1""", read_all=True)
    return (blocks_columns,)


@app.cell
def _(blocks_columns):
    blocks_columns
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Latest block number""")
    return


@app.cell
def _(client):
    # get latest block number
    # return: number
    latest_block = client.get_sql("select max(block_num) as latest_block from eth_firehose.blocks", read_all=True)
    latest_block_num = latest_block[0][0].as_py()
    return latest_block, latest_block_num


@app.cell
def _(latest_block_num):
    latest_block_num
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Recent 10 blocks - simple""")
    return


@app.cell
def _(client, latest_block_num):
    # get latest 10 blocks (simple)
    # return: block_number,  miner_address, count(txs)

    latest_blocks_simple = client.get_sql(f"""
        select 
            block_num, miner 
        from eth_firehose.blocks 
        where block_num > {latest_block_num-10} 
        LIMIT 10""", read_all=True)
    return (latest_blocks_simple,)


@app.cell
def _(latest_blocks_simple):
    latest_blocks_simple.to_pandas()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Recent 10 blocks - detail""")
    return


@app.cell
def _(client, latest_block_num):
    # get latest 10 blocks (more detail)
    # return: block_number, age, count(txs), gas_used, gas_limit, fee_recipient, base_fee, reward, burnt_fees

    latest_blocks_detail = client.get_sql(f"""
        select 
            block_num, miner, timestamp, gas_used, gas_limit, base_fee_per_gas 
        from eth_firehose.blocks 
        where block_num > {latest_block_num-10} 
        LIMIT 11""", read_all=True)
    return (latest_blocks_detail,)


@app.cell
def _(latest_blocks_detail):
    latest_blocks_detail.to_pandas()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Specific block details""")
    return


@app.cell
def _(client, latest_block_num):
    # get details on specific block
    # return: entire block with transactions

    block = client.get_sql(f"""
        select 
            block_num, miner, timestamp, gas_used, gas_limit, base_fee_per_gas, extra_data
        from eth_firehose.blocks 
        where block_num =  {latest_block_num-7550786} 
        LIMIT 3""", read_all=True)
    return (block,)


@app.cell
def _(block):
    block.to_pandas()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Transactions queries""")
    return


@app.cell
def _(client):
    tx_columns = client.get_sql(f"""
        select 
            *
        from eth_firehose.transactions 
        LIMIT 1""", read_all=True)
    return (tx_columns,)


@app.cell
def _(tx_columns):
    tx_columns
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Transactions in block""")
    return


@app.cell
def _(client):
    # get all txs for a block
    # return: tx_hash, method, block_number, age, from_address, to_address, amount, tx_fee
    txs_block_num = 14524138
    txs_in_block = client.get_sql(f"""
        select 
            block_num,
            tx_hash, 
            tx_index, 
            to, 
            "from", 
            gas_used, 
            type,
            input,
            gas_price,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            return_data
        from eth_firehose.transactions
        where block_num = {txs_block_num}
        LIMIT 100""", read_all=True)
    return txs_block_num, txs_in_block


@app.cell
def _(txs_in_block):
    txs_in_block
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Transaction details""")
    return


@app.cell
def _(client, txs_block_num, txs_in_block):
    # get details on specific tx
    # return: all tx data
    tx_index = txs_in_block.column("tx_index")[0]
    tx_details = client.get_sql(f"""
        select 
            block_num,
            tx_hash, 
            tx_index, 
            to, 
            "from", 
            gas_used, 
            type,
            input,
            gas_price,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            return_data
        from eth_firehose.transactions
        where block_num = {txs_block_num} and tx_index = {tx_index}
        LIMIT 100""", read_all=True)
    return tx_details, tx_index


@app.cell
def _(tx_details):
    tx_details
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
