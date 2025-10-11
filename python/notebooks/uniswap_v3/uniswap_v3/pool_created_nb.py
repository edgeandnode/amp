import marimo

__generated_with = '0.11.31'
app = marimo.App()


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        # Extracting `PoolCreated` events from Uniswap

        This example shows how `evm_decode` and `evm_topic` can be used to find events and decode them.
        """
    )
    return


@app.cell
def _():
    import os.path

    import marimo as mo

    from amp.client import Client
    from amp.util import process_query, to_hex

    client_url = os.getenv('AMP_URL', 'grpc://127.0.0.1:1602')
    client = Client('grpc://34.27.238.174:80')

    # The address of the Uniswap factory contract
    uniswap_factory = '1F98431c8aD98523631AE4a59f267346ea31F984'

    notebook_dir = mo.notebook_dir()
    factory_path = f'{notebook_dir}/abis/factory.json'
    pool_path = f'{notebook_dir}/abis/pool.json'
    return (
        Client,
        client,
        client_url,
        factory_path,
        mo,
        notebook_dir,
        os,
        pool_path,
        process_query,
        to_hex,
        uniswap_factory,
    )


@app.cell
def _(client):
    client.get_sql('select count(*) logs_count from eth_firehose.logs', read_all=True)
    return


@app.cell
def _(client):
    client.get_sql('select count(*) blocks_count, min(block_num) first_block, max(block_num) latest_block from eth_firehose.blocks', read_all=True)
    return


@app.cell
def _(factory_path, uniswap_factory):
    from amp.util import Abi

    # Load a JSON ABI and get the 'PoolCreated' event
    factory = Abi(factory_path)

    pool_created_sig = factory.events['PoolCreated'].signature()

    pool_created_query = f"""
        select pc.block_num,
           pc.dec['pool'] as pool,
           pc.dec['token0'] as token0,
           pc.dec['token1'] as token1,
           pc.dec['fee'] as fee,
           pc.dec['tickSpacing'] as tick_spacing
          from (select l.block_num,
                       evm_decode_log(l.topic1, l.topic2, l.topic3, l.data, '{pool_created_sig}') as dec
                  from eth_firehose.logs l
                 where l.address = arrow_cast(x'{uniswap_factory}', 'FixedSizeBinary(20)')
                   and l.topic0 = evm_topic('{pool_created_sig}')) pc
    """

    # Explain analyze
    # print(client.get_sql("explain analyze " + query, read_all=True).to_pandas()['plan'][0])
    return Abi, factory, pool_created_query, pool_created_sig


@app.cell
def _(client, pool_created_query, process_query):
    process_query(client, pool_created_query)
    return


@app.cell
def _(Abi, client, pool_created_query, pool_path):
    pool_abi = Abi(pool_path)
    swap_sig = pool_abi.events['Swap'].signature()

    query = f"""
    with pc as ({pool_created_query})
    select sw.block_num, sw.tx_hash,
           sw.dec['sender'] as sender,
           sw.dec['recipient'] as recipient,
           sw.dec['amount0'] as amount0,
           sw.dec['amount1'] as amount1,
           sw.dec['sqrtPriceX96'] as sqrt_price_x96,
           sw.dec['liquidity'] as liquidity,
           sw.dec['tick'] as tick
      from (select sl.block_num, sl.tx_hash,
                   evm_decode_log(sl.topic1, sl.topic2, sl.topic3, sl.data, '{swap_sig}') as dec
              from pc, eth_firehose.logs sl
             where sl.address = pc.pool
               and sl.block_num >= pc.block_num
               and sl.topic0 = evm_topic('{swap_sig}')) as sw
    """

    # Note: This will download all the swaps.
    # process_query(client, query)

    # Explain
    print(client.get_sql('explain ' + query, read_all=True).to_pandas()['plan'][1])
    return pool_abi, query, swap_sig


@app.cell
def _(client, process_query, query):
    # This is used to benchmark the same work as the original query, but incurring less network trafic.
    agg_query = f"""
    select count(*), max(arrow_cast(a.amount0, 'Decimal256(76,0)')), max(a.liquidity), sum(arrow_cast(a.amount0, 'Decimal256(76,0)')) from ({query}) a
    """
    process_query(client, agg_query)
    return (agg_query,)


if __name__ == '__main__':
    app.run()
