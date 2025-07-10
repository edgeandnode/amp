import marimo

__generated_with = "0.11.31"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# ERC721 Data""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Setup""")
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    import pyarrow.compute as pc

    from nozzle.client import Client
    from nozzle.loaders import PSQLClient
    from nozzle.util import process_query, to_hex
    from nozzle.util import Abi
    return Abi, Client, PSQLClient, pc, process_query, to_hex


@app.cell
def _(Client, PSQLClient):
    client = Client('grpc://34.27.238.174:80')

    local_db_uri = 'postgresql://username:password@localhost:5432/hashmasks'
    psql_client = PSQLClient(local_db_uri)
    return client, local_db_uri, psql_client


@app.cell
def _(Abi, mo):
    notebook_dir = mo.notebook_dir()
    erc721_abi_path = f'{notebook_dir}/abis/erc721.json'
    erc721 = Abi(erc721_abi_path)
    return erc721, erc721_abi_path, notebook_dir


@app.cell
def _(client):
    client.get_sql("select block_hash from eth_firehose.logs limit 1", read_all=True)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Hashmasks""")
    return


@app.cell
def _(erc721):
    # Setup hashmasks contract values
    transfer_sig = erc721.events['Transfer'].signature()
    hashmasks_contract = 'C2C747E0F7004F9E8817Db2ca4997657a7746928'
    return hashmasks_contract, transfer_sig


@app.cell
def _(transfer_sig):
    transfer_sig
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Transfers""")
    return


@app.cell
def _(hashmasks_contract, transfer_sig):
    hashmask_transfers_query = f"""
        select 
            pc.block_num,
            pc.block_hash,
            pc.timestamp,
            pc.tx_hash,
            pc.dec['from'] as from_address,
            pc.dec['to'] as to_address,
            pc.dec['tokenId'] as token_id      
        from (
            select 
                l.block_num,
                l.block_hash,
                l.tx_hash,
                l.timestamp,
                evm_decode(l.topic1, l.topic2, l.topic3, l.data, '{transfer_sig}') as dec
            from eth_firehose.logs l
            where l.address = arrow_cast(x'{hashmasks_contract}', 'FixedSizeBinary(20)')
                and l.topic0 = evm_topic('{transfer_sig}')) pc
    """
    return (hashmask_transfers_query,)


@app.cell
def _(client, hashmask_transfers_query):
    # hashmasks_transfers = process_query(client, hashmask_transfers_query)
    hashmasks_transfers_pa = client.get_sql(hashmask_transfers_query, read_all=True)
    return (hashmasks_transfers_pa,)


@app.cell
def _(hashmasks_transfers_pa, psql_client):
    psql_client.create_and_load(table_name="transfers", arrow_table=hashmasks_transfers_pa, overwrite=True)
    return


@app.cell
def _(hashmasks_transfers_pa):
    len(hashmasks_transfers_pa.column("block_num"))
    return


@app.cell
def _(hashmasks_transfers_pa):
    id_example = hashmasks_transfers_pa.column("token_id")[0].as_py
    return (id_example,)


@app.cell
def _(hashmasks_transfers_pa):
    hashmasks_transfers_pa
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Owners""")
    return


@app.cell
def _(hashmasks_contract, transfer_sig):
    hashmasks_token_owners = f"""
    with owners as
        (
            select 
                pc.dec['tokenId'] as token_id,
                last_value(pc.dec['to'] order by block_num, log_index) as owner                
            from 
                (        
                select 
                    l.block_num,
                    l.log_index,
                    evm_decode(l.topic1, l.topic2, l.topic3, l.data, '{transfer_sig}') as dec
                from eth_firehose.logs l
                where l.address = arrow_cast(x'{hashmasks_contract}', 'FixedSizeBinary(20)')
                    and l.topic0 = evm_topic('{transfer_sig}')
                ) pc
            group by token_id
        )
        select owner, array_agg(token_id), count(token_id) from owners group by owner


    """
    return (hashmasks_token_owners,)


@app.cell
def _(client, hashmasks_token_owners):
    hashmasks_token_owners_pa= client.get_sql(hashmasks_token_owners, read_all=True)
    return (hashmasks_token_owners_pa,)


@app.cell
def _(hashmasks_token_owners_pa, psql_client):
    psql_client.create_and_load(table_name="owners", arrow_table=hashmasks_token_owners_pa, overwrite=True)
    return


@app.cell
def _():
    # expr = pc.field('block_num') <= 2222800
    # filtered = hashmasks_transfers_pa.filter(expr).select(['block_num'])

     # filtered
    return


@app.cell
def _(hashmasks_token_owners_pa):
    hashmasks_token_owners_pa
    return


@app.cell
def _(hashmasks_tokens_pa):
    hashmasks_tokens_pa.to_pandas()
    return


@app.cell
def _(hashmasks_token_owners_pa):
    len(hashmasks_token_owners_pa)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Tokens""")
    return


@app.cell
def _(hashmasks_contract, transfer_sig):
    hashmasks_tokens = f"""
        select 
            pc.dec['tokenId'] as token_id,
            last_value(pc.dec['to'] order by block_num, log_index) as owner                
        from 
            (        
            select 
                l.block_num,
                l.log_index,
                evm_decode(l.topic1, l.topic2, l.topic3, l.data, '{transfer_sig}') as dec
            from eth_firehose.logs l
            where l.address = arrow_cast(x'{hashmasks_contract}', 'FixedSizeBinary(20)')
                and l.topic0 = evm_topic('{transfer_sig}')
            ) pc
        group by token_id
    """
    return (hashmasks_tokens,)


@app.cell
def _(client, hashmasks_tokens):
    hashmasks_tokens_pa= client.get_sql(hashmasks_tokens, read_all=True)
    return (hashmasks_tokens_pa,)


@app.cell
def _(hashmasks_tokens_pa, pc):
    len(pc.unique(hashmasks_tokens_pa['token_id'])) == len(hashmasks_tokens_pa)
    return


@app.cell
def _(hashmasks_tokens_pa, psql_client):
    psql_client.create_and_load(table_name="tokens", arrow_table=hashmasks_tokens_pa, overwrite=True)
    return


if __name__ == "__main__":
    app.run()
