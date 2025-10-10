from typing import List, Optional

import typer
from rich import print

from amp.client import Client

app = typer.Typer()


@app.command()
def query(table: str, columns: Optional[List[str]] = None, where: Optional[List[str]] = None, limit: Optional[int] = 1):
    client = Client('grpc://34.27.238.174:80')
    print('send ze query :rocket:')
    columns_query = '*' if not columns else columns
    where_query = '' if not where else f'where {where}'
    limit_query = 'limit 1' if not limit else f'limit {limit}'
    query_body = f'select {columns_query} from {table} {where_query} {limit_query}'
    print(f'... {query_body}\n')

    df = client.get_sql(query_body, read_all=True).to_pandas()
    print('results:')
    print(df)


if __name__ == '__main__':
    app()
