# Getting started with the Python client

To get started with the Python client, you will need
[`poetry`](https://python-poetry.org/)
installed locally. With that, run `poetry install` in this directory. That
will install all required dependencies, including
[Jupyter](https://jupyter.org/)


You will need to have the files that [`dump`](../dump) produces available
locally, and run the [server](../server) You can then start Jupyter with
```shell
poetry run jupyter notebook examples/
```

That will open a new browser tab. In that, double click on
`getting-started.ipynb` and select `Run > Run All Cells` from the menu.
