import { defineDataset } from 'project-nozzle'

export default defineDataset((ctx) => ({
    name: "erc20_eth_mainnet",
    version: "0.1.0",
    repository: "https://github.com/graphprotocol/erc20_dataset", // Optional
    readme: "Dataset.md", // Optional, defaults to `Dataset.md` if the file exists
    dependencies: {},
    tables: {},
    udfs: {},
    stream_handlers: {},
    table_functions: {}
}));
