import * as path from 'path'
import * as fs from 'fs';
import { defineDataset, build } from 'project-nozzle'

const dataset = defineDataset((ctx) => ({
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


// Create the directory if it doesn't exist
const dist_path = path.resolve(__dirname, "nozzle_dist")
if (!fs.existsSync(dist_path)) {
    fs.mkdirSync(dist_path, { recursive: true })
}

build(dataset, dist_path)