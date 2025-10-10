# Amp Dataset Studio

Visualization and validation tool for amp datasets.
Lets users view the available tables to them for querying based off the events provided by their smart contract(s). With these tables, and amp User-Defined Functions (UDFs), lets user run raw sql queries to see data from their smart contracts and visualize it in a UI.

## Getting Started

To run this application:

```bash
pnpm install
pnpm dev
```

## Building For Production

To build this application for production:

```bash
pnpm build
```

## Running amp locally to query anvil

1. Install steps

```bash
# cd into typescript dir
pnpm install
```

2. Spin up docker, from repo root (where `docker-compuse.yml` exists)

```bash
docker compose up -d
```

3. Create required `config.toml` for `ampd` binary to run. In repo root

```toml
data_dir = "data/"
providers_dir = "providers/"
dataset_defs_dir = "dataset-def-schemas/"
metadata_db_url = "postgresql://postgres:postgres@localhost:5432/amp"
```

4. Create necessary directories, also in repo root

```bash
mkdir data && mkdir providers && mkdir dataset-def-schemas
```

5. Create a provider toml config file for anvil in `./providers/anvil.toml`

```toml
kind = "evm-rpc"
url = "http://localhost:8545"
network = "anvil"
```

6. Start amp worker. Will remain running, need terminal window

```bash
AMP_CONFIG=config.toml cargo run -p ampd -- worker --node-id worker-1
```

7. Start amp server. Will remain running, create new terminal window

```bash
AMP_CONFIG=config.toml cargo run -p ampd -- server
```

8. Start anvil in `typescript/example` dir (or any that has foundry). Will remain running, create new terminal window

```bash
# cd into typescript/example
anvil
```

9. Deploy smart contract from `typescript/example` directory (or any dir that is wired up with foundry and smart contracts). create new terminal window

```bash
# cd into typescript/example
forge script contracts/script/Counter.s.sol --broadcast --rpc-url http://localhost:8545 --private-key 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
```

10. Use `ampd` to generate a manifest. Run in repo root

```bash
AMP_CONFIG=config.toml cargo run --bin ampd -- generate-manifest --network anvil --kind evm-rpc --name anvil -o dataset-def-schemas/anvil.json
```

11. Use `ampd` to dump the dataset

```bash
AMP_CONFIG=config.toml cargo run --release --bin ampd -- dump --dataset anvil
```

12. Check to make sure dataset exists:

```bash
curl localhost:1610/datasets
```

13. Run studio cli cmd in `typescript/example`

```bash
# cd into typescript/example
pnpm run studio --open
# or
bun amp studio --open
```

14. (Optional, if building on studio and want HMR for changes). Run studio dev in `typescript/studio`

```bash
# cd into typescript/studio
pnpm run dev
```
