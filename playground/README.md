# Dataset Authoring Playground (dbt-style)

This playground is a small dataset for trying the new authoring workflow with
models discovery and Jinja templating.

## What it includes

- `amp.yaml` with basic metadata, dependencies, and default `vars`
- A `models/` directory with two SELECT-only models
- Jinja usage via `ref()` and `var()`

## Prerequisites (run Amp + register raw dataset)

1. Run Amp locally (single-node mode):

```bash
cargo run -p ampd -- solo
```

2. Generate, register, and deploy the raw dataset referenced by `amp.yaml`:

```bash
mkdir -p manifests
cargo run -p ampctl manifest generate --network mainnet --kind evm-rpc -o ./manifests/eth_mainnet.json
cargo run -p ampctl dataset register my_org/eth_mainnet ./manifests/eth_mainnet.json --tag 1.0.0
cargo run -p ampctl dataset deploy my_org/eth_mainnet@1.0.0
```

Note: you must also configure a provider for the selected network/kind
(see `docs/config.md` under **Providers**).

## Usage

Build the dataset (writes to a dedicated output directory):

```bash
cargo run -p ampctl dataset build \
  --dir playground \
  --output playground/build
```

Override variables at build time:

```bash
cargo run -p ampctl dataset build \
  --dir playground \
  --output playground/build \
  --var min=5
```

Package the build output:

```bash
cargo run -p ampctl dataset package --dir playground/build
```

## Notes

- Every `models/**/*.sql` file becomes a table; the model name is the file stem.
- SQL must be a single SELECT statement.
- Dependencies are resolved to their hash. Update `dependencies` in `amp.yaml`
  to point at datasets available in your registry (or local amp server)
