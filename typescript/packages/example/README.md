# Nozzle <3 Foundry Example

This example demonstrates a crude version of a shared smart contract & application development stack
with a Nozzle data engineering layer that integrates with it.

## Requirements

- [nozzle](https://github.com/edgeandnode/project-nozzle) 
- [foundry](https://book.getfoundry.sh/getting-started/installation)
- [bun](https://bun.sh/docs/installation)

For installing `nozzle`, currently, your best bet is to build it from source. In order to do so, you'll need
to have `cargo` installed.

Once `cargo` is installed, clone this repository and run the following command from the root:

```sh
git clone git@github.com:edgeandnode/project-nozzle.git
cd project-nozzle
cargo build --release -p nozzle
```

This will create an executable `nozzle` binary at `target/release/nozzle`. For convenience, you should drop
this binary into your `$PATH`. One option would be creating a symlink to it from one of your existing `bin`
directories that are already in your `$PATH` (e.g. `/usr/local/bin`).

```sh
# You might need to run this with `sudo`
sudo ln -s $(pwd)/target/release/nozzle /usr/local/bin/nozzle
```

## Usage

Now, navigate to the `typescript/packages/examples/` folder within this repository.

Ideally you'd run the following commands in seperate terminal windows/panes. These will run continously.

```sh
# Terminal 1: This starts an `anvil` development node
anvil

# Terminal 2: This runs the `nozzle` dev server (yes, the cli is called `nozzl` for now)
bun nozzl dev
```

Now that your `anvil` node and the `nozzle` server are running, you can deploy the example `Counter` smart contract
to your local chain.

```sh
# Terminal 3: Deploys the `Counter` smart contract and runs a few transactions on it
forge script contracts/script/Counter.s.sol --broadcast --rpc-url http://localhost:8545 --private-key 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
```

You should now see some activity in terminal window #1 and #2: `anvil` should have processed your smart contract
deployment and subsequent transactions, whilst `nozzle` should have picked up the new blocks and dumped both the
raw chain dataset (called `anvil`) as well as the `counts` dataset.

You can now go ahead and query the `counts` dataset.

```sh
# Terminal 3: Query the `counts` dataset
bun nozzl query "SELECT address, block_num, count FROM example.counts ORDER BY block_num"
```

Whenever you change your dataset configuration `nozzle.config.ts` or a new block is mined (e.g. when
re-deploying the `Counter` smart contract by running the same `forge script` command as shown above),
the `nozzle` development server will automatically dump all updates and make them available for querying immediately.

## Hacking

There's some simple code examples in the `examples/` folder for how to interact with the `nozzle` server programmatically.

As long as the nozzle dev server is running, you can conveniently run the example with `bun`.

```sh
bun run examples/query.ts
```
