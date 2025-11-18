# Project Amp

_The data goes in the bucket_

**This repository is currently private. But it may eventually be open sourced, retaining commit history, issues and PRs so don't commit sensitive stuff.**

## What is this

An experiment in ETL architecture for data services on The Graph. 'Project Amp' is a codename.

## Installation

The easiest way to install Amp is using `ampup`, the official version manager and installer:

```sh
# Install ampup
curl --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/edgeandnode/amp/refs/heads/main/install | sh
```

This will install `ampup` and the latest version of `ampd`. You may need to restart your terminal or run `source ~/.zshenv` (or your shell's equivalent) to update your PATH.

Once installed, you can manage `ampd` versions:

```sh
# Install or update to the latest version
ampup install

# Switch between installed versions
ampup use v0.1.0

# Build from source (main branch)
ampup build

# Build from a specific PR or branch
ampup build --pr 123
ampup build --branch develop
```

For more details and advanced options, see the [ampup README](ampup/README.md).

### Installation via Nix

For Nix users, `ampd` is available as a flake:

```sh
# Run directly without installing
nix run github:edgeandnode/amp

# Install to your profile
nix profile install github:edgeandnode/amp

# Try it out temporarily
nix shell github:edgeandnode/amp -c ampd --version
```

You can also add it to your NixOS or home-manager configuration:

```nix
{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    amp = {
      url = "github:edgeandnode/amp";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { nixpkgs, amp, ... }: {
    # NixOS configuration
    nixosConfigurations.myhost = nixpkgs.lib.nixosSystem {
      # ...
      environment.systemPackages = [
        amp.packages.${system}.ampd
      ];
    };

    # Or home-manager configuration
    home.packages = [
      amp.packages.${system}.ampd
    ];
  };
}
```

Note: Nix handles version management, so `ampup` is not needed for Nix users.

### Building from Source (Manual)

If you prefer to build manually without using `ampup`:

```sh
cargo build --release -p ampd
```

The binary will be available at `target/release/ampd`.

### Python Quickstart

To quickly dive into the power of Amp with Python, best is to run the [`marimo_example_nb.ipynb](python/notebooks/marimo_example_nb.pyy). Follow the instructions in thy [Python README](python/README.md).

## Components

See [config.md](docs/config.md) for how to configure amp.

### Prerequisites

To run Amp, you will need a PostgreSQL metadata database. You can start one with `docker-compose`:

```
docker-compose up -d
```

This will run the metadata DB at `postgresql://postgres:postgres@localhost:5432/amp`.
Update your config file to include it:

```toml
metadata_db_url = "postgresql://postgres:postgres@localhost:5432/amp"
```

### ampd server

To run, just `cargo run -p ampd -- server`.

This starts both a JSON Lines over HTTP server and an Arrow Flight (gRPC) server.
The HTTP server is straightforward, send it a query and get results, one row per line:

```
curl -X POST http://localhost:1603 --data 'select * from "my_namespace/eth_rpc".logs limit 10'
```

The Arrow Flight server requires a specialized client. We currently have a Python client,
see docs for that [here](https://github.com/edgeandnode/amp/tree/main/python).

> **Note:**
>
> - When streaming data from the `jsonl` server, responses are always sent in uncompressed format, regardless of any `accept-encoding` headers (such as `gzip`, `br`, or `deflate`).
> - For non-streaming requests, responses will be compressed according to the `accept-encoding` header, if specified.

### Telemetry

Amp has an OpenTelemetry setup to track various metrics and traces. For local testing, a Grafana telemetry stack
is already configured and can be run through `docker-compose`:

```
docker-compose up -d
```

This will (among other things) run the `grafana/otel-lgmt` image. More info about the image can be found [here](https://github.com/grafana/docker-otel-lgtm/).

More detailed instructions regarding telemetry can be found in [here](./docs/telemetry.md).
