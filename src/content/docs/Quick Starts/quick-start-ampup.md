---
title: Quickstart Ampup
description: Build with Amp with Ampup
---

This quickstart helps you jump start Amp locally

## Prerequisites

1. Repository Setup

`git clone <repository-url>
cd <repository-name>`

2. PostgreSQL database Setup

You need this database to store metadata.  
The easiest way to set this up is by using **Docker**.

### Steps

1. From the root of the `amp` repository, start the PostgreSQL service:

```bash
   docker-compose up -d db
```

This command starts PostgreSQL in the background.

2. Verify that the PostgreSQL container is running:

```bash
docker ps | grep postgres
```

## Step-by-step

### Installation via Ampup

1. Use `ampup`, the official version manager and installer:

```sh
curl --proto '=https' --tlsv1.2 -sSf https://ampup.sh/install | sh
```

This will install `ampup` and the latest version of `ampd`. You may need to restart your terminal or run `source ~/.zshenv` (or your shell's equivalent) to update your PATH.

Once installed, you can manage `ampd` versions:

```sh
# Install or update to the latest version
ampup install

# Switch between installed versions
ampup use
```

> For more details and advanced options, see `ampup --help`.

### Installation via Nix

> This will be supported once the source repository has been released

For Nix users, `ampd` is available as a flake:

```sh
# Run directly without installing
nix run github:edgeandnode/amp

# Install to your profile
nix profile install github:edgeandnode/amp

# Try it out temporarily
nix shell github:edgeandnode/amp -c ampd --version
```

#### Add Configurations

You can also add amp to your NixOS or home-manager configuration:

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

> Note: Nix handles version management, so `ampup` is not needed for Nix users.

### Building from Source (Manual)

> This will be supported once the source repository has been released

If you prefer to build manually without using `ampup`:

```sh
cargo build --release -p ampd
```

The binary will be available at `target/release/ampd`.
