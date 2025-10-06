# nozzleup

The official version manager and installer for nozzle.

## Installation

### Quick Install

While the repository is private, you need to authenticate with GitHub:

```sh
# Obtain a personal access token.
export GITHUB_TOKEN=$(gh auth token)

# Set up `nozzleup` from the private repository.
curl --proto '=https' --tlsv1.2 -sSf \
  -H "Authorization: Bearer $GITHUB_TOKEN" \
  https://raw.githubusercontent.com/edgeandnode/project-nozzle/main/nozzleup/install | sh
```

Once installed, you can conveniently manage your `nozzle` versions through `nozzleup`.

## Usage

### Install Latest Version

> NOTE: By default, the `nozzleup` installer will also install the latest version of `nozzle`.

```sh
nozzleup
```

or

```sh
nozzleup install
```

### Install Specific Version

```sh
nozzleup install v0.1.0
```

### List Installed Versions

```sh
nozzleup list
```

### Switch Between Versions

```sh
nozzleup use v0.1.0
```

### Uninstall a Version

```sh
nozzleup uninstall v0.1.0
```

### Build from Source

Build from the default repository's main branch:

```sh
nozzleup build
```

Build from a specific branch:

```sh
nozzleup build --branch main
```

Build from a specific commit:

```sh
nozzleup build --commit abc123
```

Build from a Pull Request:

```sh
nozzleup build --pr 123
```

Build from a local repository:

```sh
nozzleup build --path /path/to/nozzle
```

Build from a custom repository:

```sh
nozzleup build --repo username/fork
```

Combine options (e.g., custom repo + branch):

```sh
nozzleup build --repo username/fork --branch develop
```

Build with a custom version name:

```sh
nozzleup build --path . --name my-custom-build
```

Build with specific number of jobs:

```sh
nozzleup build --branch main --jobs 8
```

### Update nozzleup Itself

```sh
nozzleup update
```

## How It Works

`nozzleup` is a Rust-based version manager with a minimal bootstrap script for installation.

### Installation Methods

1. **Precompiled Binaries** (default): Downloads signed binaries from [GitHub releases](https://github.com/edgeandnode/project-nozzle/releases)
2. **Build from Source**: Clones and compiles the repository using Cargo

### Directory Structure

```
~/.nozzle/
├── bin/
│   ├── nozzleup           # Version manager binary
│   └── nozzle             # Symlink to active version
├── versions/
│   ├── v0.1.0/
│   │   └── nozzle         # Binary for v0.1.0
│   └── v0.2.0/
│       └── nozzle         # Binary for v0.2.0
└── .version               # Tracks active version
```

## Supported Platforms

- Linux (x86_64, aarch64)
- macOS (aarch64/Apple Silicon)

## Environment Variables

- `GITHUB_TOKEN`: GitHub personal access token for private repository access
- `NOZZLE_REPO`: Override repository (default: `edgeandnode/project-nozzle`)
- `NOZZLE_DIR`: Override installation directory (default: `$XDG_CONFIG_HOME/.nozzle` or `$HOME/.nozzle`)

## Security

- macOS binaries are code-signed and notarized
- Private repository access uses GitHub's OAuth token mechanism

## Troubleshooting

### Command not found: nozzleup

Make sure the `nozzleup` binary is in your PATH. You may need to restart your terminal or run:

```sh
source ~/.bashrc  # or ~/.zshenv for zsh, or ~/.config/fish/config.fish for fish
```

### Download failed

- Check your internet connection
- Verify the release exists on GitHub
- For private repos, ensure `GITHUB_TOKEN` is set correctly

### Building from source requires Rust

If you're building from source (using the `build` command), you need:

- Rust toolchain (install from https://rustup.rs)
- Git
- Build dependencies (see main project documentation)

## Uninstalling

To uninstall nozzle and nozzleup, simply delete you `.nozzle` directory (default: `$XDG_CONFIG_HOME/.nozzle` or `$HOME/.nozzle`):

```sh
rm -rf ~/.nozzle # or $XDG_CONFIG_HOME/.nozzle
```

Then remove the PATH entry from your shell configuration file.
