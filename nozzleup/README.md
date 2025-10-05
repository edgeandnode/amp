# nozzleup

The official version manager and installer for nozzle.

## Features

- Install nozzle from GitHub releases or build from source
- Manage multiple versions and switch between them
- Support for private repositories (via `GITHUB_TOKEN`)
- SHA256 verification for all downloads
- Cross-platform support (Linux, macOS)
- Multi-architecture support (x86_64, aarch64)

## Installation

### Quick Install

For public repositories (once open-sourced):

```sh
curl -L https://raw.githubusercontent.com/edgeandnode/project-nozzle/main/nozzleup/install | bash
```

### Private Repository

While the repository is private, you need to set `GITHUB_TOKEN`:

```sh
export GITHUB_TOKEN=$(gh auth token)
curl -L https://raw.githubusercontent.com/edgeandnode/project-nozzle/main/nozzleup/install | GITHUB_TOKEN=$GITHUB_TOKEN bash
```

This installs `nozzleup`. Then run:

```sh
nozzleup
```

This will install the latest version of nozzle.

## Usage

### Install Latest Version

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

Build from a specific branch:

```sh
nozzleup branch main
```

Build from a specific commit:

```sh
nozzleup commit abc123
```

Build from a Pull Request:

```sh
nozzleup pr 123
```

Build from a local repository:

```sh
nozzleup path /path/to/nozzle
```

Build from a remote fork:

```sh
nozzleup repo username/fork
```

### Update nozzleup Itself

```sh
nozzleup update
```

## How It Works

`nozzleup` is a Rust-based version manager with a minimal bash bootstrap script for installation.

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

## Versioning

nozzleup uses **independent versioning** from nozzle:

- **nozzle releases:** `v0.1.0`, `v0.2.0`, `v1.0.0`, etc.
- **nozzleup releases:** `nozzleup-v1.0.0`, `nozzleup-v1.1.0`, `nozzleup-v2.0.0`, etc.

This allows nozzleup to be updated independently (bug fixes, new features) without requiring a nozzle release. nozzleup is version-agnostic and works with all versions of nozzle.

## Supported Platforms

- Linux (x86_64, aarch64)
- macOS (aarch64/Apple Silicon)

## Environment Variables

- `GITHUB_TOKEN`: GitHub personal access token for private repository access
- `NOZZLE_REPO`: Override repository (default: `edgeandnode/project-nozzle`)
- `NOZZLE_DIR`: Override installation directory (default: `~/.nozzle`)

## Security

- All downloads are verified using SHA256 checksums
- macOS binaries are code-signed and notarized
- Private repository access uses GitHub's OAuth token mechanism

## Troubleshooting

### Command not found: nozzleup

Make sure `~/.nozzle/bin` is in your PATH. You may need to restart your terminal or run:

```sh
source ~/.bashrc  # or ~/.zshenv for zsh, or ~/.config/fish/config.fish for fish
```

### Download failed

- Check your internet connection
- Verify the release exists on GitHub
- For private repos, ensure `GITHUB_TOKEN` is set correctly

### SHA256 verification failed

- Wait for the release to be fully published
- Check if the checksum file exists in the release assets
- As a last resort, use `--force` to skip verification (not recommended)

### Building from source requires Rust

If you're building from source (using `branch`, `commit`, `pr`, `path`, or `repo` commands), you need:

- Rust toolchain (install from https://rustup.rs)
- Git
- Build dependencies (see main project documentation)

## Uninstalling

To uninstall nozzle and nozzleup:

```sh
rm -rf ~/.nozzle
```

Then remove the PATH entry from your shell configuration file.

## Development

nozzleup is implemented in Rust and lives in the main nozzle repository:

- Source: `crates/bin/nozzleup/`
- Install script: `nozzleup/install`

### Building nozzleup

```sh
cargo build --release -p nozzleup
```

### Running tests

```sh
cargo test -p nozzleup
```

## License

Same as the main nozzle project.
