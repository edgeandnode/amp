# nozzleup

Update or install nozzle with ease.

## Installing

### Public Repository (once open-sourced)

```sh
curl -L https://raw.githubusercontent.com/edgeandnode/project-nozzle/refs/heads/main/nozzleup/install | bash
```

### Private Repository (current)

While the repository is private you need to set `GITHUB_TOKEN`:

```sh
export GITHUB_TOKEN=$(gh auth token)
curl -H "Authorization: Bearer $GITHUB_TOKEN" -L https://raw.githubusercontent.com/edgeandnode/project-nozzle/refs/heads/main/nozzleup/install | bash
```

This will install `nozzleup`. Then, run:

```sh
nozzleup
```

This will install the latest version of nozzle.

To install a specific version:

```sh
nozzleup --install v0.1.0
```

## Usage

### Install Latest Version

```sh
nozzleup
```

### Install Specific Version

```sh
nozzleup --install v0.1.0
```

### Build from Source

Build from a specific branch:

```sh
nozzleup --branch main
```

Build from a specific commit:

```sh
nozzleup --commit abc123
```

Build from a Pull Request:

```sh
nozzleup --pr 123
```

Build from a local repository:

```sh
nozzleup --path /path/to/nozzle
```

Build from a remote fork:

```sh
nozzleup --repo username/fork
```

### Version Management

List installed versions:

```sh
nozzleup --list
```

Switch between versions:

```sh
nozzleup --use v0.1.0
```

Update nozzleup itself:

```sh
nozzleup --update
```

## How It Works

`nozzleup` can install nozzle in two ways:

1. **Precompiled Binaries**: Downloads from [GitHub releases](https://github.com/edgeandnode/project-nozzle/releases)
2. **Build from Source**: Clones and compiles the repository using Cargo

Binaries are stored in `~/.nozzle/versions/<version>/` and the active version is symlinked to `~/.nozzle/bin/nozzle`.

### Supported Platforms

- Linux (x86_64, aarch64)
- macOS (aarch64)

### Private Repositories

While the repository is private, set the `GITHUB_TOKEN` environment variable:

```sh
export GITHUB_TOKEN=$(gh auth token)$
nozzleup
```

## Uninstalling

To uninstall nozzle and nozzleup:

```sh
rm -rf ~/.nozzle
```

Then remove the PATH entry from your shell configuration file (`~/.bashrc`, `~/.zshenv`, or `~/.config/fish/config.fish`).

## Directory Structure

```
~/.nozzle/
├── bin/
│   ├── nozzleup           # Version manager script
│   └── nozzle          # Symlink to active version binary
├── versions/
│   ├── v0.1.0/
│   │   └── nozzle      # Binary for v0.1.0
│   └── v0.2.0/
│       └── nozzle      # Binary for v0.2.0
└── .current_version    # Tracks active version
```

## Troubleshooting

### Command not found: nozzleup

Make sure `~/.nozzle/bin` is in your PATH. You may need to restart your terminal or run:

```sh
source ~/.bashrc  # or ~/.zshenv for zsh, or ~/.config/fish/config.fish for fish
```

### Download failed

Check your internet connection and verify the release exists on GitHub:
https://github.com/edgeandnode/project-nozzle/releases

### Unsupported platform

Currently, precompiled binaries are only available for:
- Linux (x86_64, aarch64)
- macOS (aarch64/Apple Silicon)

For other platforms, build from source using `nozzleup --branch main`.

### Building from source requires Rust

If you're building from source (using `--branch`, `--commit`, `--pr`, `--path`, or `--repo`), you need:
- Rust toolchain (install from https://rustup.rs)
- Git

### SHA256 verification failed

If you see a SHA256 verification error, either:
- Wait for the release to be fully published
- Use `nozzleup --force` to skip verification (not recommended)

### Private repository access

For private repositories, create a GitHub Personal Access Token with `repo` scope and set:

```sh
export GITHUB_TOKEN=ghp_yourtoken
```
