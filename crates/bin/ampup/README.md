# ampup

The official version manager and installer for amp.

## Installation

### Quick Install

```sh
# Install ampup
curl --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/edgeandnode/amp/refs/heads/main/install.sh | sh
```

Once installed, you can conveniently manage your `ampd` versions through `ampup`.

### Customizing Installation

The installer script accepts options to customize the installation process:

```sh
# Skip automatic PATH modification
curl ... | sh -s -- --no-modify-path

# Skip installing the latest ampd version
curl ... | sh -s -- --no-install-latest

# Use a custom installation directory
curl ... | sh -s -- --install-dir /custom/path

# Combine multiple options
curl ... | sh -s -- --no-modify-path --no-install-latest --install-dir ~/.custom/amp
```

**Available Options:**

- `--install-dir <DIR>`: Install to a custom directory (default: `$XDG_CONFIG_HOME/.amp` or `$HOME/.amp`)
- `--no-modify-path`: Don't automatically add `ampup` to your PATH
- `--no-install-latest`: Don't automatically install the latest `ampd` version

## Usage

### Install Latest Version

> NOTE: By default, the `ampup` installer will also install the latest version of `ampd`.

```sh
ampup
```

or

```sh
ampup install
```

### Install Specific Version

```sh
ampup install v0.1.0
```

### List Installed Versions

```sh
ampup list
```

### Switch Between Versions

```sh
ampup use v0.1.0
```

### Uninstall a Version

```sh
ampup uninstall v0.1.0
```

### Build from Source

Build from the default repository's main branch:

```sh
ampup build
```

Build from a specific branch:

```sh
ampup build --branch main
```

Build from a specific commit:

```sh
ampup build --commit abc123
```

Build from a Pull Request:

```sh
ampup build --pr 123
```

Build from a local repository:

```sh
ampup build --path /path/to/amp
```

Build from a custom repository:

```sh
ampup build --repo username/fork
```

Combine options (e.g., custom repo + branch):

```sh
ampup build --repo username/fork --branch develop
```

Build with a custom version name:

```sh
ampup build --path . --name my-custom-build
```

Build with specific number of jobs:

```sh
ampup build --branch main --jobs 8
```

### Update ampup Itself

```sh
ampup update
```

## How It Works

`ampup` is a Rust-based version manager with a minimal bootstrap script for installation.

### Installation Methods

1. **Precompiled Binaries** (default): Downloads signed binaries from [GitHub releases](https://github.com/edgeandnode/amp/releases)
2. **Build from Source**: Clones and compiles the repository using Cargo

### Directory Structure

```
~/.amp/
├── bin/
│   ├── ampup            # Version manager binary
│   └── ampd             # Symlink to active version
├── versions/
│   ├── v0.1.0/
│   │   └── ampd         # Binary for v0.1.0
│   └── v0.2.0/
│       └── ampd         # Binary for v0.2.0
└── .version             # Tracks active version
```

## Supported Platforms

- Linux (x86_64, aarch64)
- macOS (aarch64/Apple Silicon)

## Environment Variables

- `GITHUB_TOKEN`: GitHub personal access token for private repository access
- `AMP_REPO`: Override repository (default: `edgeandnode/amp`)
- `AMP_DIR`: Override installation directory (default: `$XDG_CONFIG_HOME/.amp` or `$HOME/.amp`)

## Security

- macOS binaries are code-signed and notarized
- Private repository access uses GitHub's OAuth token mechanism

## Troubleshooting

### Command not found: ampup

Make sure the `ampup` binary is in your PATH. You may need to restart your terminal or run:

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

To uninstall ampd and ampup, simply delete you `.amp` directory (default: `$XDG_CONFIG_HOME/.amp` or `$HOME/.amp`):

```sh
rm -rf ~/.amp # or $XDG_CONFIG_HOME/.amp
```

Then remove the PATH entry from your shell configuration file.
