# Amp Distribution Manifest

## Purpose

The Amp distribution manifest is a JSON file that enables `ampup` to discover, download, and manage Amp components across different platforms. It serves as the single source of truth for:

- Available Amp components (automatically discovered from release assets)
- Component versions and build metadata
- Platform-specific binary downloads (auto-discovered per component)
- Binary integrity verification (SHA256 checksums)
- Platform-specific metadata (notarization status for macOS)

For the complete manifest structure definition, see the [JSON Schema](manifest.schema.json).

## How It Works

The manifest enables a standard discovery and install flow:

1. **Discovery**: `ampup` fetches the manifest from a well-known distribution URL
2. **Platform Detection**: `ampup` identifies the host platform (target triple)
3. **Component Selection**: User selects which component to install
4. **Binary Lookup**: `ampup` locates the appropriate binary URL for the host platform
5. **Download**: `ampup` downloads the binary from the URL
6. **Verification**: `ampup` verifies the SHA256 checksum matches the manifest
7. **Installation**: `ampup` installs the verified binary to the appropriate location

This architecture allows Amp to distribute pre-built binaries for multiple platforms while ensuring integrity and authenticity through cryptographic verification.

## Key Features

### Platform Support

The manifest supports multiple operating systems, ABIs, and architectures:

- **Linux**: GNU libc (glibc) and musl libc variants
- **macOS**: Intel and Apple Silicon (automatically marked as notarized)
- **Windows**: MSVC and GNU (MinGW) toolchains

Each platform has its own binary URL, checksum, size, and platform-specific metadata.

### Integrity Verification

Every binary includes a SHA256 checksum in the format `sha256:<hash>` that `ampup` verifies after download. This prevents:

- Corrupted downloads from network issues
- Tampering or man-in-the-middle attacks
- Accidental installation of incorrect binaries

The checksum format matches GitHub's digest field format, ensuring consistency with the source of truth.

**Security requirement**: All manifest URLs must use HTTPS, and `ampup` must verify checksums before executing any downloaded binary.

### macOS Notarization

macOS binaries distributed through the Amp release process are automatically marked with `"notarized": true` in the manifest. This indicates that:

- The binary has been code-signed and notarized by Apple
- The binary can run on macOS without Gatekeeper warnings
- Users can verify the binary's authenticity through macOS security features

Non-macOS platforms omit the `notarized` field (default: `false`).

### Version Management

The manifest tracks:

- **Workspace version**: Overall Amp distribution version
- **Component versions**: Individual component versions (typically match workspace version)
- **Manifest version**: Schema version for backward compatibility
- **Build timestamp**: When the binaries were built (ISO 8601 UTC timestamp)

This enables `ampup` to compare installed versions with available versions and download only what's needed.

## Manifest Distribution

Manifests are hosted at a well-known URL endpoint and generated automatically during the CI/CD build process:

1. **Build binaries** for all target platforms
2. **Upload binaries** to GitHub Releases
3. **Auto-discover components** by parsing release asset names
4. **Extract metadata** from GitHub API (checksums via digest field, file sizes, download URLs)
5. **Generate manifest** JSON with auto-discovered components and targets
6. **Validate manifest** against JSON Schema
7. **Publish manifest** to GitHub Release

The manifest format is versioned (`manifest_version` field) to support future schema evolution while maintaining backward compatibility.

## Component Discovery

The manifest generation is **component-agnostic** and automatically discovers components from release assets:

### Asset Naming Convention

Release assets must follow one of these naming patterns:

**Standard format (3-part):**

```
<component>-<os>-<arch>
```

**Extended format (4-part with explicit ABI):**

```
<component>-<os>-<abi>-<arch>
```

#### Supported Components

- **component**: Lowercase alphanumeric with hyphens, must start with a letter (e.g., `ampd`, `ampctl`, `ampup-gen-release-manifest`)

#### Supported OS Values

**Bare OS names (default to common ABI):**

- `linux` → defaults to GNU libc (`linux-gnu`)
- `darwin` → macOS

**Explicit OS+ABI combinations:**

- `linux-gnu` → Linux with GNU libc (glibc)
- `linux-musl` → Linux with musl libc (static linking)
- `windows-msvc` → Windows with MSVC toolchain
- `windows-gnu` → Windows with GNU toolchain (MinGW)

#### Supported Architecture Values

The following architecture names are recognized and normalized:

- `x86_64` or `amd64` → normalized to `x86_64`
- `aarch64` or `arm64` → normalized to `aarch64`

**Note**: The manifest generator automatically normalizes alternative architecture names to their canonical forms.

### Asset Naming Examples

**Linux (GNU libc):**

- `ampd-linux-x86_64` → component: `ampd`, target: `x86_64-unknown-linux-gnu`
- `ampctl-linux-aarch64` → component: `ampctl`, target: `aarch64-unknown-linux-gnu`

**Linux (musl libc, static):**

- `ampd-linux-musl-x86_64` → component: `ampd`, target: `x86_64-unknown-linux-musl`
- `ampctl-linux-musl-aarch64` → component: `ampctl`, target: `aarch64-unknown-linux-musl`

**macOS (Darwin):**

- `ampd-darwin-x86_64` → component: `ampd`, target: `x86_64-apple-darwin`, notarized: `true`
- `ampctl-darwin-aarch64` → component: `ampctl`, target: `aarch64-apple-darwin`, notarized: `true`

**Windows (MSVC):**

- `ampd-windows-msvc-x86_64` → component: `ampd`, target: `x86_64-pc-windows-msvc`
- `ampctl-windows-msvc-aarch64` → component: `ampctl`, target: `aarch64-pc-windows-msvc`

**Windows (GNU/MinGW):**

- `ampd-windows-gnu-x86_64` → component: `ampd`, target: `x86_64-pc-windows-gnu`
- `ampctl-windows-gnu-aarch64` → component: `ampctl`, target: `aarch64-pc-windows-gnullvm`

**Architecture normalization:**

- `ampup-linux-arm64` → component: `ampup`, target: `aarch64-unknown-linux-gnu` (arm64 normalized to aarch64)
- `ampd-darwin-amd64` → component: `ampd`, target: `x86_64-apple-darwin` (amd64 normalized to x86_64)

### Auto-Discovery Algorithm

1. **Scan all release assets** from GitHub API
2. **Parse asset names** matching supported patterns:
   - Standard (3-part): `^([a-z][a-z0-9-]+)-(linux|darwin|windows)-(x86_64|aarch64|arm64|amd64)$`
   - Extended (4-part): `^([a-z][a-z0-9-]+)-(linux|windows)-(gnu|musl|msvc)-(x86_64|aarch64|arm64|amd64)$`
3. **Extract component name** from first capture group
4. **Parse OS and ABI** (if present) to determine target environment
5. **Normalize architecture** (arm64→aarch64, amd64→x86_64)
6. **Map to Rust target triple** (e.g., `linux-musl-x86_64` → `x86_64-unknown-linux-musl`)
7. **Group by component** and collect all targets
8. **Generate component entries** with version, targets, and metadata

### Target Triple Mapping

The manifest generator maps asset names to Rust target triples following standard conventions:

- Linux: `{arch}-unknown-linux-{abi}` (e.g., `x86_64-unknown-linux-gnu`)
- macOS: `{arch}-apple-darwin` (e.g., `aarch64-apple-darwin`)
- Windows: `{arch}-pc-windows-{abi}` (e.g., `x86_64-pc-windows-msvc`)

### Platform Support

Components automatically support any target that has a corresponding release asset:

- **Available target**: Asset exists → included in manifest
- **Missing target**: Asset missing → omitted from manifest (platform not supported)

**Client behavior**: If `ampup` cannot find a target for the current platform, it reports:

```
Error: Platform 'powerpc-unknown-linux-gnu' not supported for component 'ampd'
Available targets: x86_64-unknown-linux-gnu, aarch64-unknown-linux-gnu, x86_64-apple-darwin, aarch64-apple-darwin
```

### Adding New Components

To add a new component to the distribution:

1. **Build the binary** for desired target platforms
2. **Name the assets** following one of the supported patterns:
   - Standard: `<component>-<os>-<arch>`
   - Extended: `<component>-<os>-<abi>-<arch>`
3. **Upload to GitHub Release** - the manifest generator will automatically discover and include the component

**Example**: To add `ampctl` component with Linux musl and macOS support:

```bash
# Build and name your binaries
ampctl-linux-musl-x86_64
ampctl-linux-musl-aarch64
ampctl-darwin-x86_64
ampctl-darwin-aarch64

# Upload to GitHub release
# The manifest generator will automatically create the component entry
```

No code changes required - the manifest generation is fully automatic!
