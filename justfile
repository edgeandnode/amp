# Default target - displays available commands and their descriptions
default:
    @just --list

# Format all Rust code
#
# Requires nightly toolchain for unstable features.
# See `rustfmt.toml` for details.
fmt:
    cargo +nightly fmt --all

# Clean build artifacts and temporary files
clean:
    cargo clean
