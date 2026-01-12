# solana-storage-proto (vendored)

This crate is vendored from [anza-xyz/agave](https://github.com/anza-xyz/agave/tree/master/storage-proto) to avoid the `protobuf-src` dependency which compiles C++ libprotobuf from source, causing slow build times.

## Differences from Upstream

This is a **minimal version** that only includes the generated protobuf types and the `Stored*` types with ser/de implementations.
The following upstream code was removed:

- `convert.rs` - Conversion impls between proto types and solana-transaction-status types
- Tests

## Regenerating Protobuf Bindings

If you modify the `.proto` files, regenerate the Rust bindings:

```bash
just gen-solana-storage-proto
```

This runs `cargo check` with `RUSTFLAGS="--cfg gen_proto"`, which triggers the build.rs to regenerate `src/proto/*.rs`.

## Updating from Upstream

When upstream `solana-storage-proto` has changes you need:

1. **Update proto files** (if changed):

   ```bash
   just update-solana-storage-proto           # from master
   just update-solana-storage-proto v2.0.0    # from a specific tag/branch
   ```

2. **Regenerate proto bindings**:

   ```bash
   just gen-solana-storage-proto
   ```

3. **Update dependency versions** in `Cargo.toml` if needed (solana-\* crates).

## Why Vendor?

The upstream crate uses `protobuf-src` which:

- Compiles C++ libprotobuf from source (~30+ seconds)
- Requires a C++ toolchain
- Runs on every clean build

By vendoring with pre-generated proto code, we:

- Eliminate the C++ compilation entirely
- Only need `protoc` when regenerating (via `just gen-solana-storage-proto`)
- Keep proto bindings in version control for reproducibility
