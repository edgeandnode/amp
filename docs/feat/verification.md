---
name: "verification"
description: "Verification for the extraction of blockchain data into raw datasets. Load when asking about data integrity, verification, cryptographic proofs, or validating extracted data"
type: feature
status: experimental
components: "crate:verification"
---

# Verification

## Summary

Amp verification cryptographically verifies that extracted blockchain data using the cryptographic primitives provided by the source chain. By recomputing block hashes and Merkle roots from stored data and comparing them against the values in block headers, verification catches corrupted, missing, or modified data before it affects downstream queries. Verification can run automatically during extraction or as a standalone CLI for independent dataset validation.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [What is Verified](#what-is-verified)
3. [What is Not Verified](#what-is-not-verified)
4. [Usage](#usage)
5. [Architecture](#architecture)
6. [Implementation](#implementation)
7. [Limitations](#limitations)
8. [References](#references)

## Key Concepts

- **Hash**: A unique fingerprint produced by an irreversible function. The same input always produces the same output, but any change to the input (omission, addition, reordering) produces a different output. Used to verify data integrity.
- **Commitment**: A hash used to prove data integrity, in the context of this document. The block header contains commitments (hashes) for all transactions and receipts in the block.
- **RLP Encoding**: Recursive Length Prefix encoding, the serialization format used by the EVM. This is the input format for the Keccak-256 hash function used for the Ethereum execution layer.
- **Merkle Patricia Trie**: A data structure that produces efficient proofs of element presence. It provides a root hash that acts as a commitment to the entire sequence of entries (transactions or receipts) in a block.
- **Block Hash**: The Keccak-256 hash of the RLP-encoded block header fields. Uniquely identifies a block and its contents.
- **Transactions Root**: The Merkle Patricia Trie root hash computed from all transactions in a block. Stored in the block header as a commitment to transaction presence and ordering.
- **Receipts Root**: The Merkle Patricia Trie root hash computed from all transaction receipts in a block. Stored in the block header as a commitment to execution results (status, gas used, logs).

## What is Verified

Verification recomputes cryptographic hashes from stored data and compares them against block header values:

| Verification      | Method                                                 | Proves                                                  |
| ----------------- | ------------------------------------------------------ | ------------------------------------------------------- |
| Block header hash | Keccak-256 of RLP-encoded header fields                | All header fields are correct and unmodified            |
| Transactions root | Reconstruct Merkle Patricia Trie from all transactions | All transactions present, correctly ordered, unmodified |
| Receipts root     | Reconstruct Merkle Patricia Trie from all receipts     | Execution results (status, gas, logs) are accurate      |

When all verifications pass, the dataset provides these guarantees:

- **Completeness**: No transactions or logs are missing
- **Ordering**: Transactions and logs appear in correct sequence
- **Integrity**: No data has been added, removed, or modified

## What is Not Verified

| Item             | Reason                                                                                                       |
| ---------------- | ------------------------------------------------------------------------------------------------------------ |
| EVM execution    | Transactions are not re-executed to verify state transitions or gas calculations                             |
| Consensus rules  | Proof-of-work/proof-of-stake, block timestamps, gas limits, and other consensus parameters are not validated |
| Canonical chain  | Block headers are verified internally but not proven to belong to the chain agreed upon by network consensus |

## Usage

Run verification against a block range:

```bash
ampctl verify --dataset=edgeandnode/ethereum_mainnet --start-block=0 --end-block=100000
```

Verification outputs progress and reports any blocks that fail cryptographic checks. When a block fails, the tool identifies which commitment (block hash, transactions root, or receipts root) did not match and can optionally fetch the corresponding block from an RPC endpoint for comparison.

## Architecture

### Per-Block Verification

For EVM RPC raw datasets, Amp extracts data into three tables: blocks, logs, and transactions. Verification operates on each table:

1. **Blocks table**: RLP-encode the header fields and compute the Keccak-256 hash. If it matches the stored block hash, the header fields are well-formed.

2. **Transactions table**: Reconstruct each transaction in its RLP-encoded format (handling multiple transaction types: legacy, EIP-2930, EIP-1559, EIP-4844, EIP-7702). Build the Merkle Patricia Trie and compute the root. If it matches the `transactions_root` in the block header, all transactions are present and correctly ordered.

3. **Logs table**: Re-encode logs and combine them with transaction data to reconstruct receipts. Build the Merkle Patricia Trie for receipts and compute the root. If it matches the `receipts_root` in the block header, all execution results are accurate.

### Segment Chain Verification

Amp stores data in Parquet files called [segments](../glossary.md#segment), where each segment covers a contiguous range of blocks. Per-block verification ensures individual block integrity, but segments must also form a coherent chain.

Every EVM block contains a parent block hash, linking blocks into a chain. Segments are similarly linked using the block hash and parent hash of their boundary blocks. This is stronger than checking block numbers alone, which cannot handle chain reorganizations where two blocks may have the same number but different contents.

For query execution, Amp selects segments that form a well-formed chain. Combined with per-block verification, this ensures queries execute over data that is internally consistent, properly sequenced, and free of duplicates.

## Implementation

### Verification Process

```
Block Data (from Amp)
        │
        ▼
┌───────────────────┐
│  RLP Encode       │
│  Header Fields    │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐     ┌─────────────────┐
│  Keccak-256 Hash  │────▶│ Compare to      │
└───────────────────┘     │ Stored Block    │
                          │ Hash            │
                          └─────────────────┘

Transaction Data (from Amp)
        │
        ▼
┌───────────────────┐
│  Reconstruct      │
│  Transaction      │
│  Envelopes        │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐     ┌─────────────────┐
│  Build Merkle     │────▶│ Compare to      │
│  Patricia Trie    │     │ transactions_   │
└───────────────────┘     │ root            │
                          └─────────────────┘

Log + Transaction Data (from Amp)
        │
        ▼
┌───────────────────┐
│  Reconstruct      │
│  Receipt          │
│  Envelopes        │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐     ┌─────────────────┐
│  Build Merkle     │────▶│ Compare to      │
│  Patricia Trie    │     │ receipts_root   │
└───────────────────┘     └─────────────────┘
```

### Supported Transaction Types

| Type   | Status        | Description                                                       |
| ------ | ------------- | ----------------------------------------------------------------- |
| `0x00` | Supported     | Legacy transactions                                               |
| `0x01` | Supported     | Access list ([EIP-2930](https://eips.ethereum.org/EIPS/eip-2930)) |
| `0x02` | Supported     | Dynamic fee ([EIP-1559](https://eips.ethereum.org/EIPS/eip-1559)) |
| `0x03` | Supported     | Blob ([EIP-4844](https://eips.ethereum.org/EIPS/eip-4844))        |
| `0x04` | Supported     | Set code ([EIP-7702](https://eips.ethereum.org/EIPS/eip-7702))    |
| `0x64` | Not supported | Arbitrum legacy (pre-Nitro)                                       |
| `0x65` | Not supported | Arbitrum contract                                                 |
| `0x66` | Not supported | Arbitrum retry                                                    |
| `0x67` | Not supported | Arbitrum submit retry                                             |
| `0x68` | Not supported | Arbitrum deposit (L1 to L2)                                       |
| `0x69` | Not supported | Arbitrum user (Classic)                                           |
| `0x6a` | Not supported | Arbitrum compressed (Nitro)                                       |
| `0x7e` | Not supported | OP Stack deposit (L1 to L2)                                       |

## Limitations

- **Canonical chain not proven**: Verification proves that block data is internally consistent with block header commitments, but does not prove those block headers belong to the canonical chain agreed upon by network consensus. A misconfigured or malicious RPC provider could serve internally consistent blocks that diverge from the canonical chain. Closing this gap requires integrating with a consensus-layer light client.

- **Cross-chain verification**: Verifying that an L2 transaction has been finalized on L1 requires checking the rollup protocol's finality guarantees, which varies by chain. This is not currently implemented.

- **L2 transaction types**: Arbitrum and OP Stack deposit transaction types are not yet supported for verification.

## References

- [data](data.md) - Dependency: Data lake architecture, segments, and revisions
- [provider-extractor-evm-rpc](provider-extractor-evm-rpc.md) - Related: EVM RPC data extraction
- [Blog: Amp Verifiable Extraction](../blog/verifiable-extraction.md) - Extended: Detailed explanation with verification examples
