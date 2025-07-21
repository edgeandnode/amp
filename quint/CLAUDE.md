# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Summary

This directory contains semi-formal models for verification of Nozzle behavior, written in the Quint specification language. Quint is similar to TLA+ with Apalache and is used for formal verification of system properties.

## Key Components

### Files
- `segments.qnt`: Main specification file containing modules for modeling segments of blockchain data
- `util.qnt`: Utility module with helper functions
- `justfile`: Contains commands for type checking and running the models

## Commands

### Type Check the Model
```bash
just check
```
This runs `quint typecheck segments.qnt` to verify the model is syntactically correct.

### Run the Model
```bash
just run
```
This executes `quint run segments.qnt --main=model --max-steps=3 --invariants=inv --verbosity=3` to verify invariants hold for up to 3 steps.

## Architecture

### Module Structure
1. **segment**: Defines the basic `Segment` type representing a contiguous range of blocks with associated hashes
2. **chain**: Models chains of adjacent segments with well-formedness constraints
3. **table**: Represents sets of chains, distinguishing between canonical chains and forks
4. **model**: The main verification module that defines state variables, invariants, and actions

### Key Types
- `Segment`: Contains start/end block numbers and their associated hashes
- `Chain`: A list of adjacent segments forming a continuous sequence
- `Table`: A set of chains representing the overall state

### Verification Focus
The models verify properties like:
- Well-formedness of segments (valid block ranges, distinct hashes)
- Adjacency constraints in chains (proper hash linkage between segments)
- Table structure (at most one canonical chain, fork management)

## Development Notes

- The models use the Quint specification language - consult the [Quint docs](https://quint-lang.org/docs/why) for syntax
- The `TODO` comments indicate areas still under development
- The `BlockNumbers` and `BlockHashes` ranges (0-8) are simplified for model checking efficiency
