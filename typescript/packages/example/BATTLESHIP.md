# Battleship ZK-SNARK Implementation Documentation

This document provides comprehensive technical documentation for the trustless Battleship game implementation using ZK-SNARKs for anti-cheat mechanisms.

## Executive Summary

This implementation creates a fully trustless, on-chain Battleship game where players cannot cheat due to cryptographic guarantees provided by Zero-Knowledge Succinct Non-Interactive Arguments of Knowledge (ZK-SNARKs).

### Key Features

- **Zero Trust**: No trusted third parties required - all game rules enforced cryptographically
- **Complete Privacy**: Ship positions remain secret until game end
- **Anti-Cheat**: Impossible to make invalid moves, lie about hits/misses, or cheat in any way
- **Verifiable**: All game states and transitions are cryptographically provable
- **On-Chain**: Complete game state lives on Ethereum with economic incentives

### Core Components

- **Board Circuit** (`board.circom`): Validates ship placement and generates commitments
- **Impact Circuit** (`impact.circom`): Verifies hit/miss claims with state transitions
- **Smart Contract** (`Battleship.sol`): Manages game lifecycle and proof verification
- **Test Suite**: Comprehensive anti-cheat and game simulation tests

## Architecture Overview

The system consists of three main layers working together:

```
┌─────────────────────────────────────────────────────────────┐
│                        Client Layer                         │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │   Ship Setup    │  │ Proof Generation│  │   Game UI    │ │
│  └─────────────────┘  └─────────────────┘  └──────────────┘ │
└─────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│                      Circuit Layer                          │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │ Board Circuit   │  │ Impact Circuit  │  │ Proof System │ │
│  │ (Placement)     │  │ (Hit/Miss)      │  │ (Groth16)    │ │
│  └─────────────────┘  └─────────────────┘  └──────────────┘ │
└─────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│                     Blockchain Layer                        │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │ Battleship.sol  │  │ Board Verifier  │  │Shot Verifier │ │
│  │ (Game Logic)    │  │ Contract        │  │ Contract     │ │
│  └─────────────────┘  └─────────────────┘  └──────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Setup Phase**: Players generate board commitments using the Board Circuit
2. **Game Phase**: Each shot requires an Impact Circuit proof to verify hit/miss claims
3. **Verification**: Smart contract validates all proofs and manages game state
4. **Settlement**: Winner determined cryptographically, funds distributed automatically

## ZK-SNARK Circuits

### Board Circuit (`board.circom`)

**Purpose**: Validates that a player's ship arrangement follows game rules and generates cryptographic commitments.

**Inputs**:
- **Private**: Ship positions `[x, y, orientation]` for 5 ships + salt
- **Public**: None (only outputs)

**Outputs**:
- `commitment`: Poseidon hash of all ship positions + salt
- `initialStateCommitment`: Starting state for shot verification chain

**Validation Logic**:
1. **Ship Placement**: Each ship must be placed within 10x10 grid bounds
2. **Orientation**: Ships can only be horizontal (0) or vertical (1)
3. **No Overlaps**: All 17 ship coordinates must be unique (pairwise comparison)
4. **Ship Lengths**: Carrier(5), Battleship(4), Cruiser(3), Submarine(3), Destroyer(2)

### Impact Circuit (`impact.circom`)

**Purpose**: Verifies hit/miss claims and maintains commitment chain for game state integrity.

**Public Inputs**:
- `previousCommitment`: Previous game state hash
- `targetX`, `targetY`: Coordinates being attacked
- `claimedResult`: Player's claim (0=miss, 1=hit, 2=sunk)
- `claimedShipId`: Which ship was sunk (0-4, or 255 if none)
- `boardCommitment`: Original board commitment from setup

**Private Inputs**:
- `ships[5][3]`: All ship positions and orientations
- `previousHitCounts[5]`: Hit count per ship before this attack
- `salt`: Same salt used in board commitment

**Public Outputs**:
- `newCommitment`: Updated game state hash after this attack
- `remainingShips`: Number of ships not yet sunk

**Verification Logic**:
1. **Board Consistency**: Verify ships match original board commitment
2. **State Consistency**: Verify previous hit counts match previous commitment  
3. **Hit Detection**: Check if attack coordinates hit any ship
4. **State Transition**: Calculate new hit counts and remaining ships
5. **Claim Validation**: Verify player's claimed result matches actual outcome
6. **Commitment Update**: Generate new state commitment for next round

### Helper Circuits

**Ship Circuit (`ship.circom`)**:
- `ValidateShip`: Validates individual ship placement
- `GenerateShipCoords`: Calculates all coordinates for a ship

**Utils Circuit (`utils.circom`)**:
- `InBounds`: Checks coordinate bounds (0-9)
- `CoordEqual`: Coordinate equality comparison
- `ShipInBounds`: Validates all ship coordinates in bounds

### Game Lifecycle

1. **Creation**:
   - Player submits board proof and stake
   - Game created with unique ID
   - Waiting for second player

2. **Joining**:
   - Second player submits board proof and matching stake
   - Random starting player selected
   - Game begins immediately

3. **First Attack**:
   - Only allowed for first move
   - Starting player attacks with coordinates
   - Shot recorded in attacker's grid

4. **Respond & Counter**:
   - Defender submits impact proof for previous shot
   - Proof verified against multiple commitments
   - If game not over, defender counter-attacks
   - Process repeats

5. **Game End**:
   - Triggered when impact proof shows 0 remaining ships
   - Winner gets entire prize pool
   - Game marked as ended

## Game Mechanics

### Ship Types and Lengths
- **Carrier**: 5 cells
- **Battleship**: 4 cells  
- **Cruiser**: 3 cells
- **Submarine**: 3 cells
- **Destroyer**: 2 cells

**Total**: 17 cells occupied on 10×10 grid (83 empty cells)

### Placement Rules
- Ships can be horizontal (orientation=0) or vertical (orientation=1)
- Ships must be entirely within 10×10 grid (coordinates 0-9)
- Ships cannot overlap (all 17 coordinates must be unique)
- Ships cannot be adjacent (no additional constraint in current implementation)

### Game Flow

1. **Setup Phase**:
   - Each player generates board commitment with Board Circuit
   - Players join game by submitting board proof + stake
   - Starting player randomly selected

2. **Attack Phase**:
   - Current attacker chooses coordinates (0-9, 0-9)
   - Coordinates recorded in attacker's shot grid
   - Cannot attack same coordinates twice

3. **Response Phase**:
   - Defender generates impact proof using Impact Circuit
   - Proof reveals hit/miss/sunk result
   - State commitment updated with new hit counts
   - If no ships remain, attacker wins
   - If ships remain, defender counter-attacks

4. **Victory Conditions**:
   - Game ends when impact proof shows 0 remaining ships
   - Winner receives entire prize pool (both stakes)
   - Players can forfeit at any time (opponent wins)

### Turn Structure

```
Game Start (Random starter)
    ↓
Player A Attacks (coordinates)
    ↓  
Player B Responds (impact proof) → Game End?
    ↓                                  ↓
Player B Counter-Attacks           Winner Gets Prize Pool
    ↓
Player A Responds (impact proof) → Game End?
    ↓                                  ↓
Player A Counter-Attacks           Winner Gets Prize Pool
    ↓
    ... (continues until victory)
```

## Anti-Cheat Mechanisms

This implementation prevents all known cheating vectors through cryptographic constraints:

### 1. Invalid Ship Placement Prevention

**Problem**: Players might place ships outside bounds, overlap ships, or use wrong ship lengths.

**Solution**: Board Circuit validates:
- All coordinates within 0-9 bounds
- No coordinate appears twice (overlap detection)
- Correct ship lengths enforced by circuit structure
- Only valid orientations (0 or 1)

### 2. False Hit/Miss Claims Prevention

**Problem**: Players might lie about whether attacks hit their ships.

**Solution**: Impact Circuit cryptographically verifies:
- Exact ship coordinates against board commitment
- Whether attack coordinates intersect any ship
- Correct hit/miss/sunk determination
- State transition consistency

### 3. State Manipulation Prevention

**Problem**: Players might manipulate hit counts or game state between rounds.

**Solution**: Commitment chain prevents tampering:
- Each state commitment cryptographically binds hit counts
- Previous commitment must match current private state
- New commitment computed deterministically
- No way to forge valid state transitions

### 4. Ship Movement Prevention

**Problem**: Players might move ships during gameplay.

**Solution**: Board commitment prevents ship movement:
- Board commitment generated once during setup
- Impact proofs must use same ship positions
- Any change in ship positions breaks board commitment verification
- Immutable ship positions guaranteed

### 5. Double-Spend Shot Prevention

**Problem**: Players might attack same coordinates multiple times.

**Solution**: Smart contract shot grid tracking:
- 100-bit packed storage per player (10×10 grid)
- Each shot sets corresponding bit
- Duplicate shots automatically rejected
- On-chain enforcement with bit operations

### 6. Commitment Consistency Enforcement

**Problem**: Players might use different commitments in different proofs.

**Solution**: Multi-layer commitment verification:
- Board commitment stored on-chain during setup
- State commitments updated deterministically each round
- Impact proofs must reference exact stored commitments
- Cross-verification prevents inconsistencies

## Security Considerations

### Trust Assumptions

**Trusted Components**:
- **Circuit Implementation**: Circuits must correctly encode game rules
- **Trusted Setup**: Powers of Tau ceremony must be secure
- **Ethereum**: Blockchain security for contract execution

**Trustless Components**:
- **Player Actions**: No trust required between players
- **Game Outcomes**: Cryptographically guaranteed to be correct
- **Economic Settlement**: Automatic based on proofs

### Commitment Scheme Security

**Properties Required**:
- **Hiding**: Commitments reveal nothing about committed values
- **Binding**: Impossible to change committed value after reveal
- **Completeness**: Valid commitments always verify correctly
- **Soundness**: Invalid proofs always reject

**Implementation Security**:
- **Hash Function**: Poseidon hash resistant to known attacks
- **Randomness**: High-entropy salt prevents rainbow table attacks  
- **Determinism**: Same input always produces same commitment
- **Non-Malleability**: Cannot modify commitments without detection

**Key Management**:
- **Salt Generation**: Cryptographically secure random number generation
- **Salt Storage**: Client-side storage with backup mechanisms
- **Salt Loss**: Game becomes unplayable if salt lost
- **Recommendation**: Deterministic salt derivation from master seed
