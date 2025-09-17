# Battleship ZkSnark Implementation Documentation

This document provides comprehensive technical documentation for the trustless Battleship game implementation using ZK-SNARKs for anti-cheat mechanisms.

## System Overview

### Architecture
The system implements a complete trustless Battleship game using:
- **Circom circuits** for zero-knowledge proofs of valid game states
- **Solidity smart contracts** for on-chain game logic and proof verification
- **TypeScript test suite** using Vitest for comprehensive circuit and contract testing
- **Groth16 proving system** for efficient proof generation and verification

### Core Principle
Every player response in the game requires a ZK-SNARK proof, ensuring no cheating is possible. The system proves:
1. **Board validity**: Ship placements follow game rules (no overlaps, within bounds)
2. **Shot responses**: Hit/miss/sunk claims are truthful based on private board state

## Circuit Architecture

### 1. Board Validation Circuit (`board.circom`)

**Purpose**: Proves a board commitment represents a valid 5-ship arrangement

**Public Outputs**:
- `commitment`: Poseidon hash of ship positions and salt

**Private Inputs**:
- Ship positions: `carrier[3]`, `battleship[3]`, `cruiser[3]`, `submarine[3]`, `destroyer[3]`
- Each ship: `[x, y, orientation]` where orientation: 0=horizontal, 1=vertical
- `salt`: Random value for commitment uniqueness

**Validation Logic**:
1. **Individual ship validation**: Each ship placement checked for bounds and orientation
2. **Overlap detection**: Optimized O(n²) coordinate uniqueness check using encoded coordinates
3. **Commitment generation**: Poseidon hash of all ship data + salt

**Key Optimizations**:
- Coordinate encoding: `x*10 + y` for efficient comparison
- Early exit on invalid placements
- 136 pairwise comparisons for 17 total ship coordinates

### 2. Impact Verification Circuit (`impact.circom`)

**Purpose**: Verifies hit/miss/sunk claims using commitment chain to track game state

**Public Inputs**:
- `previousCommitment`: Previous game state hash
- `targetX`, `targetY`: Shot coordinates (0-9)
- `claimedResult`: 0=miss, 1=hit, 2=sunk
- `claimedShipId`: 0-4 if sunk, 255 otherwise
- `boardCommitment`: Original board commitment for validation

**Public Outputs**:
- `newCommitment`: New game state hash after shot
- `remainingShips`: Ships remaining after shot

**Private Inputs**:
- `ships[5][3]`: All ship positions [x, y, orientation]
- `previousHitCounts[5]`: Hit counts per ship before shot
- `salt`: Commitment salt

**Verification Steps**:
1. **Previous state verification**: Validate previousCommitment matches previousHitCounts + salt
2. **Ship coordinate generation**: Generate all ship cell positions
3. **Hit detection**: Check if shot coordinates hit any ship
4. **State transition**: Calculate new hit counts and check for sunk ships
5. **Result validation**: Verify claimed result matches actual impact
6. **New commitment**: Generate and verify new state commitment

**Commitment Chain**:
- State tracked as `Poseidon([hitCount0, hitCount1, hitCount2, hitCount3, hitCount4, salt])`
- Ensures continuous game state integrity
- Prevents replay attacks and state manipulation

### 3. Supporting Circuits

#### Ship Validation (`ship.circom`)
- `ValidateShip(shipLength)`: Validates individual ship placement
- Ensures ships are within 10x10 board bounds
- Validates orientation (0 or 1)

#### Utility Functions (`utils.circom`)
- `InBounds()`: Coordinate boundary checking (0-9)
- `CoordEqual()`: Coordinate comparison
- `GenerateShipCoords(shipLength)`: Generate all ship cell positions
- `ValidOrientation()`: Ensure orientation is binary (0 or 1)

## Smart Contract Architecture

### Main Contract (`Battleship.sol`)

**Core Components**:
- **Game State**: Tracks 2-player games with board commitments, stakes, and move history
- **Proof Verification**: Integrates Groth16 verifiers for board and unoact proofs
- **Turn Management**: Enforces proper game flow and turn order

**Key Structures**:
```solidity
struct Game {
    address[2] players;
    bytes32[2] boardCommitments;
    uint256[2] shotGrids; // 100-bit grid per player for duplicate shot prevention
    uint256 stakeAmount;
    uint256 prizePool;
    uint8 lastShotX, lastShotY;
    address lastPlayer;
    uint8 startingPlayer;
    address winner;
}

struct BoardProof {
    uint256[2] piA;
    uint256[2][2] piB;
    uint256[2] piC;
    uint256[1] publicInputs; // [commitment]
}

struct ImpactProof {
    uint256[2] piA;
    uint256[2][2] piB;
    uint256[2] piC;
    uint256[7] publicInputs; // [previousCommitment, targetX, targetY, claimedResult, claimedShipId, boardCommitment, newCommitment, remainingShips]
}
```

**Game Flow**:
1. **createGame()**: Create game with board proof, deposit stake
2. **joinGame()**: Join with matching stake and board proof (random starting player selected)
3. **attack()**: Fire initial shot (starting player only)
4. **respondAndCounter()**: Respond to shot with proof + counter-attack
5. **forfeitGame()**: Forfeit game and give opponent victory
6. **cancelGame()**: Cancel game before second player joins (creator only)
7. Auto-victory when defender has 0 ships remaining

**Security Features**:
- All responses require valid ZK-SNARK proofs
- Commitment matching prevents board substitution
- Turn validation prevents unauthorized moves
- Duplicate shot prevention via shot grid tracking (100-bit grid per player)
- Coordinate validation for shot responses
- Automatic prize distribution on victory
- Starting player randomization using block entropy

### Verifier Contracts

**BoardVerifier.sol**:
- Groth16 verifier for board validation circuit
- Input: `[commitment]` (1 element)
- Generated by snarkjs from circuit trusted setup

**ImpactVerifier.sol**:
- Groth16 verifier for impact verification circuit  
- Input: `[previousCommitment, targetX, targetY, claimedResult, claimedShipId, boardCommitment, newCommitment, remainingShips]` (8 elements: 6 inputs + 2 outputs)
- Generated by snarkjs from circuit trusted setup

## Test Infrastructure

### Circuit Testing (`test/circuits/`)

**Framework**: Vitest with TypeScript integration

**Test Structure**:
- `board.test.ts`: Board validation circuit tests
- `impact.test.ts`: Impact verification circuit tests
- `utils.ts`: Circuit testing utilities and helpers

**Circuit Tester Class**:
```typescript
class CircuitTester<I> {
  witness(input: I): Promise<WitnessOutput>  // Generate witness
  prove(input: I): Promise<ProofOutput>      // Generate proof
  verify(proof, signals): Promise<boolean>   // Verify proof
}
```

**Testing Capabilities**:
- Direct circuit witness calculation
- Full proof generation and verification
- Comprehensive constraint validation
- Edge case and error condition testing
- BoardSimulation utility for game state testing
- Anti-cheat scenario validation

### Smart Contract Testing (`contracts/test/`)

**Framework**: Forge (Foundry) with Solidity

**Test Suites**:
- `BattleshipIntegration.t.sol`: Full game scenarios
- `BattleshipGameLoop.t.sol`: Turn mechanics
- `BattleshipEdgeCases.t.sol`: Error conditions
- `BattleshipCircuitValidation.t.sol`: Proof validation

**Helper Infrastructure**:
- `BattleshipTestHelper.sol`: Common test utilities
- `MockVerifiers.sol`: Mock proofs for unit testing

## Circuit Testing Infrastructure

### BoardSimulation Testing Utility

The implementation includes a sophisticated `BoardSimulation` utility for comprehensive game state testing:

**Features**:
- Complete game state simulation with ship placement
- Hit/miss/sunk detection and validation
- Commitment chain tracking throughout game progression
- Anti-cheat scenario testing with override capabilities
- Visual board representation for debugging

**Key Methods**:
```typescript
interface BoardSimulation {
  fireShot(x: number, y: number, override?: ShotOverride): Promise<ExpectedResult>
  getRemainingShips(): number
  getCommitment(): string
  getHitCounts(): Array<number>
  isShipSunk(shipId: number): boolean
  getCellState(x: number, y: number): CellState
  visualizeBoard(): string
}
```

**Anti-Cheat Testing**:
The simulation supports cheat attempt validation:
- `cheatMiss()`: Claim miss when actually hit
- `cheatHit()`: Claim hit when actually miss  
- `cheatSunk()`: Premature sunk claims
- `cheatHitCounts()`: Manipulate previous hit counts
- `cheatOverride()`: Custom proof manipulation

### Circuit Testing Utilities

**Circuit Tester Class** (`utils.ts`):
- Singleton pattern for efficient test execution
- WASM witness calculator integration
- Full Groth16 proof generation and verification
- Support for both board and shot circuits

**Utility Functions**:
- `validShips()`: Generate valid ship placements
- `overlappingShips()`: Create invalid overlapping configurations  
- `outOfBoundsShips()`: Generate boundary violation scenarios
- `poseidonHash()`: Commitment generation utilities
- `validateShipPlacement()`: Complete validation logic

## Build System and Workflow

### Package.json Scripts

**Circuit Operations**:
```bash
pnpm run compile:board    # Compile board circuit with circom
pnpm run compile:impact     # Compile impact circuit with circom
pnpm run build           # Compile both circuits
```

**Trusted Setup**:
```bash
pnpm run setup:board      # Generate initial proving key
pnpm run setup:impact       # Generate initial proving key  
pnpm run setup           # Setup both circuits
```

**Key Contribution** (Security Critical):
```bash
pnpm run contribute:board # Add randomness to ceremony
pnpm run contribute:impact  # Add randomness to ceremony
pnpm run contribute      # Contribute to both ceremonies
```

**Verifier Export**:
```bash
pnpm run export:board    # Generate Solidity verifier
pnpm run export:impact   # Generate Solidity verifier
pnpm run export          # Export both verifiers
```

**Automated Build**:
```bash
./build-circuits.sh         # Automated build with entropy generation
./build-circuits.sh board   # Build only board circuit
./build-circuits.sh impact  # Build only impact circuit
```

**Testing**:
```bash
pnpm run test                # Run vitest (circuit tests)
pnpm run test:run            # Single test run
pnpm run test:watch          # Watch mode for development
(cd contracts && forge test) # Smart contract tests (from contracts directory)
```

### Automated Build Process

The `build-circuits.sh` script provides fully automated circuit compilation:

**Features**:
- Automatic entropy generation for trusted setup ceremony
- Sequential circuit compilation (board and impact)
- Automated proving key generation and contribution
- Support for individual circuit builds
- Error handling and build validation

**Process Flow**:
1. Generate random entropy using OpenSSL
2. Compile circuit with circom
3. Setup initial proving key with Powers of Tau
4. Contribute entropy to ceremony
5. Validate successful compilation

**Usage Examples**:
```bash
./build-circuits.sh          # Build both circuits
./build-circuits.sh board    # Build only board circuit  
./build-circuits.sh impact   # Build only impact circuit
```

### Build Artifacts

**Circuit Outputs** (`circom/build/`):
- `{circuit}.r1cs`: R1CS constraint system
- `{circuit}_js/{circuit}.wasm`: WASM witness generator
- `{circuit}_0000.zkey`: Initial proving key
- `{circuit}_final.zkey`: Final proving key (post-ceremony)
- `vkey_{circuit}.json`: Verification key

**Dependencies**:
- `powersOfTau28_hez_final_14.ptau`: Powers of tau ceremony file
- Pre-compiled for circuits up to 2^14 constraints

## Development Workflow Guide

### Quick Start Development Process

1. **Initial Setup**:
```bash
pnpm install                  # Install dependencies
```

2. **Circuit Development**:
```bash
pnpm run build               # Compile circuits
./build-circuits.sh          # Full automated build with setup
```

3. **Testing**:
```bash
pnpm test                     # Run circuit tests
pnpm run test:watch           # Development mode
(cd contracts && forge test)  # Smart contract tests (from contracts directory)
```

4. **Contract Development**:
```bash
(cd contracts && forge build)  # Compile contracts
(cd contracts && forge test)   # Run contract tests
pnpm run deploy:counter        # Deploy to local testnet
```

### Circuit Development Best Practices

1. **Iterative Testing**: Use `pnpm run test:watch` for rapid feedback
2. **Constraint Validation**: Always validate circuit constraints with edge cases
3. **Commitment Testing**: Verify commitment chain integrity in impact circuits
4. **Anti-Cheat Testing**: Use BoardSimulation with cheat scenarios
5. **Performance Testing**: Monitor proof generation times and circuit size

### Contract Development Best Practices

1. **Mock Testing**: Use MockVerifiers for unit tests
2. **Integration Testing**: Test full game scenarios with BattleshipTestHelper
3. **Security Testing**: Validate all proof verification paths
4. **Gas Optimization**: Monitor gas costs for verifier calls
5. **Error Handling**: Test all revert conditions and error states

### Dependencies and Tools

**Core Dependencies**:
- `circom`: Circuit compiler
- `snarkjs`: Proof generation and verification
- `circomlibjs`: Poseidon hash utilities
- `forge`: Solidity testing framework

**Testing Dependencies**:
- `vitest`: TypeScript test runner
- `circom_runtime`: WASM witness calculator
- Custom utilities for simulation and testing

## Game Constraints and Rules

### Ship Configuration
- **Carrier**: Length 5
- **Battleship**: Length 4  
- **Cruiser**: Length 3
- **Submarine**: Length 3
- **Destroyer**: Length 2
- **Total**: 17 occupied cells

### Board Rules
- 10x10 grid (coordinates 0-9)
- Ships placed horizontally (0) or vertically (1)
- No overlapping ships
- All ship cells must be within bounds

### Game Mechanics
- 2-player turn-based gameplay
- Equal stakes required from both players
- Random starting player selection
- Continuous turn alternation until victory
- Winner takes full prize pool

### Victory Conditions
- Game ends when any player's last ship is sunk
- Automatic victory detection via remaining ships count
- Prize distribution to attacking player (last to move)

## Security Considerations

### ZK-SNARK Security
- **Trusted Setup**: Requires secure ceremony for proving keys
- **Circuit Soundness**: Comprehensive constraint validation prevents cheating
- **Privacy**: Ship positions remain private until game end
- **Completeness**: Valid moves always generate valid proofs

### Smart Contract Security  
- **Proof Validation**: All responses require valid ZK-SNARK verification
- **State Integrity**: Commitment chain prevents state manipulation
- **Turn Validation**: Strict turn order enforcement
- **Economic Security**: Stake-based incentive alignment

### Known Limitations
- **Setup Trust**: Initial trusted setup ceremony required
- **Gas Costs**: Proof verification consumes significant gas
- **Proving Time**: Client-side proof generation takes time
- **Circuit Size**: Large circuits require more resources

## Development Guidelines

### Circuit Development
- Test individual templates before integration
- Use assertion templates for constraint validation
- Optimize for constraint count and proving time
- Validate all edge cases thoroughly

### Smart Contract Development  
- Always verify proofs on-chain
- Validate public inputs match expected ranges
- Handle all error conditions gracefully
- Test with both valid and invalid proofs

### Testing Strategy
- Unit test individual circuit components
- Integration test full game scenarios
- Property-based testing for edge cases
- Gas optimization testing for contract calls

## Performance Characteristics

### Circuit Complexity
- **Board Circuit**: ~5K-10K constraints (estimated)
- **Shot Circuit**: ~10K-15K constraints (estimated)
- **Proving Time**: 1-5 seconds per proof (browser)
- **Verification Time**: <1ms on-chain

### Gas Costs (estimated)
- **Board Verification**: ~200K gas
- **Shot Verification**: ~250K gas  
- **Game Creation**: ~150K gas
- **Move Execution**: ~300K gas total

## Future Enhancement Opportunities

### Circuit Optimizations
- Reduce constraint count through algorithmic improvements
- Implement batched proof verification
- Explore alternative proving systems (PLONK, FRI-based)

### Smart Contract Features
- Tournament modes with multiple games
- Spectator modes with selective reveal
- Replay system for game history
- Rating and leaderboard systems

### User Experience
- Web-based game interface
- Real-time game state updates
- Mobile-friendly proof generation
- Automated proof management

This documentation serves as the definitive reference for understanding and extending the Battleship ZK-SNARK implementation. All technical decisions, constraints, and architectural patterns are captured to enable future development by AI agents and human developers alike.

# TODO (for human):

- Consider optimizing smart contract input (some zk proof public inputs are known to the smart contract and could be passed from it instead)
- Effect-ify (e.g. ffi.ts, etc.)
- Create SDK for web app
