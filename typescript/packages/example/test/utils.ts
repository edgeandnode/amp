// @ts-ignore
import { WitnessCalculatorBuilder } from "circom_runtime"
import * as circomlib from "circomlibjs"
import * as fs from "fs/promises"
import * as path from "path"
import * as snarkjs from "snarkjs"

/**
 * Path to the build artifacts directory.
 */
const artifacts = path.resolve(import.meta.dirname, "..", "circom", "build")

/**
 * Load an artifact from the build artifacts directory.
 */
const loadArtifact = (artifact: string): Promise<Uint8Array> => {
  const file = path.join(artifacts, artifact)
  return fs.readFile(file)
}

/**
 * Load the WASM file for a given circuit.
 *
 * @param circuit - The circuit to load the WASM file for.
 */
const loadWasm = (circuit: string): Promise<Uint8Array> => {
  return loadArtifact(path.join(`${circuit}_js`, `${circuit}.wasm`))
}

/**
 * Load the ZKEY file for a given circuit.
 *
 * @param circuit - The circuit to load the ZKEY file for.
 */
const loadZkey = (circuit: string): Promise<Uint8Array> => {
  return loadArtifact(`${circuit}_final.zkey`)
}

/**
 * Load the VKEY file for a given circuit.
 *
 * @param circuit - The circuit to load the VKEY file for.
 */
const loadVkey = (circuit: string): Promise<Uint8Array> => {
  return loadArtifact(`vkey_${circuit}.json`)
}

/**
 * Options for the witness calculator.
 */
export interface WitnessCalculatorOptions {
  readonly memorySize?: number
  readonly sym?: boolean
  readonly sanityCheck?: boolean
  readonly logGetSignal?: (signal: string, pVal: number) => void
  readonly logSetSignal?: (signal: string, pVal: number) => void
  readonly logStartComponent?: (cIdx: number) => void
  readonly logFinishComponent?: (cIdx: number) => void
}

/**
 * Witness calculator interface.
 */
export interface WitnessCalculator<I = any> {
  readonly calculateWitness: (input: I, sanityCheck?: boolean) => Promise<WitnessOutput>
}

/**
 * Array of field elements: [w0, w1, w2, ..., wn] where each element is a
 * big integer in the finite field (typically Fp for some large prime p).
 *
 * 1. w0 = 1: First element is always 1 (the constant "one" wire)
 * 2. Public inputs: Next elements are the public inputs to the circuit
 * 3. Private inputs: Followed by private inputs (witnesses)
 * 4. Intermediate values: Remaining elements are computed intermediate
 *    values from gates
 *
 * @example
 * ```typescript
 *  [
 *    1,                    // w0 (constant one)
 *    publicInput1,         // w1
 *    publicInput2,         // w2
 *    privateInput1,        // w3
 *    privateInput2,        // w4
 *    intermediate1,        // w5 (computed value)
 *    intermediate2,        // w6 (computed value)
 *    ...
 *  ]
 * ```
 */
export type WitnessOutput = Array<bigint>

/**
 * Proof output
 */
export type ProofOutput = {
  proof: snarkjs.Groth16Proof
  publicSignals: snarkjs.PublicSignals
}

/**
 * Create a witness calculator for a given circuit.
 *
 * @param wasm - The WASM file for the circuit.
 * @param options - The options for the witness calculator.
 */
const createWitnessCalculator = async <I = any>(
  wasm: Uint8Array<ArrayBufferLike>,
  options?: WitnessCalculatorOptions,
): Promise<WitnessCalculator<I>> => {
  return await WitnessCalculatorBuilder(wasm, options)
}

/**
 * Circuit tester for a given circuit.
 *
 * @param circuit - The circuit to test.
 */
export class CircuitTester<I = any> {
  constructor(
    /**
     * Calculate the witness for the given input.
     */
    public readonly calculator: WitnessCalculator<I>,
    /**
     * The WASM file for the circuit.
     */
    public readonly wasm: Uint8Array<ArrayBufferLike>,
    /**
     * The ZKEY file for the circuit.
     */
    public readonly zkey: Uint8Array<ArrayBufferLike>,
    /**
     * The VKEY file for the circuit.
     */
    public readonly vkey: Uint8Array<ArrayBufferLike>,
  ) {}

  /**
   * Prove the circuit with the given input.
   *
   * @param input - The input to the circuit.
   */
  prove(input: I): Promise<ProofOutput> {
    return snarkjs.groth16.fullProve(input as any, this.wasm, this.zkey)
  }

  /**
   * Verify the proof with the given public signals to be valid.
   *
   * @param proof - The proof to verify.
   * @param publicSignals - The public signals to verify the proof with.
   */
  verify(proof: snarkjs.Groth16Proof, publicSignals: snarkjs.PublicSignals): Promise<boolean> {
    return snarkjs.groth16.verify(this.vkey, publicSignals, proof)
  }

  /**
   * Calculate the witness for the given input.
   *
   * @param input - The input to the circuit.
   * @param sanityCheck - Whether to perform sanity checks.
   * @returns
   */
  witness(input: I, sanityCheck?: boolean): Promise<WitnessOutput> {
    return this.calculator.calculateWitness(input, sanityCheck)
  }
}

/**
 * Shot circuit input
 */
export interface ShotCircuitInput {
  // Public inputs
  readonly previousCommitment: string
  readonly targetX: number
  readonly targetY: number
  readonly claimedResult: number
  readonly claimedShipId: number
  readonly boardCommitment: string
  // Private inputs
  readonly ships: Array<Array<number>>
  readonly previousHitCounts: Array<number>
  readonly salt: number
}

/**
 * Create a circuit tester for a given circuit.
 *
 * @param circuit - The circuit to create a tester for.
 */
const createCircuitTester = <I = any>(circuit: string): Promise<CircuitTester<I>> => {
  return new Promise((resolve, reject) => {
    Promise.all([
      loadWasm(circuit),
      loadZkey(circuit),
      loadVkey(circuit),
    ]).then(async ([wasm, zkey, vkey]) => {
      const calculator = await createWitnessCalculator(wasm)
      resolve(new CircuitTester(calculator, wasm, zkey, vkey))
    }).catch(reject)
  })
}

/**
 * Create a singleton circuit tester for a given circuit.
 *
 * @param circuit - The circuit to create a tester for.
 */
const createSingletonCircuitTester = <I = any>(circuit: string) => (): Promise<CircuitTester<I>> => {
  let tester: Promise<CircuitTester<I>> | undefined
  if (tester === undefined) {
    tester = createCircuitTester(circuit)
  }
  return tester
}

/**
 * Load the shot circuit tester instance.
 */
export const loadShotCircuitTester = createSingletonCircuitTester<ShotCircuitInput>("shot")

/**
 * Load the board circuit tester instance.
 */
export const loadBoardCircuitTester = createSingletonCircuitTester<BoardCircuitInput>("board")

/**
 * Board circuit input
 */
export interface BoardCircuitInput {
  readonly carrier: Array<number>
  readonly battleship: Array<number>
  readonly cruiser: Array<number>
  readonly submarine: Array<number>
  readonly destroyer: Array<number>
  readonly salt: number
}

let poseidon: Promise<circomlib.Poseidon> | undefined

/**
 * Generate a poseidon hash for the given inputs.
 */
export const poseidonHash = async (inputs: ReadonlyArray<circomlib.BigNumberish>): Promise<string> => {
  if (poseidon === undefined) {
    poseidon = circomlib.buildPoseidon()
  }

  const loaded = await poseidon
  return loaded.F.toString(loaded(inputs as any))
}

/**
 * Generate a poseidon hash for the given inputs with a salt.
 */
export const poseidonHashSalt = async (
  inputs: ReadonlyArray<circomlib.BigNumberish>,
  salt: number,
): Promise<string> => {
  return poseidonHash([...inputs, salt])
}

/**
 * Generate valid ship placement with a given salt
 */
export const validShips = (salt: number): BoardCircuitInput => ({
  carrier: [0, 0, 0],
  battleship: [0, 1, 0],
  cruiser: [0, 2, 0],
  submarine: [0, 3, 0],
  destroyer: [0, 4, 0],
  salt,
})

/**
 * Generate overlapping ship placement with a given salt
 */
export const overlappingShips = (salt: number): BoardCircuitInput => ({
  carrier: [0, 0, 0],
  battleship: [2, 0, 1],
  cruiser: [0, 2, 0],
  submarine: [0, 3, 0],
  destroyer: [0, 4, 0],
  salt,
})

/**
 * Generate out of bounds ship placement with a given salt
 */
export const outOfBoundsShips = (salt: number): BoardCircuitInput => ({
  carrier: [6, 0, 0],
  battleship: [0, 1, 0],
  cruiser: [0, 2, 0],
  submarine: [0, 3, 0],
  destroyer: [0, 4, 0],
  salt,
})

/**
 * Ship configuration type
 */
export type ShipConfig = [number, number, number] // [x, y, orientation]

/**
 * Ship lengths for each ship type
 */
export const SHIP_LENGTHS = [5, 4, 3, 3, 2] // carrier, battleship, cruiser, submarine, destroyer

/**
 * Generate all coordinates occupied by a ship
 */
export const getShipCoordinates = (ship: ShipConfig): Array<[number, number]> => {
  const [x, y, orientation] = ship
  const length = SHIP_LENGTHS[0] // This function needs the ship type, but for validation we'll handle it elsewhere
  const coords: Array<[number, number]> = []

  for (let i = 0; i < length; i++) {
    if (orientation === 0) { // horizontal
      coords.push([x + i, y])
    } else { // vertical
      coords.push([x, y + i])
    }
  }

  return coords
}

/**
 * Get coordinates occupied by a ship with known length
 */
export const getShipCoordinatesWithLength = (ship: ShipConfig, length: number): Array<[number, number]> => {
  const [x, y, orientation] = ship
  const coords: Array<[number, number]> = []

  for (let i = 0; i < length; i++) {
    if (orientation === 0) { // horizontal
      coords.push([x + i, y])
    } else { // vertical
      coords.push([x, y + i])
    }
  }

  return coords
}

/**
 * Check if a ship placement is within bounds
 */
export const isShipInBounds = (ship: ShipConfig, length: number): boolean => {
  const [x, y, orientation] = ship
  if (x < 0 || y < 0) {
    return false
  }

  if (orientation === 0) {
    return x + length <= 10 && y < 10
  }

  return x < 10 && y + length <= 10
}

/**
 * Check if two coordinate sets overlap
 */
export const doCoordinatesOverlap = (coords1: Array<[number, number]>, coords2: Array<[number, number]>): boolean => {
  for (const [x1, y1] of coords1) {
    for (const [x2, y2] of coords2) {
      if (x1 === x2 && y1 === y2) {
        return true
      }
    }
  }
  return false
}

/**
 * Validate a complete ship configuration
 */
export const validateShipPlacement = (ships: Array<ShipConfig>): { valid: boolean; errors: Array<string> } => {
  const errors: Array<string> = []

  if (ships.length !== 5) {
    errors.push(`Expected 5 ships, got ${ships.length}`)
    return { valid: false, errors }
  }

  const allCoordinates: Array<[number, number]> = []
  for (let i = 0; i < ships.length; i++) {
    const ship = ships[i]
    const length = SHIP_LENGTHS[i]
    const shipName = ["carrier", "battleship", "cruiser", "submarine", "destroyer"][i]

    // Check bounds
    if (!isShipInBounds(ship, length)) {
      errors.push(`${shipName} extends beyond board boundaries`)
      continue
    }

    // Check orientation
    if (ship[2] !== 0 && ship[2] !== 1) {
      errors.push(`${shipName} has invalid orientation ${ship[2]} (must be 0 or 1)`)
      continue
    }

    // Get coordinates for this ship
    const shipCoords = getShipCoordinatesWithLength(ship, length)

    // Check for overlaps with previous ships
    for (const coord of shipCoords) {
      if (allCoordinates.some(([x, y]) => x === coord[0] && y === coord[1])) {
        errors.push(`${shipName} overlaps with another ship at (${coord[0]}, ${coord[1]})`)
      }
    }

    allCoordinates.push.apply(shipCoords)
  }

  return { valid: errors.length === 0, errors }
}

/**
 * Generate a valid random ship placement
 */
export const generateValidShipPlacement = (seed: number): Array<ShipConfig> => {
  // Use seed for deterministic randomness in tests
  const rng = () => {
    seed = (seed * 9301 + 49297) % 233280
    return seed / 233280
  }

  const ships: Array<ShipConfig> = []
  const occupiedCoords = new Set<string>()

  for (let shipIndex = 0; shipIndex < 5; shipIndex++) {
    const length = SHIP_LENGTHS[shipIndex]
    while (true) {
      const orientation = Math.floor(rng() * 2) // 0 or 1
      const maxX = orientation === 0 ? 10 - length : 10
      const maxY = orientation === 1 ? 10 - length : 10

      const x = Math.floor(rng() * maxX)
      const y = Math.floor(rng() * maxY)

      const ship: ShipConfig = [x, y, orientation]
      const coords = getShipCoordinatesWithLength(ship, length)

      // Check if any coordinate is already occupied
      const overlaps = coords.some(([cx, cy]) => occupiedCoords.has(`${cx},${cy}`))

      if (!overlaps) {
        ships.push(ship)
        coords.forEach(([cx, cy]) => occupiedCoords.add(`${cx},${cy}`))
        break
      }
    }
  }

  return ships
}

/**
 * Get a coordinate that hits a specific ship
 */
export const getHitCoordinate = (ship: ShipConfig, shipLength: number, cellIndex: number = 0): [number, number] => {
  const coords = getShipCoordinatesWithLength(ship, shipLength)
  return coords[Math.min(cellIndex, coords.length - 1)]
}

/**
 * Get a coordinate that doesn't hit any ship (water)
 */
export const getWaterCoordinate = (ships: Array<ShipConfig>): [number, number] => {
  const occupiedCoords = new Set<string>()

  // Collect all occupied coordinates
  for (let i = 0; i < ships.length; i++) {
    const coords = getShipCoordinatesWithLength(ships[i], SHIP_LENGTHS[i])
    coords.forEach(([x, y]) => occupiedCoords.add(`${x},${y}`))
  }

  // Find first empty coordinate
  for (let y = 0; y < 10; y++) {
    for (let x = 0; x < 10; x++) {
      if (!occupiedCoords.has(`${x},${y}`)) {
        return [x, y]
      }
    }
  }

  // Fallback (shouldn't happen with valid ship placements)
  return [9, 9]
}
