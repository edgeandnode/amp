import {
  type BoardCircuitInput,
  type ImpactCircuitInput,
  loadBoardCircuitTester,
  loadImpactCircuitTester,
  poseidonHashSalt,
} from "./utils.ts"

// Types for board simulation
type CellState = "empty" | "ship" | "hit" | "miss"

interface ExpectedResult {
  readonly result: number // 0=miss, 1=hit, 2=sunk
  readonly shipId: number // 0-4 or 255
  readonly newHitCounts: Array<number>
  readonly remainingShips: number
}

interface ShotRecord {
  readonly x: number
  readonly y: number
  readonly claimed: ImpactCircuitInput
  readonly actual: ExpectedResult
  readonly circuitOutputs: {
    readonly newCommitment: string
    readonly remainingShips: number
  }
}

export interface BoardSimulation {
  // Shoot with optional override callback
  fireShot: (
    x: number,
    y: number,
    override?: (input: ImpactCircuitInput) => ImpactCircuitInput,
  ) => Promise<ExpectedResult>

  // State getters
  getRemainingShips: () => number
  getCommitment: () => string
  getHitCounts: () => Array<number>
  getShotHistory: () => Array<ShotRecord>
  getLastShot: () => ShotRecord | null

  // Board state queries
  getActualResultAt: (x: number, y: number) => ExpectedResult

  isShipSunk: (shipId: number) => boolean
  isCarrierSunk: () => boolean
  isBattleshipSunk: () => boolean
  isCruiserSunk: () => boolean
  isSubmarineSunk: () => boolean
  isDestroyerSunk: () => boolean

  getCellState: (x: number, y: number) => CellState
  visualizeBoard: () => string
}

export const createBoardSimulation = async (
  boardInput: BoardCircuitInput,
  initialHitCounts?: Array<number>,
): Promise<BoardSimulation> => {
  const board = await loadBoardCircuitTester()
  const impact = await loadImpactCircuitTester()

  const boardWitness = await board.witness(boardInput)
  const boardCommitment = `${boardWitness[1]}`
  const ships = [
    boardInput.carrier,
    boardInput.battleship,
    boardInput.cruiser,
    boardInput.submarine,
    boardInput.destroyer,
  ]

  // Ship lengths for sunk detection
  const shipLengths = [5, 4, 3, 3, 2] // carrier, battleship, cruiser, submarine, destroyer

  // Initialize internal state
  let previousHitCounts = initialHitCounts || [0, 0, 0, 0, 0]
  let previousCommitment = await poseidonHashSalt(previousHitCounts, boardInput.salt)
  let newCommitment = previousCommitment

  // Calculate remaining ships from initial hit counts
  let remainingShips = 0
  for (let i = 0; i < 5; i++) {
    if (previousHitCounts[i] < shipLengths[i]) {
      remainingShips++
    }
  }
  const shotHistory: Array<ShotRecord> = []

  // Initialize 10x10 board grid with ship positions
  const gameBoard: Array<Array<number>> = Array(10).fill(null).map(() => Array(10).fill(-1)) // -1 = empty
  const cellStates: Array<Array<CellState>> = Array(10).fill(null).map(() => Array(10).fill("empty"))

  // Place ships on board
  for (let shipIndex = 0; shipIndex < ships.length; shipIndex++) {
    const [x, y, orientation] = ships[shipIndex]
    const length = shipLengths[shipIndex]

    for (let i = 0; i < length; i++) {
      const cellX = orientation === 0 ? x + i : x // horizontal: x + i, vertical: x
      const cellY = orientation === 0 ? y : y + i // horizontal: y, vertical: y + i
      gameBoard[cellX][cellY] = shipIndex
      cellStates[cellX][cellY] = "ship"
    }
  }

  // Calculate actual result based on current board state
  const calculateActualResult = (x: number, y: number): ExpectedResult => {
    const shipId = gameBoard[x][y]

    if (shipId === -1) {
      // Miss
      return {
        result: 0,
        shipId: 255,
        newHitCounts: [...previousHitCounts],
        remainingShips,
      }
    }

    // Hit - update counts
    const newHitCounts = [...previousHitCounts]
    newHitCounts[shipId]++

    // Check if ship is sunk
    const isSunk = newHitCounts[shipId] === shipLengths[shipId]

    // Calculate remaining ships after this shot
    let remainingAfterShot = 0
    for (let i = 0; i < 5; i++) {
      if (newHitCounts[i] < shipLengths[i]) {
        remainingAfterShot++
      }
    }

    return {
      result: isSunk ? 2 : 1,
      shipId: isSunk ? shipId : 255,
      newHitCounts,
      remainingShips: remainingAfterShot,
    }
  }

  const fireShot = async (x: number, y: number, override?: (input: ImpactCircuitInput) => ImpactCircuitInput) => {
    // Validate coordinates
    if (x < 0 || x >= 10 || y < 0 || y >= 10) {
      throw new Error(`Invalid coordinates: (${x}, ${y})`)
    }

    // Check if cell has already been shot
    if (cellStates[x][y] === "hit" || cellStates[x][y] === "miss") {
      throw new Error(`Coordinates (${x}, ${y}) have already been shot`)
    }

    // Calculate actual result
    const actual = calculateActualResult(x, y)

    // Build default circuit input with correct values
    let shotInput: ImpactCircuitInput = {
      previousCommitment,
      targetX: x,
      targetY: y,
      claimedResult: actual.result,
      claimedShipId: actual.shipId,
      boardCommitment,
      ships,
      previousHitCounts,
      salt: boardInput.salt,
    }

    // Apply override if provided (for cheat testing)
    if (override) {
      shotInput = override(shotInput)
    }

    // Execute circuit
    const shotWitness = await impact.witness(shotInput)

    // Extract circuit outputs
    newCommitment = shotWitness[1].toString()
    const circuitRemainingShips = Number(shotWitness[2])

    // Update internal state (only if circuit succeeded)
    previousCommitment = newCommitment
    previousHitCounts = actual.newHitCounts
    remainingShips = actual.remainingShips

    // Update board cell state
    if (actual.result === 0) {
      cellStates[x][y] = "miss"
    } else {
      cellStates[x][y] = "hit"
    }

    // Record shot in history
    shotHistory.push({
      x,
      y,
      claimed: shotInput,
      actual,
      circuitOutputs: {
        newCommitment,
        remainingShips: circuitRemainingShips,
      },
    })

    return actual
  }

  // State getter methods
  const getRemainingShips = () => remainingShips
  const getCommitment = () => newCommitment
  const getHitCounts = () => [...previousHitCounts]
  const getShotHistory = () => [...shotHistory]
  const getLastShot = () => shotHistory.length > 0 ? shotHistory[shotHistory.length - 1] : null

  // Board state queries
  const getActualResultAt = (x: number, y: number) => calculateActualResult(x, y)
  const isShipSunk = (shipId: number) => previousHitCounts[shipId] === shipLengths[shipId]
  const getCellState = (x: number, y: number) => cellStates[x][y]

  const visualizeBoard = () => {
    let result = "  0123456789\n"
    for (let y = 0; y < 10; y++) {
      result += `${y} `
      for (let x = 0; x < 10; x++) {
        const state = cellStates[x][y]
        const char = state === "empty" ?
          "." :
          state === "ship" ?
          "S" :
          state === "hit"
          ? "X"
          : "O"
        result += char
      }
      result += "\n"
    }
    return result
  }

  return {
    fireShot,
    getRemainingShips,
    getCommitment,
    getHitCounts,
    getShotHistory,
    getLastShot,
    getActualResultAt,
    isShipSunk,
    isCarrierSunk: () => isShipSunk(0),
    isBattleshipSunk: () => isShipSunk(1),
    isCruiserSunk: () => isShipSunk(2),
    isSubmarineSunk: () => isShipSunk(3),
    isDestroyerSunk: () => isShipSunk(4),
    getCellState,
    visualizeBoard,
  }
}

export const cheatOverride = (override: Partial<ImpactCircuitInput>) => (input: ImpactCircuitInput) => ({
  ...input,
  ...override,
})

export const cheatMiss = () =>
  cheatOverride({
    claimedResult: 0,
    claimedShipId: 255,
  })

export const cheatHit = (shipId = 255) =>
  cheatOverride({
    claimedResult: 1,
    claimedShipId: shipId,
  })

export const cheatSunk = (shipId: number) =>
  cheatOverride({
    claimedResult: 2,
    claimedShipId: shipId,
  })

export const cheatHitCounts = (hitCounts: Array<number>) =>
  cheatOverride({
    previousHitCounts: hitCounts,
  })
