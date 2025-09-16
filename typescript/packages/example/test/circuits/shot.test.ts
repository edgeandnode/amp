import { describe, expect, test } from "vitest"
import { loadBoardCircuitTester, loadShotCircuitTester, poseidonHashSalt } from "../utils.ts"

const standardShips = [
  [0, 0, 0], // Carrier: (0,0) to (4,0) horizontal
  [0, 1, 0], // Battleship: (0,1) to (3,1) horizontal
  [0, 2, 0], // Cruiser: (0,2) to (2,2) horizontal
  [0, 3, 0], // Submarine: (0,3) to (2,3) horizontal
  [0, 4, 0], // Destroyer: (0,4) to (1,4) horizontal
]

// Helper function to generate board commitment
const generateBoardCommitment = async (ships: Array<Array<number>>, salt: number): Promise<string> => {
  const board = await loadBoardCircuitTester()
  const witness = await board.witness({
    carrier: ships[0],
    battleship: ships[1],
    cruiser: ships[2],
    submarine: ships[3],
    destroyer: ships[4],
    salt,
  })
  return witness[1].toString()
}

describe("shot circuit", () => {
  describe("miss scenarios", () => {
    test("shot at empty water", async () => {
      const shot = await loadShotCircuitTester()
      const salt = 12345
      const previousHitCounts = [0, 0, 0, 0, 0]
      const previousCommitment = await poseidonHashSalt(previousHitCounts, salt)
      const boardCommitment = await generateBoardCommitment(standardShips, salt)

      await expect(shot.witness({
        // Public inputs
        previousCommitment,
        targetX: 5,
        targetY: 5,
        claimedResult: 0,
        claimedShipId: 255,
        // Private inputs
        boardCommitment,
        ships: standardShips,
        previousHitCounts,
        salt,
      })).resolves.toBeDefined()
    })

    test("shot at corner empty space", async () => {
      const shot = await loadShotCircuitTester()
      const salt = 54321
      const previousHitCounts = [1, 0, 2, 0, 1] // Some ships already hit
      const previousCommitment = await poseidonHashSalt(previousHitCounts, salt)
      const boardCommitment = await generateBoardCommitment(standardShips, salt)

      await expect(shot.witness({
        // Public inputs
        previousCommitment,
        boardCommitment,
        targetX: 9,
        targetY: 9,
        claimedResult: 0,
        claimedShipId: 255,
        // Private inputs
        ships: standardShips,
        previousHitCounts,
        salt,
      })).resolves.toBeDefined()
    })
  })

  describe("hit scenarios", () => {
    test("first hit on carrier", async () => {
      const shot = await loadShotCircuitTester()
      const salt = 98765
      const previousHitCounts = [0, 0, 0, 0, 0]
      const previousCommitment = await poseidonHashSalt(previousHitCounts, salt)
      const boardCommitment = await generateBoardCommitment(standardShips, salt)

      await expect(shot.witness({
        // Public inputs
        previousCommitment,
        boardCommitment,
        targetX: 2,
        targetY: 0,
        claimedResult: 1,
        claimedShipId: 255,
        // Private inputs
        ships: standardShips,
        previousHitCounts,
        salt,
      })).resolves.toBeDefined()
    })

    test("hit partially damaged battleship", async () => {
      const shot = await loadShotCircuitTester()
      const salt = 11111
      const previousHitCounts = [2, 1, 0, 0, 0] // Battleship already hit once
      const previousCommitment = await poseidonHashSalt(previousHitCounts, salt)
      const boardCommitment = await generateBoardCommitment(standardShips, salt)

      await expect(shot.witness({
        // Public inputs
        previousCommitment,
        boardCommitment,
        targetX: 3,
        targetY: 1,
        claimedResult: 1,
        claimedShipId: 255,
        // Private inputs
        ships: standardShips,
        previousHitCounts,
        salt,
      })).resolves.toBeDefined()
    })

    test("hit destroyer", async () => {
      const shot = await loadShotCircuitTester()
      const salt = 22222
      const previousHitCounts = [0, 0, 0, 0, 0]
      const previousCommitment = await poseidonHashSalt(previousHitCounts, salt)
      const boardCommitment = await generateBoardCommitment(standardShips, salt)

      await expect(shot.witness({
        // Public inputs
        previousCommitment,
        boardCommitment,
        targetX: 1,
        targetY: 4,
        claimedResult: 1,
        claimedShipId: 255,
        // Private inputs
        ships: standardShips,
        previousHitCounts,
        salt,
      })).resolves.toBeDefined()
    })
  })

  describe("sunk scenarios", () => {
    test("sink destroyer with second hit", async () => {
      const shot = await loadShotCircuitTester()
      const salt = 33333
      const previousHitCounts = [0, 0, 0, 0, 1] // Destroyer hit once
      const previousCommitment = await poseidonHashSalt(previousHitCounts, salt)
      const boardCommitment = await generateBoardCommitment(standardShips, salt)

      await expect(shot.witness({
        // Public inputs
        previousCommitment,
        boardCommitment,
        targetX: 0,
        targetY: 4,
        claimedResult: 2,
        claimedShipId: 4,
        // Private inputs
        ships: standardShips,
        previousHitCounts,
        salt,
      })).resolves.toBeDefined()
    })

    test("sink cruiser with final hit", async () => {
      const shot = await loadShotCircuitTester()
      const salt = 44444
      const previousHitCounts = [1, 0, 2, 0, 0] // Cruiser hit twice
      const previousCommitment = await poseidonHashSalt(previousHitCounts, salt)
      const boardCommitment = await generateBoardCommitment(standardShips, salt)

      await expect(shot.witness({
        // Public inputs
        previousCommitment,
        boardCommitment,
        targetX: 2,
        targetY: 2,
        claimedResult: 2,
        claimedShipId: 2,
        // Private inputs
        ships: standardShips,
        previousHitCounts,
        salt,
      })).resolves.toBeDefined()
    })

    test("sink carrier with final hit", async () => {
      const shot = await loadShotCircuitTester()
      const salt = 55555
      const previousHitCounts = [4, 2, 0, 3, 2] // Carrier hit 4 times, destroyer sunk, submarine sunk
      const previousCommitment = await poseidonHashSalt(previousHitCounts, salt)
      const boardCommitment = await generateBoardCommitment(standardShips, salt)

      await expect(shot.witness({
        // Public inputs
        previousCommitment,
        boardCommitment,
        targetX: 4,
        targetY: 0,
        claimedResult: 2,
        claimedShipId: 0,
        // Private inputs
        ships: standardShips,
        previousHitCounts,
        salt,
      })).resolves.toBeDefined()
    })
  })

  describe("game over scenarios", () => {
    test("sink final ship ends game", async () => {
      const shot = await loadShotCircuitTester()
      const salt = 66666
      const previousHitCounts = [5, 4, 3, 3, 1] // Only destroyer partially hit
      const previousCommitment = await poseidonHashSalt(previousHitCounts, salt)
      const boardCommitment = await generateBoardCommitment(standardShips, salt)

      await expect(shot.witness({
        // Public inputs
        previousCommitment,
        boardCommitment,
        targetX: 1,
        targetY: 4,
        claimedResult: 2,
        claimedShipId: 4,
        // Private inputs
        ships: standardShips,
        previousHitCounts,
        salt,
      })).resolves.toBeDefined()
    })
  })

  describe("invalid shot claims", () => {
    test("claim hit but shot misses", async () => {
      const shot = await loadShotCircuitTester()
      const salt = 77777
      const previousHitCounts = [0, 0, 0, 0, 0]
      const previousCommitment = await poseidonHashSalt(previousHitCounts, salt)
      const boardCommitment = await generateBoardCommitment(standardShips, salt)

      await expect(shot.witness({
        // Public inputs
        previousCommitment,
        targetX: 9,
        targetY: 9,
        claimedResult: 1,
        claimedShipId: 255,
        // Private inputs
        boardCommitment,
        ships: standardShips,
        previousHitCounts,
        salt,
      })).rejects.toThrow("Assert Failed")
    })

    test("claim miss but shot actually hits", async () => {
      const shot = await loadShotCircuitTester()
      const salt = 88888
      const previousHitCounts = [0, 0, 0, 0, 0]
      const previousCommitment = await poseidonHashSalt(previousHitCounts, salt)
      const boardCommitment = await generateBoardCommitment(standardShips, salt)

      await expect(shot.witness({
        // Public inputs
        previousCommitment,
        targetX: 0,
        targetY: 0,
        claimedResult: 0,
        claimedShipId: 255,
        // Private inputs
        boardCommitment,
        ships: standardShips,
        previousHitCounts,
        salt,
      })).rejects.toThrow("Assert Failed")
    })

    test("wrong ship id when sinking", async () => {
      const shot = await loadShotCircuitTester()
      const salt = 99999
      const previousHitCounts = [0, 0, 0, 0, 1] // Destroyer partially hit
      const previousCommitment = await poseidonHashSalt(previousHitCounts, salt)
      const boardCommitment = await generateBoardCommitment(standardShips, salt)

      await expect(shot.witness({
        // Public inputs
        previousCommitment,
        targetX: 0,
        targetY: 4,
        claimedResult: 2,
        claimedShipId: 2,
        // Private inputs
        boardCommitment,
        ships: standardShips,
        previousHitCounts,
        salt,
      })).rejects.toThrow("Assert Failed")
    })
  })

  describe("edge cases", () => {
    test("shot at coordinate boundary", async () => {
      const shot = await loadShotCircuitTester()
      const salt = 30303
      const previousHitCounts = [0, 0, 0, 0, 0]
      const previousCommitment = await poseidonHashSalt(previousHitCounts, salt)
      const boardCommitment = await generateBoardCommitment(standardShips, salt)

      // This should actually be valid since it correctly identifies the hit
      await expect(shot.witness({
        // Public inputs
        previousCommitment,
        targetX: 0,
        targetY: 0,
        claimedResult: 1,
        claimedShipId: 255,
        // Private inputs
        boardCommitment,
        ships: standardShips,
        previousHitCounts,
        salt,
      })).resolves.toBeDefined()
    })

    test("shot at maximum coordinates", async () => {
      const shot = await loadShotCircuitTester()
      const salt = 40404
      const previousHitCounts = [0, 0, 0, 0, 0]
      const previousCommitment = await poseidonHashSalt(previousHitCounts, salt)
      const boardCommitment = await generateBoardCommitment(standardShips, salt)

      await expect(shot.witness({
        // Public inputs
        previousCommitment,
        targetX: 9,
        targetY: 9,
        claimedResult: 0,
        claimedShipId: 255,
        // Private inputs
        boardCommitment,
        ships: standardShips,
        previousHitCounts,
        salt,
      })).resolves.toBeDefined()
    })

    test("multiple ships partially damaged", async () => {
      const shot = await loadShotCircuitTester()
      const salt = 50505
      const previousHitCounts = [2, 1, 2, 1, 1] // All ships partially hit
      const previousCommitment = await poseidonHashSalt(previousHitCounts, salt)
      const boardCommitment = await generateBoardCommitment(standardShips, salt)

      await expect(shot.witness({
        // Public inputs
        previousCommitment,
        targetX: 2,
        targetY: 1,
        claimedResult: 1,
        claimedShipId: 255,
        // Private inputs
        boardCommitment,
        ships: standardShips,
        previousHitCounts,
        salt,
      })).resolves.toBeDefined()
    })
  })

  describe("commitment chain validation", () => {
    test("valid commitment chain progression", async () => {
      const shot = await loadShotCircuitTester()
      const salt = 60606

      let hitCounts = [0, 0, 0, 0, 0]
      const commitment1 = await poseidonHashSalt(hitCounts, salt)

      await expect(shot.witness({
        previousCommitment: commitment1,
        targetX: 9,
        targetY: 9,
        claimedResult: 0,
        claimedShipId: 255,
        boardCommitment: await generateBoardCommitment(standardShips, salt),
        ships: standardShips,
        previousHitCounts: hitCounts,
        salt,
      })).resolves.toBeDefined()

      // Shot 2: Hit carrier
      const previousCounts = [...hitCounts]
      hitCounts = [1, 0, 0, 0, 0]

      await expect(shot.witness({
        // Public inputs
        previousCommitment: commitment1,
        targetX: 0,
        targetY: 0,
        claimedResult: 1,
        claimedShipId: 255,
        // Private inputs
        boardCommitment: await generateBoardCommitment(standardShips, salt),
        ships: standardShips,
        previousHitCounts: previousCounts,
        salt,
      })).resolves.toBeDefined()
    })

    test("broken commitment chain", async () => {
      const shot = await loadShotCircuitTester()
      const salt = 70707
      const hitCounts = [0, 0, 0, 0, 0]
      const wrongCommitment = await poseidonHashSalt([1, 0, 0, 0, 0], salt)
      // const correctCommitment = await poseidonHashSalt([0, 0, 0, 0, 0], salt)
      const boardCommitment = await generateBoardCommitment(standardShips, salt)

      await expect(shot.witness({
        // Public inputs
        previousCommitment: wrongCommitment,
        targetX: 0,
        targetY: 0,
        claimedResult: 1,
        claimedShipId: 255,
        // Private inputs
        boardCommitment,
        ships: standardShips,
        previousHitCounts: hitCounts,
        salt,
      })).rejects.toThrow("Assert Failed")
    })
  })
})
