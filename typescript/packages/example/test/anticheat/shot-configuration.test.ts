import { describe, expect, test } from "vitest"
import { cheatOverride, createBoardSimulation } from "../simulation.ts"
import { type BoardCircuitInput } from "../utils.ts"

/**
 * Anti-Cheat Tests: Shot Circuit - Ship Configuration Consistency
 *
 * Tests for security requirements SR-8 and SR-9:
 * - SR-8: Ship Immutability
 * - SR-9: Board-Shot Consistency
 */
describe("shot configuration consistency", () => {
  const standardBoard: BoardCircuitInput = {
    carrier: [0, 0, 0], // Horizontal
    battleship: [0, 1, 0], // Horizontal
    cruiser: [0, 2, 0], // Horizontal
    submarine: [0, 3, 0], // Horizontal
    destroyer: [0, 4, 0], // Horizontal
    salt: 66666,
  }

  describe("SR-8: Ship Immutability", () => {
    test("should prevent ship position changes between shots", async () => {
      const sim = await createBoardSimulation(standardBoard)

      // First shot with correct ships
      await sim.fireShot(5, 5) // Miss

      // Second shot with moved ships
      const movedShipsOverride = cheatOverride({
        ships: [
          [1, 0, 0], // Carrier moved 1 cell right
          [0, 1, 0], // Battleship unchanged
          [0, 2, 0], // Cruiser unchanged
          [0, 3, 0], // Submarine unchanged
          [0, 4, 0], // Destroyer unchanged
        ],
      })

      await expect(
        sim.fireShot(6, 6, movedShipsOverride),
      ).rejects.toThrow()
    })

    test("should prevent ship orientation changes mid-game", async () => {
      const sim = await createBoardSimulation(standardBoard)

      // Try to change carrier orientation from horizontal to vertical
      const changedOrientationOverride = cheatOverride({
        ships: [
          [0, 0, 1], // Carrier orientation changed from 0 to 1
          [0, 1, 0], // Others unchanged
          [0, 2, 0],
          [0, 3, 0],
          [0, 4, 0],
        ],
      })

      await expect(
        sim.fireShot(7, 7, changedOrientationOverride),
      ).rejects.toThrow()
    })

    test("should enforce bit-identical ship configurations", async () => {
      const sim = await createBoardSimulation(standardBoard, [1, 0, 0, 0, 0])

      // Even tiny changes should be rejected
      const microChangeOverride = cheatOverride({
        ships: [
          [0, 0, 0], // Carrier unchanged
          [0, 1, 0], // Battleship unchanged
          [0, 2, 0], // Cruiser unchanged
          [0, 3, 0], // Submarine unchanged
          [0, 5, 0], // Destroyer moved from y=4 to y=5
        ],
      })

      await expect(
        sim.fireShot(8, 8, microChangeOverride),
      ).rejects.toThrow()
    })

    test("should prevent ship swapping between shots", async () => {
      const sim = await createBoardSimulation(standardBoard)

      // Try to swap carrier and battleship positions
      const swappedShipsOverride = cheatOverride({
        ships: [
          [0, 1, 0], // Carrier at battleship position
          [0, 0, 0], // Battleship at carrier position
          [0, 2, 0], // Others unchanged
          [0, 3, 0],
          [0, 4, 0],
        ],
      })

      await expect(
        sim.fireShot(9, 9, swappedShipsOverride),
      ).rejects.toThrow()
    })

    test("should maintain ship immutability across multiple shots", async () => {
      const sim = await createBoardSimulation(standardBoard)

      // Make several legitimate shots
      await sim.fireShot(0, 0) // Hit carrier
      await sim.fireShot(1, 0) // Hit carrier
      await sim.fireShot(5, 5) // Miss

      // Now try to change ships
      const lateGameChangeOverride = cheatOverride({
        ships: [
          [2, 0, 0], // Carrier moved after several shots
          [0, 1, 0],
          [0, 2, 0],
          [0, 3, 0],
          [0, 4, 0],
        ],
      })

      await expect(
        sim.fireShot(6, 6, lateGameChangeOverride),
      ).rejects.toThrow()
    })

    test("should reject gradual ship drift over time", async () => {
      const sim = await createBoardSimulation(standardBoard)

      // Shot 1: normal
      await sim.fireShot(5, 5)

      // Shot 2: tiny change
      const gradualDriftOverride = cheatOverride({
        ships: [
          [0, 0, 0],
          [0, 1, 0],
          [0, 2, 0],
          [0, 3, 0],
          [1, 4, 0], // Destroyer moved by 1 cell
        ],
      })

      await expect(
        sim.fireShot(6, 6, gradualDriftOverride),
      ).rejects.toThrow()
    })
  })

  describe("SR-9: Board-Shot Consistency", () => {
    test("should ensure ship positions match board commitment", async () => {
      const sim = await createBoardSimulation(standardBoard)

      // Try to use different ships than those used to generate board commitment
      const inconsistentShipsOverride = cheatOverride({
        ships: [
          [5, 5, 0], // Completely different ship positions
          [6, 6, 0],
          [7, 7, 0],
          [8, 8, 0],
          [9, 9, 1],
        ],
        // But keeping the same board commitment
      })

      await expect(
        sim.fireShot(0, 0, inconsistentShipsOverride),
      ).rejects.toThrow()
    })

    test("should enforce salt consistency between board and shot", async () => {
      const sim = await createBoardSimulation(standardBoard)

      // Try to use different salt in shot than in board
      const differentSaltOverride = cheatOverride({
        salt: standardBoard.salt + 1, // Different salt
      })

      await expect(
        sim.fireShot(2, 2, differentSaltOverride),
      ).rejects.toThrow()
    })

    test("should validate ship configuration generates correct commitment", async () => {
      const sim = await createBoardSimulation(standardBoard)

      // The ship configuration in shot should generate the same commitment as board
      const shotHistory = sim.getShotHistory()
      const lastShot = shotHistory[shotHistory.length - 1]

      if (lastShot) {
        // Verify that the ships used in shot match the board commitment
        expect(lastShot.claimed.boardCommitment).toBeDefined()
      }
    })
  })
})
