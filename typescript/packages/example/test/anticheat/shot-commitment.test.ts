import { describe, expect, test } from "vitest"
import { cheatOverride, createBoardSimulation } from "../simulation.ts"
import { type BoardCircuitInput, poseidonHashSalt } from "../utils.ts"

/**
 * Anti-Cheat Tests: Shot Circuit - Commitment Chain Integrity
 *
 * Tests for security requirements SR-13 through SR-15:
 * - SR-13: Commitment Chain Continuity
 * - SR-14: Salt Consistency
 * - SR-15: State Commitment Binding
 */
describe("shot commitment chain integrity", () => {
  const standardBoard: BoardCircuitInput = {
    carrier: [0, 0, 0], // Horizontal
    battleship: [0, 1, 0], // Horizontal
    cruiser: [0, 2, 0], // Horizontal
    submarine: [0, 3, 0], // Horizontal
    destroyer: [0, 4, 0], // Horizontal
    salt: 55555,
  }

  describe("SR-13: Commitment Chain Continuity", () => {
    test("should validate commitment progression through multiple shots", async () => {
      const sim = await createBoardSimulation(standardBoard)

      // First shot
      await sim.fireShot(0, 0) // Hit carrier
      const commitment1 = sim.getCommitment()

      // Second shot
      await sim.fireShot(1, 0) // Hit carrier again
      const commitment2 = sim.getCommitment()

      // Third shot
      await sim.fireShot(5, 5) // Miss
      const commitment3 = sim.getCommitment()

      // Each commitment should be different (except miss keeps same state)
      expect(commitment1).not.toBe(commitment2)
      expect(commitment2).toBe(commitment3) // Miss doesn't change state
    })
  })

  describe("SR-14: Salt Consistency", () => {
    test("should maintain same salt throughout game", async () => {
      const sim = await createBoardSimulation(standardBoard)

      // Try to change salt mid-game
      const differentSaltOverride = cheatOverride({
        salt: 99999, // Different from standardBoard.salt (55555)
      })

      await expect(
        sim.fireShot(0, 0, differentSaltOverride),
      ).rejects.toThrow()
    })

    test("should reject salt changes between shots", async () => {
      const sim = await createBoardSimulation(standardBoard)

      // First shot with correct salt
      await sim.fireShot(5, 5) // Miss

      // Second shot with different salt
      const saltChangeOverride = cheatOverride({
        salt: standardBoard.salt + 1, // Slightly different salt
      })

      await expect(
        sim.fireShot(6, 6, saltChangeOverride),
      ).rejects.toThrow()
    })

    test("should enforce salt consistency across board and shot circuits", async () => {
      // Create board with specific salt
      const boardWithSalt = { ...standardBoard, salt: 12345 }
      const sim = await createBoardSimulation(boardWithSalt)

      // Try to use different salt in shot circuit
      const inconsistentSaltOverride = cheatOverride({
        salt: 67890, // Different from board circuit salt
      })

      await expect(
        sim.fireShot(0, 0, inconsistentSaltOverride),
      ).rejects.toThrow()
    })
  })

  describe("SR-15: State Commitment Binding", () => {
    test("should cryptographically bind commitment to exact state", async () => {
      const sim = await createBoardSimulation(standardBoard, [2, 1, 0, 0, 0])

      // Try to use a commitment for a different state
      const differentStateCommitment = await poseidonHashSalt([1, 2, 0, 0, 0], standardBoard.salt)

      const wrongBindingOverride = cheatOverride({
        previousCommitment: differentStateCommitment.toString(),
      })

      await expect(
        sim.fireShot(5, 5, wrongBindingOverride),
      ).rejects.toThrow()
    })

    test("should prevent commitment forgery", async () => {
      const sim = await createBoardSimulation(standardBoard, [1, 1, 1, 0, 0])

      // Try to use a manually crafted commitment
      const forgedCommitmentOverride = cheatOverride({
        previousCommitment: "123456789", // Fake commitment
      })

      await expect(
        sim.fireShot(7, 7, forgedCommitmentOverride),
      ).rejects.toThrow()
    })

    test("should validate commitment matches provided hit counts exactly", async () => {
      const sim = await createBoardSimulation(standardBoard, [3, 2, 1, 0, 0])

      // Use correct commitment but wrong hit counts
      const correctCommitment = sim.getCommitment()

      const mismatchedStateOverride = cheatOverride({
        previousCommitment: correctCommitment,
        previousHitCounts: [3, 2, 0, 1, 0],
      })

      await expect(
        sim.fireShot(8, 8, mismatchedStateOverride),
      ).rejects.toThrow()
    })

    test("should enforce deterministic commitment generation", async () => {
      // Same state should always produce same commitment
      const state = [2, 1, 1, 0, 1]
      const salt = standardBoard.salt

      const commitment1 = await poseidonHashSalt(state, salt)
      const commitment2 = await poseidonHashSalt(state, salt)

      expect(commitment1.toString()).toBe(commitment2.toString())
    })

    test("should detect commitment manipulation attempts", async () => {
      const sim = await createBoardSimulation(standardBoard, [1, 0, 0, 0, 0])

      // Try various commitment manipulation tactics
      const manipulationTests = [
        "0x" + "0".repeat(64), // All zeros
        "0x" + "f".repeat(64), // All ones
        "999999999999999999", // Large number
        "0", // Zero
        "-1", // Negative
      ]

      for (const fakeCommitment of manipulationTests) {
        const manipulationOverride = cheatOverride({
          previousCommitment: fakeCommitment,
        })

        await expect(
          sim.fireShot(9, 9, manipulationOverride),
        ).rejects.toThrow()
      }
    })

    test("should maintain commitment integrity in complex game states", async () => {
      // Start with complex state
      const sim = await createBoardSimulation(standardBoard, [4, 3, 2, 1, 1])

      // Make several moves and verify commitment chain
      await sim.fireShot(4, 0) // Sink carrier
      const afterCarrierSunk = sim.getCommitment()

      await sim.fireShot(3, 1) // Sink battleship
      const afterBattleshipSunk = sim.getCommitment()

      await sim.fireShot(9, 9) // Miss
      const afterMiss = sim.getCommitment()

      // Miss shouldn't change commitment
      expect(afterBattleshipSunk).toBe(afterMiss)

      // But sinking ships should change commitments
      expect(afterCarrierSunk).not.toBe(afterBattleshipSunk)
    })
  })
})
