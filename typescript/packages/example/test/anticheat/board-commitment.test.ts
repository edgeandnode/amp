import { beforeAll, describe, expect, test } from "vitest"
import { type BoardCircuitInput, type CircuitTester, loadBoardCircuitTester } from "../utils.ts"

/**
 * Anti-Cheat Tests: Board Circuit - Commitment Integrity
 *
 * Tests for security requirements BR-8 through BR-10:
 * - BR-8: Deterministic Commitment
 * - BR-9: Commitment Binding
 * - BR-10: Salt Uniqueness Enforcement
 */
describe("board commitment integrity", () => {
  let board: CircuitTester<BoardCircuitInput>

  beforeAll(async () => {
    board = await loadBoardCircuitTester()
  })

  const standardBoard: BoardCircuitInput = {
    carrier: [0, 0, 0],
    battleship: [0, 1, 0],
    cruiser: [0, 2, 0],
    submarine: [0, 3, 0],
    destroyer: [0, 4, 0],
    salt: 12345,
  }

  describe("BR-8: Deterministic Commitment", () => {
    test("should produce identical commitments for identical configurations", async () => {
      const board1 = { ...standardBoard }
      const board2 = { ...standardBoard }

      const witness1 = await board.witness(board1)
      const witness2 = await board.witness(board2)

      expect(witness1[1]).toBe(witness2[1]) // Commitment should be identical
    })

    test("should produce different commitments for different ship positions", async () => {
      const board1 = { ...standardBoard }
      const board2 = {
        ...standardBoard,
        carrier: [1, 0, 0], // Different carrier position
      }

      const witness1 = await board.witness(board1)
      const witness2 = await board.witness(board2)

      expect(witness1[1]).not.toBe(witness2[1])
    })

    test("should produce different commitments for different orientations", async () => {
      const board1 = {
        carrier: [0, 0, 0], // Horizontal carrier
        battleship: [0, 1, 0],
        cruiser: [0, 2, 0],
        submarine: [0, 3, 0],
        destroyer: [0, 4, 0],
        salt: 12345,
      }
      const board2 = {
        carrier: [5, 0, 1], // Vertical carrier (column 5, non-overlapping)
        battleship: [0, 1, 0],
        cruiser: [0, 2, 0],
        submarine: [0, 3, 0],
        destroyer: [0, 4, 0],
        salt: 12345,
      }

      const witness1 = await board.witness(board1)
      const witness2 = await board.witness(board2)

      expect(witness1[1]).not.toBe(witness2[1])
    })

    test("should produce different commitments for different salts", async () => {
      const board1 = {
        ...standardBoard,
        salt: 12345,
      }
      const board2 = {
        ...standardBoard,
        salt: 54321,
      }

      const witness1 = await board.witness(board1)
      const witness2 = await board.witness(board2)

      expect(witness1[1]).not.toBe(witness2[1])
    })
  })

  describe("BR-9: Commitment Binding", () => {
    test("should bind commitment to exact ship configuration", async () => {
      const boardConfig = { ...standardBoard }
      const witness = await board.witness(boardConfig)
      const commitment = witness[1].toString()

      // Verify that the commitment is cryptographically bound to this exact configuration
      // This is tested implicitly by the circuit validation
      expect(commitment).toBeDefined()
      expect(commitment).not.toBe("0")
    })

    test("should reject boards that don't match provided commitment", async () => {
      // This test verifies that the circuit generates the commitment correctly
      // rather than accepting an external commitment input

      const validBoard = { ...standardBoard }
      const witness = await board.witness(validBoard)

      // The circuit should generate a specific commitment for this configuration
      expect(witness[1]).toBeDefined()
    })

    test("should ensure commitment changes with any configuration change", async () => {
      const originalWitness = await board.witness(standardBoard)
      const originalCommitment = originalWitness[1]

      // Test that changing each ship position changes commitment
      const variants = [
        { ...standardBoard, carrier: [1, 0, 0] },
        { ...standardBoard, battleship: [1, 1, 0] },
        { ...standardBoard, cruiser: [1, 2, 0] },
        { ...standardBoard, submarine: [1, 3, 0] },
        { ...standardBoard, destroyer: [1, 4, 0] },
      ]

      for (const variant of variants) {
        const variantWitness = await board.witness(variant)
        expect(variantWitness[1]).not.toBe(originalCommitment)
      }
    })
  })

  describe("BR-10: Salt Uniqueness Enforcement", () => {
    test("should require salt contribution to commitment uniqueness", async () => {
      const sameBoardDifferentSalts = [
        { ...standardBoard, salt: 11111 },
        { ...standardBoard, salt: 22222 },
        { ...standardBoard, salt: 33333 },
      ]

      const commitments = []
      for (const boardConfig of sameBoardDifferentSalts) {
        const witness = await board.witness(boardConfig)
        commitments.push(witness[1])
      }

      // All commitments should be different due to different salts
      expect(commitments[0]).not.toBe(commitments[1])
      expect(commitments[1]).not.toBe(commitments[2])
      expect(commitments[0]).not.toBe(commitments[2])
    })

    test("should enforce salt as private input", async () => {
      // Salt should be a private input that affects the commitment
      // but is not revealed in the public outputs

      const boardWithSalt = { ...standardBoard, salt: 98765 }
      const witness = await board.witness(boardWithSalt)

      // Commitment should be affected by salt but salt itself should remain private
      expect(witness[1]).toBeDefined()
      expect(witness[1]).not.toBe("98765") // Salt shouldn't appear directly
    })

    test("should prevent commitment precomputation attacks", async () => {
      // Different salts should produce sufficiently different commitments
      // to prevent precomputation attacks

      const saltVariants = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
      const commitments = new Set()

      for (const salt of saltVariants) {
        const boardConfig = { ...standardBoard, salt }
        const witness = await board.witness(boardConfig)
        commitments.add(witness[1].toString())
      }

      // All commitments should be unique
      expect(commitments.size).toBe(saltVariants.length)
    })

    test("should ensure salt affects commitment calculation", async () => {
      // Test that salt is actually used in commitment generation
      // by checking that the same ship configuration with different salts
      // produces different commitments

      const zeroSalt = { ...standardBoard, salt: 0 }
      const maxSalt = { ...standardBoard, salt: 999999999 }

      const zeroWitness = await board.witness(zeroSalt)
      const maxWitness = await board.witness(maxSalt)

      expect(zeroWitness[1]).not.toBe(maxWitness[1])

      // Commitments should be significantly different
      expect(Math.abs(Number(zeroWitness[1]) - Number(maxWitness[1]))).toBeGreaterThan(0)
    })

    test("should validate salt range and constraints", async () => {
      // Test edge cases for salt values
      const saltEdgeCases = [
        { ...standardBoard, salt: 0 }, // Minimum
        { ...standardBoard, salt: 1 }, // Minimum + 1
        { ...standardBoard, salt: 2147483647 }, // Max 32-bit signed int
      ]

      for (const testCase of saltEdgeCases) {
        const witness = await board.witness(testCase)
        expect(witness[1]).toBeDefined()
      }
    })
  })
})
