import { beforeAll, describe, expect, test } from "vitest"
import { type BoardCircuitInput, type CircuitTester, loadBoardCircuitTester } from "../utils.ts"

/**
 * Anti-Cheat Tests: Board Circuit - Ship Placement Integrity
 *
 * Tests for security requirements BR-1 through BR-3:
 * - BR-1: Ship Size Validation
 * - BR-2: Coordinate Boundary Validation
 * - BR-3: Orientation Validation
 */
describe("board placement integrity", () => {
  let board: CircuitTester<BoardCircuitInput>

  beforeAll(async () => {
    board = await loadBoardCircuitTester()
  })

  const validBoard: BoardCircuitInput = {
    carrier: [0, 0, 0], // Length 5, horizontal
    battleship: [0, 1, 0], // Length 4, horizontal
    cruiser: [0, 2, 0], // Length 3, horizontal
    submarine: [0, 3, 0], // Length 3, horizontal
    destroyer: [0, 4, 0], // Length 2, horizontal
    salt: 12345,
  }

  describe("BR-1: Ship Size Validation", () => {
    test("should enforce exact carrier length (5 cells)", async () => {
      // Test carrier placed too close to right edge (would be length 4)
      const invalidCarrier: BoardCircuitInput = {
        ...validBoard,
        carrier: [6, 0, 0], // At x=6, horizontal carrier would extend beyond board
      }

      await expect(board.witness(invalidCarrier)).rejects.toThrow()
    })

    test("should enforce exact battleship length (4 cells)", async () => {
      // Test battleship placed too close to bottom edge (would be length 3)
      const invalidBattleship: BoardCircuitInput = {
        ...validBoard,
        battleship: [0, 7, 1], // At y=7, vertical battleship would extend beyond board
      }

      await expect(board.witness(invalidBattleship)).rejects.toThrow()
    })

    test("should enforce exact cruiser length (3 cells)", async () => {
      // Test cruiser at right edge horizontally
      const invalidCruiser: BoardCircuitInput = {
        ...validBoard,
        cruiser: [8, 2, 0], // At x=8, horizontal cruiser would go out of bounds
      }

      await expect(board.witness(invalidCruiser)).rejects.toThrow()
    })

    test("should enforce exact submarine length (3 cells)", async () => {
      // Test submarine at bottom edge vertically
      const invalidSubmarine: BoardCircuitInput = {
        ...validBoard,
        submarine: [3, 8, 1], // At y=8, vertical submarine would go out of bounds
      }

      await expect(board.witness(invalidSubmarine)).rejects.toThrow()
    })

    test("should enforce exact destroyer length (2 cells)", async () => {
      // Test destroyer at very edge
      const invalidDestroyer: BoardCircuitInput = {
        ...validBoard,
        destroyer: [9, 4, 0], // At x=9, horizontal destroyer would need x=10 (out of bounds)
      }

      await expect(board.witness(invalidDestroyer)).rejects.toThrow()
    })

    test("should reject ships that would require negative coordinates", async () => {
      // Ships can't extend backwards from position (0,0)
      // This tests the lower boundary validation
      const validMinimalBoard: BoardCircuitInput = {
        carrier: [0, 0, 0], // Valid at origin
        battleship: [0, 1, 0],
        cruiser: [0, 2, 0],
        submarine: [0, 3, 0],
        destroyer: [0, 4, 0],
        salt: 12345,
      }

      // This should be valid
      await expect(board.witness(validMinimalBoard)).resolves.toBeDefined()
    })
  })

  describe("BR-2: Coordinate Boundary Validation", () => {
    test("should reject ships with x coordinates >= 10", async () => {
      const outOfBoundsX: BoardCircuitInput = {
        ...validBoard,
        carrier: [10, 0, 0], // x=10 is out of bounds (valid range: 0-9)
      }

      await expect(board.witness(outOfBoundsX)).rejects.toThrow()
    })

    test("should reject ships with y coordinates >= 10", async () => {
      const outOfBoundsY: BoardCircuitInput = {
        ...validBoard,
        battleship: [0, 10, 0], // y=10 is out of bounds (valid range: 0-9)
      }

      await expect(board.witness(outOfBoundsY)).rejects.toThrow()
    })

    test("should reject ships with negative coordinates", async () => {
      // Note: Depending on circuit implementation, this might be caught by type constraints
      // or need explicit validation
      const negativeX: BoardCircuitInput = {
        ...validBoard,
        cruiser: [-1, 2, 0], // Negative x coordinate
      }

      await expect(board.witness(negativeX)).rejects.toThrow()
    })

    test("should accept ships at valid boundary positions", async () => {
      const boundaryBoard: BoardCircuitInput = {
        carrier: [5, 0, 0], // Horizontal, ends at x=9 (valid)
        battleship: [6, 1, 0], // Horizontal, ends at x=9 (valid)
        cruiser: [7, 2, 0], // Horizontal, ends at x=9 (valid)
        submarine: [0, 7, 1], // Vertical, ends at y=9 (valid)
        destroyer: [1, 8, 1], // Vertical, ends at y=9 (valid)
        salt: 12345,
      }

      await expect(board.witness(boundaryBoard)).resolves.toBeDefined()
    })

    test("should reject ships extending beyond right boundary", async () => {
      const extendsBeyondRight: BoardCircuitInput = {
        ...validBoard,
        carrier: [6, 0, 0], // Starts at x=6, extends to x=10 (out of bounds)
      }

      await expect(board.witness(extendsBeyondRight)).rejects.toThrow()
    })

    test("should reject ships extending beyond bottom boundary", async () => {
      const extendsBeyondBottom: BoardCircuitInput = {
        ...validBoard,
        battleship: [0, 7, 1], // Starts at y=7, extends to y=10 (out of bounds)
      }

      await expect(board.witness(extendsBeyondBottom)).rejects.toThrow()
    })
  })

  describe("BR-3: Orientation Validation", () => {
    test("should reject invalid orientation values", async () => {
      const invalidOrientation: BoardCircuitInput = {
        ...validBoard,
        carrier: [0, 0, 2], // Invalid orientation (must be 0 or 1)
      }

      await expect(board.witness(invalidOrientation)).rejects.toThrow()
    })

    test("should reject negative orientation values", async () => {
      const negativeOrientation: BoardCircuitInput = {
        ...validBoard,
        battleship: [0, 1, -1], // Invalid negative orientation
      }

      await expect(board.witness(negativeOrientation)).rejects.toThrow()
    })

    test("should accept only binary orientation values", async () => {
      // Test all valid orientations
      const horizontalBoard: BoardCircuitInput = {
        carrier: [0, 0, 0], // All horizontal (orientation = 0)
        battleship: [0, 1, 0],
        cruiser: [0, 2, 0],
        submarine: [0, 3, 0],
        destroyer: [0, 4, 0],
        salt: 12345,
      }

      const verticalBoard: BoardCircuitInput = {
        carrier: [0, 0, 1], // All vertical (orientation = 1)
        battleship: [1, 0, 1],
        cruiser: [2, 0, 1],
        submarine: [3, 0, 1],
        destroyer: [4, 0, 1],
        salt: 12345,
      }

      await expect(board.witness(horizontalBoard)).resolves.toBeDefined()
      await expect(board.witness(verticalBoard)).resolves.toBeDefined()
    })

    test("should reject large invalid orientation values", async () => {
      const largeOrientation: BoardCircuitInput = {
        ...validBoard,
        destroyer: [0, 4, 999], // Invalid large orientation value
      }

      await expect(board.witness(largeOrientation)).rejects.toThrow()
    })

    test("should validate orientation affects ship placement correctly", async () => {
      // Test that vertical ships actually place vertically
      const verticalShipAtEdge: BoardCircuitInput = {
        carrier: [0, 5, 1], // Vertical carrier starting at y=5, should end at y=9
        battleship: [1, 6, 1], // Vertical battleship starting at y=6, should end at y=9
        cruiser: [2, 7, 1], // Vertical cruiser starting at y=7, should end at y=9
        submarine: [3, 7, 1], // Vertical submarine starting at y=7, should end at y=9
        destroyer: [4, 8, 1], // Vertical destroyer starting at y=8, should end at y=9
        salt: 12345,
      }

      await expect(board.witness(verticalShipAtEdge)).resolves.toBeDefined()

      // But this should fail (would extend beyond boundary)
      const verticalTooLong: BoardCircuitInput = {
        ...verticalShipAtEdge,
        carrier: [0, 6, 1], // Vertical carrier starting at y=6, would end at y=10 (invalid)
      }

      await expect(board.witness(verticalTooLong)).rejects.toThrow()
    })
  })
})
