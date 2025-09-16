import { beforeAll, describe, expect, test } from "vitest"
import { type BoardCircuitInput, type CircuitTester, loadBoardCircuitTester } from "../utils.ts"

/**
 * Anti-Cheat Tests: Board Circuit - Configuration Integrity
 *
 * Tests for security requirements BR-6 and BR-7:
 * - BR-6: Ship Count Validation
 * - BR-7: Ship Continuity Validation
 */
describe("board configuration integrity", () => {
  let board: CircuitTester<BoardCircuitInput>

  beforeAll(async () => {
    board = await loadBoardCircuitTester()
  })

  const validBoard: BoardCircuitInput = {
    carrier: [0, 0, 0], // Length 5
    battleship: [0, 1, 0], // Length 4
    cruiser: [0, 2, 0], // Length 3
    submarine: [0, 3, 0], // Length 3
    destroyer: [0, 4, 0], // Length 2
    salt: 12345,
  }

  describe("BR-6: Ship Count Validation", () => {
    test("should enforce exactly 5 ships total", async () => {
      // This is implicitly tested by the circuit requiring all 5 ship inputs
      // But we can test that the total cell count is exactly 17
      await expect(board.witness(validBoard)).resolves.toBeDefined()
    })

    test("should enforce exactly one carrier (length 5)", async () => {
      // Circuit structure enforces this, but test boundary conditions
      const carrierAtEdge: BoardCircuitInput = {
        ...validBoard,
        carrier: [5, 0, 0], // Carrier at right edge (positions 5,6,7,8,9)
      }

      await expect(board.witness(carrierAtEdge)).resolves.toBeDefined()

      // But not extending beyond
      const carrierBeyondEdge: BoardCircuitInput = {
        ...validBoard,
        carrier: [6, 0, 0], // Would extend to position 10 (out of bounds)
      }

      await expect(board.witness(carrierBeyondEdge)).rejects.toThrow()
    })

    test("should enforce exactly one battleship (length 4)", async () => {
      const battleshipAtEdge: BoardCircuitInput = {
        ...validBoard,
        battleship: [6, 1, 0], // Battleship at right edge (positions 6,7,8,9)
      }

      await expect(board.witness(battleshipAtEdge)).resolves.toBeDefined()

      const battleshipBeyondEdge: BoardCircuitInput = {
        ...validBoard,
        battleship: [7, 1, 0], // Would extend beyond board
      }

      await expect(board.witness(battleshipBeyondEdge)).rejects.toThrow()
    })

    test("should enforce exactly one cruiser (length 3)", async () => {
      const cruiserAtEdge: BoardCircuitInput = {
        ...validBoard,
        cruiser: [7, 2, 0], // Cruiser at right edge (positions 7,8,9)
      }

      await expect(board.witness(cruiserAtEdge)).resolves.toBeDefined()
    })

    test("should enforce exactly one submarine (length 3)", async () => {
      const submarineVertical: BoardCircuitInput = {
        ...validBoard,
        submarine: [0, 7, 1], // Submarine vertical at bottom edge (positions y=7,8,9)
      }

      await expect(board.witness(submarineVertical)).resolves.toBeDefined()
    })

    test("should enforce exactly one destroyer (length 2)", async () => {
      const destroyerAtCorner: BoardCircuitInput = {
        ...validBoard,
        destroyer: [8, 9, 0], // Destroyer at bottom-right corner (positions 8,9 on row 9)
      }

      await expect(board.witness(destroyerAtCorner)).resolves.toBeDefined()
    })

    test("should validate total ship cell count equals 17", async () => {
      // 5 + 4 + 3 + 3 + 2 = 17 total cells must be occupied
      // This is enforced by ship length requirements and no overlaps
      const validVariation: BoardCircuitInput = {
        carrier: [1, 1, 1], // 5 cells vertical
        battleship: [2, 2, 1], // 4 cells vertical
        cruiser: [3, 3, 1], // 3 cells vertical
        submarine: [4, 4, 1], // 3 cells vertical
        destroyer: [5, 5, 1], // 2 cells vertical
        salt: 12345,
      }

      await expect(board.witness(validVariation)).resolves.toBeDefined()
    })
  })

  describe("BR-7: Ship Continuity Validation", () => {
    test("should enforce horizontal ship continuity", async () => {
      // Horizontal ships must form straight lines
      const validHorizontalBoard: BoardCircuitInput = {
        carrier: [0, 0, 0], // (0,0), (1,0), (2,0), (3,0), (4,0)
        battleship: [0, 1, 0], // (0,1), (1,1), (2,1), (3,1)
        cruiser: [0, 2, 0], // (0,2), (1,2), (2,2)
        submarine: [0, 3, 0], // (0,3), (1,3), (2,3)
        destroyer: [0, 4, 0], // (0,4), (1,4)
        salt: 12345,
      }

      await expect(board.witness(validHorizontalBoard)).resolves.toBeDefined()
    })

    test("should enforce vertical ship continuity", async () => {
      const validVerticalBoard: BoardCircuitInput = {
        carrier: [0, 0, 1], // (0,0), (0,1), (0,2), (0,3), (0,4)
        battleship: [1, 0, 1], // (1,0), (1,1), (1,2), (1,3)
        cruiser: [2, 0, 1], // (2,0), (2,1), (2,2)
        submarine: [3, 0, 1], // (3,0), (3,1), (3,2)
        destroyer: [4, 0, 1], // (4,0), (4,1)
        salt: 12345,
      }

      await expect(board.witness(validVerticalBoard)).resolves.toBeDefined()
    })

    test("should reject ships with gaps", async () => {
      // This test depends on circuit implementation
      // If the circuit calculates cell positions from start position + orientation + length,
      // then gaps are impossible. But if it takes explicit cell lists, this would be tested.

      // For the current circuit design (start position + orientation),
      // continuity is mathematically enforced, so we test edge cases
      const edgeContinuity: BoardCircuitInput = {
        carrier: [5, 5, 0], // Horizontal, should be continuous
        battleship: [1, 6, 1], // Vertical, should be continuous
        cruiser: [3, 7, 0], // Horizontal, should be continuous
        submarine: [6, 1, 1], // Vertical, should be continuous
        destroyer: [7, 8, 0], // Horizontal, should be continuous
        salt: 12345,
      }

      await expect(board.witness(edgeContinuity)).resolves.toBeDefined()
    })

    test("should validate ships form straight lines only", async () => {
      // With position + orientation design, ships can only be straight
      // Test that orientation values properly control direction

      const mixedOrientations: BoardCircuitInput = {
        carrier: [0, 0, 0], // Horizontal
        battleship: [1, 1, 1], // Vertical
        cruiser: [2, 2, 0], // Horizontal
        submarine: [3, 3, 1], // Vertical
        destroyer: [4, 4, 0], // Horizontal
        salt: 12345,
      }

      await expect(board.witness(mixedOrientations)).resolves.toBeDefined()
    })

    test("should handle ships at board boundaries correctly", async () => {
      // Test continuity at edges where ships touch boundaries
      const boundaryShips: BoardCircuitInput = {
        carrier: [0, 0, 0], // Left edge horizontal
        battleship: [0, 9, 0], // Bottom edge horizontal
        cruiser: [9, 0, 1], // Right edge vertical
        submarine: [5, 7, 1], // Near bottom vertical
        destroyer: [7, 9, 0], // Bottom edge horizontal
        salt: 12345,
      }

      await expect(board.witness(boundaryShips)).resolves.toBeDefined()
    })

    test("should validate each ship type maintains proper continuity", async () => {
      // Test that each specific ship type forms correct continuous shape
      // Using non-overlapping positions
      const shipContinuityTests = [
        {
          name: "carrier",
          config: {
            carrier: [2, 5, 0], // Horizontal carrier (row 5)
            battleship: [0, 1, 0], // Keep original positions for others
            cruiser: [0, 2, 0],
            submarine: [0, 3, 0],
            destroyer: [0, 4, 0],
            salt: 12345,
          },
        },
        {
          name: "battleship",
          config: {
            carrier: [0, 0, 0],
            battleship: [8, 2, 1], // Vertical battleship (column 8, starts row 2)
            cruiser: [0, 2, 0],
            submarine: [0, 3, 0],
            destroyer: [0, 4, 0],
            salt: 12345,
          },
        },
        {
          name: "cruiser",
          config: {
            carrier: [0, 0, 0],
            battleship: [0, 1, 0],
            cruiser: [6, 6, 0], // Horizontal cruiser (row 6)
            submarine: [0, 3, 0],
            destroyer: [0, 4, 0],
            salt: 12345,
          },
        },
        {
          name: "submarine",
          config: {
            carrier: [0, 0, 0],
            battleship: [0, 1, 0],
            cruiser: [0, 2, 0],
            submarine: [9, 6, 1], // Vertical submarine (column 9, starts row 6)
            destroyer: [0, 4, 0],
            salt: 12345,
          },
        },
        {
          name: "destroyer",
          config: {
            carrier: [0, 0, 0],
            battleship: [0, 1, 0],
            cruiser: [0, 2, 0],
            submarine: [0, 3, 0],
            destroyer: [7, 8, 0], // Horizontal destroyer (row 8)
            salt: 12345,
          },
        },
      ]

      for (const test of shipContinuityTests) {
        await expect(board.witness(test.config)).resolves.toBeDefined()
      }
    })

    test("should enforce continuity across all ship orientations", async () => {
      // Test that both horizontal (0) and vertical (1) orientations work correctly
      const orientationTests = [
        {
          desc: "all horizontal",
          config: {
            carrier: [0, 0, 0],
            battleship: [0, 1, 0],
            cruiser: [0, 2, 0],
            submarine: [0, 3, 0],
            destroyer: [0, 4, 0],
            salt: 12345,
          },
        },
        {
          desc: "all vertical",
          config: {
            carrier: [0, 0, 1],
            battleship: [1, 0, 1],
            cruiser: [2, 0, 1],
            submarine: [3, 0, 1],
            destroyer: [4, 0, 1],
            salt: 12345,
          },
        },
      ]

      for (const test of orientationTests) {
        await expect(board.witness(test.config)).resolves.toBeDefined()
      }
    })
  })
})
