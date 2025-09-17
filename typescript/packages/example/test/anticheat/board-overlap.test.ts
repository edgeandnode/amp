import { describe, expect, test } from "vitest"
import { type BoardCircuitInput, loadBoardCircuitTester } from "../utils.ts"

/**
 * Anti-Cheat Tests: Board Circuit - Ship Overlap Prevention
 *
 * Tests for security requirements BR-4 and BR-5:
 * - BR-4: Coordinate Uniqueness
 * - BR-5: Complete Overlap Detection
 */
describe("board overlap prevention", () => {
  const validBoard: BoardCircuitInput = {
    carrier: [0, 0, 0], // Horizontal (0,0) to (4,0)
    battleship: [0, 1, 0], // Horizontal (0,1) to (3,1)
    cruiser: [0, 2, 0], // Horizontal (0,2) to (2,2)
    submarine: [0, 3, 0], // Horizontal (0,3) to (2,3)
    destroyer: [0, 4, 0], // Horizontal (0,4) to (1,4)
    salt: 12345,
  }

  describe("BR-4: Coordinate Uniqueness", () => {
    test("should reject direct ship cell overlaps", async () => {
      const board = await loadBoardCircuitTester()
      // Place carrier and battleship at same position
      const directOverlap: BoardCircuitInput = {
        ...validBoard,
        carrier: [0, 0, 0], // (0,0) to (4,0)
        battleship: [0, 0, 0], // Same position - overlap at (0,0)
      }

      await expect(board.witness(directOverlap)).rejects.toThrow()
    })

    test("should reject partial overlaps between ships", async () => {
      const board = await loadBoardCircuitTester()
      // Carrier and battleship sharing some cells
      const partialOverlap: BoardCircuitInput = {
        ...validBoard,
        carrier: [0, 0, 0], // (0,0) to (4,0)
        battleship: [2, 0, 0], // (2,0) to (5,0) - overlaps at (2,0), (3,0), (4,0)
      }

      await expect(board.witness(partialOverlap)).rejects.toThrow()
    })

    test("should reject single cell overlaps", async () => {
      const board = await loadBoardCircuitTester()
      // Ships sharing exactly one cell
      const singleCellOverlap: BoardCircuitInput = {
        ...validBoard,
        cruiser: [0, 2, 0], // (0,2) to (2,2)
        submarine: [2, 2, 1], // (2,2) to (2,4) - overlaps at (2,2)
      }

      await expect(board.witness(singleCellOverlap)).rejects.toThrow()
    })

    test("should ensure all 17 ship cells are unique", async () => {
      const board = await loadBoardCircuitTester()
      // Total cells: 5 + 4 + 3 + 3 + 2 = 17
      // Each cell position must be unique across all ships
      const edgeCaseOverlap: BoardCircuitInput = {
        carrier: [0, 0, 0], // (0,0) to (4,0) - 5 cells
        battleship: [0, 1, 0], // (0,1) to (3,1) - 4 cells
        cruiser: [0, 2, 0], // (0,2) to (2,2) - 3 cells
        submarine: [0, 3, 0], // (0,3) to (2,3) - 3 cells
        destroyer: [1, 1, 0], // (1,1) to (2,1) - overlaps with battleship at (1,1), (2,1)
        salt: 12345,
      }

      await expect(board.witness(edgeCaseOverlap)).rejects.toThrow()
    })

    test("should accept ships with no overlaps", async () => {
      const board = await loadBoardCircuitTester()
      const nonOverlappingBoard: BoardCircuitInput = {
        carrier: [0, 0, 0], // (0,0) to (4,0)
        battleship: [0, 1, 0], // (0,1) to (3,1)
        cruiser: [5, 0, 0], // (5,0) to (7,0)
        submarine: [5, 1, 0], // (5,1) to (7,1)
        destroyer: [0, 2, 0], // (0,2) to (1,2)
        salt: 12345,
      }

      await expect(board.witness(nonOverlappingBoard)).resolves.toBeDefined()
    })
  })

  describe("BR-5: Complete Overlap Detection", () => {
    test("should detect overlaps between horizontal ships", async () => {
      const board = await loadBoardCircuitTester()
      const horizontalOverlap: BoardCircuitInput = {
        carrier: [1, 1, 0], // (1,1) to (5,1)
        battleship: [3, 1, 0], // (3,1) to (6,1) - overlaps at (3,1), (4,1), (5,1)
        cruiser: [0, 2, 0],
        submarine: [0, 3, 0],
        destroyer: [0, 4, 0],
        salt: 12345,
      }

      await expect(board.witness(horizontalOverlap)).rejects.toThrow()
    })

    test("should detect overlaps between vertical ships", async () => {
      const board = await loadBoardCircuitTester()
      const verticalOverlap: BoardCircuitInput = {
        carrier: [1, 1, 1], // (1,1) to (1,5)
        battleship: [1, 3, 1], // (1,3) to (1,6) - overlaps at (1,3), (1,4), (1,5)
        cruiser: [2, 0, 1],
        submarine: [3, 0, 1],
        destroyer: [4, 0, 1],
        salt: 12345,
      }

      await expect(board.witness(verticalOverlap)).rejects.toThrow()
    })

    test("should detect overlaps between horizontal and vertical ships", async () => {
      const board = await loadBoardCircuitTester()
      const mixedOrientationOverlap: BoardCircuitInput = {
        carrier: [2, 2, 0], // (2,2) to (6,2) horizontal
        battleship: [4, 0, 1], // (4,0) to (4,3) vertical - overlaps at (4,2)
        cruiser: [0, 0, 0],
        submarine: [0, 1, 0],
        destroyer: [0, 3, 0],
        salt: 12345,
      }

      await expect(board.witness(mixedOrientationOverlap)).rejects.toThrow()
    })

    test("should detect complex multi-ship overlaps", async () => {
      const board = await loadBoardCircuitTester()
      // Multiple ships sharing the same area
      const multiShipOverlap: BoardCircuitInput = {
        carrier: [2, 2, 0], // (2,2) to (6,2)
        battleship: [3, 1, 1], // (3,1) to (3,4) - overlaps carrier at (3,2)
        cruiser: [4, 2, 1], // (4,2) to (4,4) - overlaps carrier at (4,2)
        submarine: [5, 1, 1], // (5,1) to (5,3) - overlaps carrier at (5,2)
        destroyer: [0, 0, 0],
        salt: 12345,
      }

      await expect(board.witness(multiShipOverlap)).rejects.toThrow()
    })

    test("should handle edge case overlaps at board boundaries", async () => {
      const board = await loadBoardCircuitTester()
      const boundaryOverlap: BoardCircuitInput = {
        carrier: [5, 9, 0], // (5,9) to (9,9) - at bottom edge
        battleship: [9, 6, 1], // (9,6) to (9,9) - at right edge, overlaps carrier at (9,9)
        cruiser: [0, 0, 0],
        submarine: [0, 1, 0],
        destroyer: [0, 2, 0],
        salt: 12345,
      }

      await expect(board.witness(boundaryOverlap)).rejects.toThrow()
    })

    test("should detect overlaps involving all ship types", async () => {
      const board = await loadBoardCircuitTester()
      // Test that overlap detection works for every pair of ship types
      const carrierBattleshipOverlap: BoardCircuitInput = {
        carrier: [0, 0, 0], // (0,0) to (4,0)
        battleship: [2, 0, 1], // (2,0) to (2,3) - overlaps at (2,0)
        cruiser: [0, 1, 0],
        submarine: [0, 2, 0],
        destroyer: [0, 3, 0],
        salt: 12345,
      }

      await expect(board.witness(carrierBattleshipOverlap)).rejects.toThrow()
    })

    test("should accept ships that are adjacent but not overlapping", async () => {
      const board = await loadBoardCircuitTester()
      const adjacentButValid: BoardCircuitInput = {
        carrier: [0, 0, 0], // (0,0) to (4,0)
        battleship: [0, 1, 0], // (0,1) to (3,1) - adjacent to carrier
        cruiser: [5, 0, 1], // (5,0) to (5,2) - adjacent to carrier
        submarine: [1, 2, 0], // (1,2) to (3,2) - adjacent to battleship
        destroyer: [5, 3, 0], // (5,3) to (6,3) - adjacent to cruiser
        salt: 12345,
      }

      await expect(board.witness(adjacentButValid)).resolves.toBeDefined()
    })

    test("should handle minimum separation requirements", async () => {
      const board = await loadBoardCircuitTester()
      // Ships can be adjacent (touching edges) but not overlapping
      const touchingEdges: BoardCircuitInput = {
        carrier: [0, 0, 0], // (0,0) to (4,0)
        battleship: [6, 1, 1], // (6,1) to (6,4) - separate from carrier
        cruiser: [0, 1, 0], // (0,1) to (2,1) - edge touches carrier
        submarine: [5, 0, 1], // (5,0) to (5,2) - edge touches carrier
        destroyer: [7, 6, 0], // (7,6) to (8,6) - separate from all others
        salt: 12345,
      }

      await expect(board.witness(touchingEdges)).resolves.toBeDefined()
    })
  })
})
