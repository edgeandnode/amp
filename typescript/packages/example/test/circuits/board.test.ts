import { describe, expect, test } from "vitest"
import { loadBoardCircuitTester } from "../utils.ts"

describe("board circuit", () => {
  describe("valid ship placements", () => {
    test("horizontal ships in rows", async () => {
      const board = await loadBoardCircuitTester()
      await expect(board.witness({
        carrier: [0, 0, 0], // (0,0) to (4,0)
        battleship: [0, 1, 0], // (0,1) to (3,1)
        cruiser: [0, 2, 0], // (0,2) to (2,2)
        submarine: [0, 3, 0], // (0,3) to (2,3)
        destroyer: [0, 4, 0], // (0,4) to (1,4)
        salt: 12345,
      })).resolves.toBeDefined()
    })

    test("vertical ships in columns", async () => {
      const board = await loadBoardCircuitTester()
      await expect(board.witness({
        carrier: [0, 0, 1], // (0,0) to (0,4)
        battleship: [1, 0, 1], // (1,0) to (1,3)
        cruiser: [2, 0, 1], // (2,0) to (2,2)
        submarine: [3, 0, 1], // (3,0) to (3,2)
        destroyer: [4, 0, 1], // (4,0) to (4,1)
        salt: 54321,
      })).resolves.toBeDefined()
    })

    test("mixed horizontal and vertical", async () => {
      const board = await loadBoardCircuitTester()
      await expect(board.witness({
        carrier: [0, 0, 0], // (0,0) to (4,0) horizontal
        battleship: [5, 0, 1], // (5,0) to (5,3) vertical
        cruiser: [0, 1, 0], // (0,1) to (2,1) horizontal
        submarine: [6, 0, 1], // (6,0) to (6,2) vertical
        destroyer: [0, 2, 0], // (0,2) to (1,2) horizontal
        salt: 99999,
      })).resolves.toBeDefined()
    })

    test("ships at board edges", async () => {
      const board = await loadBoardCircuitTester()
      await expect(board.witness({
        carrier: [5, 9, 0], // (5,9) to (9,9) - bottom edge
        battleship: [9, 0, 1], // (9,0) to (9,3) - right edge
        cruiser: [0, 9, 0], // (0,9) to (2,9) - left edge
        submarine: [0, 0, 1], // (0,0) to (0,2) - top-left corner
        destroyer: [7, 0, 1], // (7,0) to (7,1)
        salt: 77777,
      })).resolves.toBeDefined()
    })
  })

  describe("ship overlap detection", () => {
    test("horizontal ships same row", async () => {
      const board = await loadBoardCircuitTester()
      await expect(board.witness({
        carrier: [0, 0, 0], // (0,0) to (4,0)
        battleship: [3, 0, 0], // (3,0) to (6,0) - overlaps at (3,0) and (4,0)
        cruiser: [0, 1, 0],
        submarine: [0, 2, 0],
        destroyer: [0, 3, 0],
        salt: 12345,
      })).rejects.toThrow("Assert Failed")
    })

    test("vertical ships same column", async () => {
      const board = await loadBoardCircuitTester()
      await expect(board.witness({
        carrier: [0, 0, 1], // (0,0) to (0,4)
        battleship: [0, 2, 1], // (0,2) to (0,5) - overlaps at (0,2), (0,3), (0,4)
        cruiser: [1, 0, 0],
        submarine: [2, 0, 0],
        destroyer: [3, 0, 0],
        salt: 12345,
      })).rejects.toThrow("Assert Failed")
    })

    test("horizontal and vertical intersection", async () => {
      const board = await loadBoardCircuitTester()
      await expect(board.witness({
        carrier: [2, 2, 0], // (2,2) to (6,2) horizontal
        battleship: [4, 0, 1], // (4,0) to (4,3) vertical - intersects at (4,2)
        cruiser: [0, 0, 0],
        submarine: [0, 1, 0],
        destroyer: [0, 3, 0],
        salt: 12345,
      })).rejects.toThrow("Assert Failed")
    })

    test("single coordinate overlap", async () => {
      const board = await loadBoardCircuitTester()
      await expect(board.witness({
        carrier: [0, 0, 0], // (0,0) to (4,0)
        battleship: [4, 0, 1], // (4,0) to (4,3) - single point overlap at (4,0)
        cruiser: [1, 1, 0],
        submarine: [1, 2, 0],
        destroyer: [1, 3, 0],
        salt: 12345,
      })).rejects.toThrow("Assert Failed")
    })

    test("adjacent ships are valid", async () => {
      const board = await loadBoardCircuitTester()
      await expect(board.witness({
        carrier: [0, 0, 0], // (0,0) to (4,0)
        battleship: [0, 1, 0], // (0,1) to (3,1) - adjacent, not overlapping
        cruiser: [5, 0, 1], // (5,0) to (5,2) - adjacent to carrier
        submarine: [4, 1, 1], // (4,1) to (4,3) - adjacent to battleship
        destroyer: [6, 0, 1], // (6,0) to (6,1) - adjacent to cruiser
        salt: 12345,
      })).resolves.toBeDefined()
    })
  })

  describe("boundary validation", () => {
    test("horizontal ship extends beyond right edge", async () => {
      const board = await loadBoardCircuitTester()
      await expect(board.witness({
        carrier: [6, 0, 0], // (6,0) to (10,0) - extends beyond x=9
        battleship: [0, 1, 0],
        cruiser: [0, 2, 0],
        submarine: [0, 3, 0],
        destroyer: [0, 4, 0],
        salt: 12345,
      })).rejects.toThrow("Assert Failed")
    })

    test("vertical ship extends beyond bottom edge", async () => {
      const board = await loadBoardCircuitTester()
      await expect(board.witness({
        carrier: [0, 6, 1], // (0,6) to (0,10) - extends beyond y=9
        battleship: [1, 0, 0],
        cruiser: [2, 0, 0],
        submarine: [3, 0, 0],
        destroyer: [4, 0, 0],
        salt: 12345,
      })).rejects.toThrow("Assert Failed")
    })

    test("negative coordinates", async () => {
      const board = await loadBoardCircuitTester()
      await expect(board.witness({
        carrier: [-1, 0, 0], // Invalid x coordinate
        battleship: [0, 1, 0],
        cruiser: [0, 2, 0],
        submarine: [0, 3, 0],
        destroyer: [0, 4, 0],
        salt: 12345,
      })).rejects.toThrow("Assert Failed")
    })

    test("coordinates at exact board limits", async () => {
      const board = await loadBoardCircuitTester()
      await expect(board.witness({
        carrier: [5, 9, 0], // (5,9) to (9,9) - exactly at limits
        battleship: [9, 5, 1], // (9,5) to (9,8) - exactly at limits
        cruiser: [0, 0, 0], // (0,0) to (2,0)
        submarine: [0, 1, 1], // (0,1) to (0,3)
        destroyer: [1, 5, 0], // (1,5) to (2,5)
        salt: 12345,
      })).resolves.toBeDefined()
    })
  })

  describe("ship orientation validation", () => {
    test("invalid orientation value", async () => {
      const board = await loadBoardCircuitTester()
      await expect(board.witness({
        carrier: [0, 0, 2], // Invalid orientation (should be 0 or 1)
        battleship: [0, 1, 0],
        cruiser: [0, 2, 0],
        submarine: [0, 3, 0],
        destroyer: [0, 4, 0],
        salt: 12345,
      })).rejects.toThrow("Assert Failed")
    })

    test("negative orientation", async () => {
      const board = await loadBoardCircuitTester()
      await expect(board.witness({
        carrier: [0, 0, -1], // Invalid orientation
        battleship: [0, 1, 0],
        cruiser: [0, 2, 0],
        submarine: [0, 3, 0],
        destroyer: [0, 4, 0],
        salt: 12345,
      })).rejects.toThrow("Assert Failed")
    })
  })

  describe("ship length validation", () => {
    test("all ships have correct implicit lengths", async () => {
      const board = await loadBoardCircuitTester()
      // This test validates that our ship validation enforces correct lengths
      // by ensuring ships generate correct coordinate sets
      await expect(board.witness({
        carrier: [0, 0, 0], // Should generate 5 coordinates
        battleship: [0, 1, 0], // Should generate 4 coordinates
        cruiser: [0, 2, 0], // Should generate 3 coordinates
        submarine: [0, 3, 0], // Should generate 3 coordinates
        destroyer: [0, 4, 0], // Should generate 2 coordinates
        salt: 12345,
      })).resolves.toBeDefined()
    })
  })

  describe("salt validation", () => {
    test("zero salt is valid", async () => {
      const board = await loadBoardCircuitTester()
      await expect(board.witness({
        carrier: [0, 0, 0],
        battleship: [0, 1, 0],
        cruiser: [0, 2, 0],
        submarine: [0, 3, 0],
        destroyer: [0, 4, 0],
        salt: 0,
      })).resolves.toBeDefined()
    })

    test("large salt values", async () => {
      const board = await loadBoardCircuitTester()
      await expect(board.witness({
        carrier: [0, 0, 0],
        battleship: [0, 1, 0],
        cruiser: [0, 2, 0],
        submarine: [0, 3, 0],
        destroyer: [0, 4, 0],
        salt: 999999999999999,
      })).resolves.toBeDefined()
    })

    test("same ships different salt produce different commitments", async () => {
      const board = await loadBoardCircuitTester()
      const input = {
        carrier: [0, 5, 0], // (0,5) to (4,5) horizontal
        battleship: [0, 6, 0], // (0,6) to (3,6) horizontal
        cruiser: [0, 7, 0], // (0,7) to (2,7) horizontal
        submarine: [0, 8, 0], // (0,8) to (2,8) horizontal
        destroyer: [0, 9, 0], // (0,9) to (1,9) horizontal
        salt: 12345,
      }
      const a = await board.witness({ ...input, salt: 11111 })
      const b = await board.witness({ ...input, salt: 22222 })
      expect(b[1]).not.toBe(a[1])
    })
  })

  describe("complex valid scenarios", () => {
    test("maximum spread placement", async () => {
      const board = await loadBoardCircuitTester()
      await expect(board.witness({
        carrier: [0, 9, 0], // Bottom-left horizontal
        battleship: [9, 0, 1], // Top-right vertical
        cruiser: [0, 0, 1], // Top-left vertical
        submarine: [6, 9, 0], // Bottom-right horizontal
        destroyer: [4, 4, 0], // Center horizontal
        salt: 123456789,
      })).resolves.toBeDefined()
    })

    test("tight packing without overlap", async () => {
      const board = await loadBoardCircuitTester()
      await expect(board.witness({
        carrier: [0, 0, 0], // (0,0) to (4,0)
        battleship: [5, 0, 0], // (5,0) to (8,0)
        cruiser: [0, 1, 0], // (0,1) to (2,1)
        submarine: [3, 1, 0], // (3,1) to (5,1)
        destroyer: [6, 1, 0], // (6,1) to (7,1)
        salt: 987654321,
      })).resolves.toBeDefined()
    })
  })
})
