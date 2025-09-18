import { describe, expect, test } from "vitest"
import { cheatHit, cheatMiss, cheatOverride, cheatSunk, createBoardSimulation } from "../simulation.ts"
import { type BoardCircuitInput } from "../utils.ts"

/**
 * Anti-Cheat Tests: Shot Response Integrity
 *
 * Tests for security requirements SR-1 through SR-3:
 * - SR-1: Hit Detection Accuracy
 * - SR-2: Miss Detection Accuracy
 * - SR-3: Sunk Ship Detection
 */
describe("shot response integrity", () => {
  const standardBoard: BoardCircuitInput = {
    carrier: [2, 2, 0], // Horizontal at (2,2)
    battleship: [2, 3, 0], // Horizontal at (2,3)
    cruiser: [2, 4, 0], // Horizontal at (2,4)
    submarine: [2, 5, 0], // Horizontal at (2,5)
    destroyer: [2, 6, 0], // Horizontal at (2,6)
    salt: 11111,
  }

  describe("SR-1: Hit Detection Accuracy", () => {
    test("should reject miss claims when shot actually hits ship", async () => {
      const sim = await createBoardSimulation(standardBoard)

      // Test hitting each ship type and claiming miss (should fail)
      const shipHitTests = [
        { x: 2, y: 2, name: "carrier" },
        { x: 2, y: 3, name: "battleship" },
        { x: 2, y: 4, name: "cruiser" },
        { x: 2, y: 5, name: "submarine" },
        { x: 2, y: 6, name: "destroyer" },
      ]

      for (const hitTest of shipHitTests) {
        await expect(
          sim.fireShot(hitTest.x, hitTest.y, cheatMiss()),
        ).rejects.toThrow()
      }
    })

    test("should reject miss claims at all ship cell positions", async () => {
      const sim = await createBoardSimulation(standardBoard)

      // Test all carrier positions (length 5)
      for (let i = 0; i < 5; i++) {
        await expect(
          sim.fireShot(2 + i, 2, cheatMiss()),
        ).rejects.toThrow()
      }

      // Test all battleship positions (length 4)
      for (let i = 0; i < 4; i++) {
        await expect(
          sim.fireShot(2 + i, 3, cheatMiss()),
        ).rejects.toThrow()
      }
    })

    test("should accurately detect hits on partially damaged ships", async () => {
      // Start with some ships partially hit
      const sim = await createBoardSimulation(standardBoard, [2, 1, 0, 0, 0])

      // Hitting already damaged ships should still be detected as hits
      await expect(
        sim.fireShot(2, 2, cheatMiss()), // Hit carrier (already 2 hits)
      ).rejects.toThrow()

      await expect(
        sim.fireShot(2, 3, cheatMiss()), // Hit battleship (already 1 hit)
      ).rejects.toThrow()
    })
  })

  describe("SR-2: Miss Detection Accuracy", () => {
    test("should reject hit claims when shot actually misses", async () => {
      const sim = await createBoardSimulation(standardBoard)

      // Test various empty water positions
      const missPositions = [
        { x: 0, y: 0 },
        { x: 1, y: 1 },
        { x: 9, y: 9 },
        { x: 0, y: 9 },
        { x: 9, y: 0 },
        { x: 5, y: 5 },
      ]

      for (const pos of missPositions) {
        await expect(
          sim.fireShot(pos.x, pos.y, cheatHit()),
        ).rejects.toThrow()
      }
    })

    test("should reject false sunk claims on misses", async () => {
      const sim = await createBoardSimulation(standardBoard)

      // Claim sinking destroyer when shooting at empty water
      await expect(
        sim.fireShot(8, 8, cheatSunk(4)),
      ).rejects.toThrow()
    })

    test("should accurately detect misses near ship edges", async () => {
      const sim = await createBoardSimulation(standardBoard)

      // Test positions adjacent to ships (should be misses)
      const nearMissPositions = [
        { x: 1, y: 2 }, // Left of carrier
        { x: 7, y: 2 }, // Right of carrier
        { x: 2, y: 1 }, // Above carrier
        { x: 2, y: 7 }, // Below destroyer
      ]

      for (const pos of nearMissPositions) {
        await expect(
          sim.fireShot(pos.x, pos.y, cheatHit()),
        ).rejects.toThrow()
      }
    })
  })

  describe("SR-3: Sunk Ship Detection", () => {
    test("should reject premature sunk claims", async () => {
      // Destroyer has 1 hit, need 1 more to sink
      const sim = await createBoardSimulation(standardBoard, [0, 0, 0, 0, 1])

      // Hit destroyer but claim it was already sunk before this shot
      const prematureSunkOverride = cheatOverride({
        claimedResult: 0, // Claim miss
        // But destroyer should be newly sunk with this hit
      })

      await expect(
        sim.fireShot(3, 6, prematureSunkOverride), // Second destroyer hit
      ).rejects.toThrow()
    })

    test("should reject false sunk claims on partial hits", async () => {
      // Carrier has 3 hits, still need 2 more
      const sim = await createBoardSimulation(standardBoard, [3, 0, 0, 0, 0])

      // Hit carrier again but claim it's sunk (should be just hit)
      await expect(
        sim.fireShot(5, 2, cheatSunk(0)), // 4th hit on carrier, not sunk yet
      ).rejects.toThrow()
    })

    test("should enforce exact sunk detection for all ship types", async () => {
      const shipTests = [
        { shipId: 0, length: 5, initialHits: 4 }, // Carrier
        { shipId: 1, length: 4, initialHits: 3 }, // Battleship
        { shipId: 2, length: 3, initialHits: 2 }, // Cruiser
        { shipId: 3, length: 3, initialHits: 2 }, // Submarine
        { shipId: 4, length: 2, initialHits: 1 }, // Destroyer
      ]

      for (const shipTest of shipTests) {
        const hitCounts = [0, 0, 0, 0, 0]
        hitCounts[shipTest.shipId] = shipTest.initialHits

        const sim = await createBoardSimulation(standardBoard, hitCounts)

        // Hit the ship one more time but claim just hit (should be sunk)
        await expect(
          sim.fireShot(2, 2 + shipTest.shipId, cheatHit()),
        ).rejects.toThrow()
      }
    })

    test("should reject wrong ship ID for sunk claims", async () => {
      // Destroyer nearly sunk
      const sim = await createBoardSimulation(standardBoard, [0, 0, 0, 0, 1])

      // Sink destroyer but claim different ship was sunk
      await expect(
        sim.fireShot(3, 6, cheatSunk(0)), // Claim carrier sunk instead of destroyer
      ).rejects.toThrow()
    })

    test("should validate ship ID matches newly sunk ship", async () => {
      // Multiple ships nearly sunk
      const sim = await createBoardSimulation(standardBoard, [4, 3, 2, 2, 1])

      // Sink carrier (5th hit) but claim battleship was sunk
      await expect(
        sim.fireShot(6, 2, cheatSunk(1)), // Hit carrier, claim battleship sunk
      ).rejects.toThrow()
    })

    test("should reject sunk claims with invalid ship IDs", async () => {
      // Destroyer nearly sunk
      const sim = await createBoardSimulation(standardBoard, [0, 0, 0, 0, 1])

      // Use invalid ship ID
      await expect(
        sim.fireShot(3, 6, cheatSunk(99)), // Invalid ship ID
      ).rejects.toThrow()
    })
  })
})
