import { describe, expect, test } from "vitest"
import { cheatHit, cheatMiss, cheatOverride, cheatSunk, createBoardSimulation } from "../simulation.ts"
import { type BoardCircuitInput } from "../utils.ts"

/**
 * Anti-Cheat Tests: Shot Circuit - Result Claiming Integrity
 *
 * Tests for security requirements SR-16 through SR-18:
 * - SR-16: Result Priority Enforcement
 * - SR-17: Ship ID Accuracy
 * - SR-18: No-Sunk Ship ID Constraint
 */
describe("shot result claiming integrity", () => {
  const standardBoard: BoardCircuitInput = {
    carrier: [0, 0, 0], // Length 5
    battleship: [0, 1, 0], // Length 4
    cruiser: [0, 2, 0], // Length 3
    submarine: [0, 3, 0], // Length 3
    destroyer: [0, 4, 0], // Length 2
    salt: 77777,
  }

  describe("SR-16: Result Priority Enforcement", () => {
    test("should enforce Sunk > Hit > Miss priority", async () => {
      // Test case: destroyer with 1 hit, shooting its second cell should be SUNK not HIT
      const sim = await createBoardSimulation(standardBoard, [0, 0, 0, 0, 1])

      // Shoot destroyer's second cell - should be sunk, not just hit
      await expect(
        sim.fireShot(1, 4, cheatHit()), // Claim hit when should be sunk
      ).rejects.toThrow()
    })

    test("should reject lower priority claims when higher applies", async () => {
      const priorityTests = [
        {
          description: "miss claim when hit applies",
          initialHits: [0, 0, 0, 0, 0],
          shotTarget: { x: 0, y: 0 }, // Hit carrier
          wrongClaim: cheatMiss(),
        },
        {
          description: "hit claim when sunk applies",
          initialHits: [0, 0, 0, 0, 1], // Destroyer nearly sunk
          shotTarget: { x: 1, y: 4 }, // Sink destroyer
          wrongClaim: cheatHit(),
        },
        {
          description: "miss claim when sunk applies",
          initialHits: [4, 0, 0, 0, 0], // Carrier nearly sunk
          shotTarget: { x: 4, y: 0 }, // Sink carrier
          wrongClaim: cheatMiss(),
        },
      ]

      for (const test of priorityTests) {
        const sim = await createBoardSimulation(standardBoard, test.initialHits)

        await expect(
          sim.fireShot(test.shotTarget.x, test.shotTarget.y, test.wrongClaim),
        ).rejects.toThrow()
      }
    })

    test("should enforce highest applicable result only", async () => {
      // Multiple ships nearly sunk, shoot one specific ship
      const sim = await createBoardSimulation(standardBoard, [4, 3, 2, 2, 1])

      // Sink carrier - result should be SUNK, not HIT
      await expect(
        sim.fireShot(4, 0, cheatHit()), // Claim hit when sinking carrier
      ).rejects.toThrow()

      // Sink destroyer - result should be SUNK
      await expect(
        sim.fireShot(1, 4, cheatHit()), // Claim hit when sinking destroyer
      ).rejects.toThrow()
    })

    test("should allow correct priority claims", async () => {
      const sim = await createBoardSimulation(standardBoard, [0, 0, 0, 0, 1])

      // Correct sunk claim
      await sim.fireShot(1, 4) // Sink destroyer correctly
      expect(sim.isDestroyerSunk()).toBe(true)
      expect(sim.getRemainingShips()).toBe(4)
    })

    test("should handle priority edge cases (circuit behavior)", async () => {
      // Ship with max hits - circuit allows further hits beyond max
      const sim = await createBoardSimulation(standardBoard, [5, 4, 3, 3, 2])

      // All ships sunk - circuit allows additional hits
      const positions = [
        { x: 0, y: 0 }, // Any carrier position
        { x: 0, y: 1 }, // Any battleship position
        { x: 0, y: 2 }, // Any cruiser position
        { x: 0, y: 3 }, // Any submarine position
        { x: 0, y: 4 }, // Any destroyer position
      ]

      for (const pos of positions) {
        await sim.fireShot(pos.x, pos.y, cheatHit()) // Should succeed based on circuit behavior
      }

      // Verify hit counts incremented beyond ship lengths
      const hitCounts = sim.getHitCounts()
      expect(hitCounts[0]).toBe(6) // Carrier: 5 + 1
      expect(hitCounts[1]).toBe(5) // Battleship: 4 + 1
    })
  })

  describe("SR-17: Ship ID Accuracy", () => {
    test("should validate ship ID matches actually sunk ship", async () => {
      // Destroyer nearly sunk
      const sim = await createBoardSimulation(standardBoard, [0, 0, 0, 0, 1])

      // Sink destroyer but claim wrong ship ID
      const wrongShipIds = [0, 1, 2, 3] // All except destroyer (4)

      for (const wrongId of wrongShipIds) {
        await expect(
          sim.fireShot(1, 4, cheatSunk(wrongId)),
        ).rejects.toThrow()
      }
    })

    test("should enforce correct ship ID for each ship type", async () => {
      const shipTests = [
        { shipId: 0, name: "carrier", position: { x: 4, y: 0 }, initialHits: [4, 0, 0, 0, 0] },
        { shipId: 1, name: "battleship", position: { x: 3, y: 1 }, initialHits: [0, 3, 0, 0, 0] },
        { shipId: 2, name: "cruiser", position: { x: 2, y: 2 }, initialHits: [0, 0, 2, 0, 0] },
        { shipId: 3, name: "submarine", position: { x: 2, y: 3 }, initialHits: [0, 0, 0, 2, 0] },
        { shipId: 4, name: "destroyer", position: { x: 1, y: 4 }, initialHits: [0, 0, 0, 0, 1] },
      ]

      for (const test of shipTests) {
        const sim = await createBoardSimulation(standardBoard, test.initialHits)

        // Sink the ship with correct ID - should work
        await sim.fireShot(test.position.x, test.position.y)
        expect(sim.isShipSunk(test.shipId)).toBe(true)
      }
    })

    test("should reject invalid ship IDs", async () => {
      const sim = await createBoardSimulation(standardBoard, [0, 0, 0, 0, 1])

      const invalidShipIds = [5, 6, 10, 99, 255] // Valid range is 0-4 or 255 for no-sunk

      for (const invalidId of invalidShipIds) {
        // Skip 255 as it's valid for no-sunk case
        if (invalidId === 255) continue

        await expect(
          sim.fireShot(1, 4, cheatSunk(invalidId)),
        ).rejects.toThrow()
      }
    })

    test("should prevent ship ID spoofing in complex scenarios", async () => {
      // Multiple ships nearly sunk
      const sim = await createBoardSimulation(standardBoard, [4, 3, 2, 2, 1])

      // Sink carrier but claim battleship was sunk
      await expect(
        sim.fireShot(4, 0, cheatSunk(1)), // Hit carrier, claim battleship
      ).rejects.toThrow()

      // Sink destroyer but claim submarine was sunk
      await expect(
        sim.fireShot(1, 4, cheatSunk(3)), // Hit destroyer, claim submarine
      ).rejects.toThrow()
    })

    test("should validate ship ID corresponds to actually hit ship", async () => {
      const sim = await createBoardSimulation(standardBoard, [0, 0, 2, 2, 1])

      // Shoot at empty water but claim any ship sunk
      const shipIds = [0, 1, 2, 3, 4]

      for (const shipId of shipIds) {
        await expect(
          sim.fireShot(9, 9, cheatSunk(shipId)), // Miss but claim ship sunk
        ).rejects.toThrow()
      }
    })
  })

  describe("SR-18: No-Sunk Ship ID Constraint", () => {
    test("should enforce null ship ID (255) for miss results", async () => {
      const sim = await createBoardSimulation(standardBoard)

      // But manually setting wrong ship ID for miss should fail
      const wrongShipIdForMiss = cheatOverride({
        claimedResult: 0, // Miss
        claimedShipId: 0, // Wrong: should be 255 for miss
      })

      await expect(
        sim.fireShot(9, 9, wrongShipIdForMiss),
      ).rejects.toThrow()
    })

    test("should enforce null ship ID (255) for hit results", async () => {
      const sim = await createBoardSimulation(standardBoard)

      // Hit should have ship ID 255 (not sunk yet)
      const wrongShipIdForHit = cheatOverride({
        claimedResult: 1, // Hit
        claimedShipId: 0, // Wrong: should be 255 for non-sunk hit
      })

      await expect(
        sim.fireShot(0, 0, wrongShipIdForHit), // Hit carrier
      ).rejects.toThrow()
    })

    test("should allow valid ship IDs only for sunk results", async () => {
      const sim = await createBoardSimulation(standardBoard, [0, 0, 0, 0, 1])

      // Valid: sunk result with correct ship ID
      await sim.fireShot(1, 4) // Sink destroyer

      const lastShot = sim.getLastShot()
      expect(lastShot?.actual.result).toBe(2) // Sunk
      expect(lastShot?.actual.shipId).toBe(4) // Destroyer
    })

    test("should reject specific ship IDs for non-sunk results", async () => {
      const sim = await createBoardSimulation(standardBoard)

      const invalidCombinations = [
        { result: 0, shipId: 0 }, // Miss with carrier ID
        { result: 0, shipId: 1 }, // Miss with battleship ID
        { result: 0, shipId: 4 }, // Miss with destroyer ID
        { result: 1, shipId: 0 }, // Hit with carrier ID
        { result: 1, shipId: 2 }, // Hit with cruiser ID
      ]

      for (const combo of invalidCombinations) {
        const invalidOverride = cheatOverride({
          claimedResult: combo.result,
          claimedShipId: combo.shipId,
        })

        const target = combo.result === 1 ? { x: 0, y: 0 } : { x: 9, y: 9 }

        await expect(
          sim.fireShot(target.x, target.y, invalidOverride),
        ).rejects.toThrow()
      }
    })

    test("should validate ship ID 255 is only for non-sunk results", async () => {
      const sim = await createBoardSimulation(standardBoard, [0, 0, 0, 0, 1])

      // Sunk result with ship ID 255 should fail
      const sunkWithNullId = cheatOverride({
        claimedResult: 2, // Sunk
        claimedShipId: 255, // Wrong: should be specific ship ID
      })

      await expect(
        sim.fireShot(1, 4, sunkWithNullId),
      ).rejects.toThrow()
    })

    test("should handle edge case ship ID validations", async () => {
      const sim = await createBoardSimulation(standardBoard)

      // Test boundary values
      const boundaryTests = [
        { result: 0, shipId: 254 }, // Just below 255
        { result: 0, shipId: 256 }, // Just above 255 (if supported)
        { result: 1, shipId: 5 }, // Just above valid ship range
      ]

      for (const test of boundaryTests) {
        const boundaryOverride = cheatOverride({
          claimedResult: test.result,
          claimedShipId: test.shipId,
        })

        const target = test.result === 1 ? { x: 0, y: 0 } : { x: 9, y: 9 }

        await expect(
          sim.fireShot(target.x, target.y, boundaryOverride),
        ).rejects.toThrow()
      }
    })
  })
})
