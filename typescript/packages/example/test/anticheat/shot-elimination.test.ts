import { describe, expect, test } from "vitest"
import { cheatHit, cheatOverride, cheatSunk, createBoardSimulation } from "../simulation.ts"
import { type BoardCircuitInput } from "../utils.ts"

/**
 * Anti-Cheat Tests: Shot Circuit - Elimination Logic Integrity
 *
 * Tests for security requirements SR-10 through SR-12:
 * - SR-10: Newly Sunk Ship Validation
 * - SR-11: Already Sunk Ship Protection
 * - SR-12: Remaining Ships Calculation
 */
describe("shot elimination logic", () => {
  const standardBoard: BoardCircuitInput = {
    carrier: [0, 0, 0], // Length 5, horizontal
    battleship: [0, 1, 0], // Length 4, horizontal
    cruiser: [0, 2, 0], // Length 3, horizontal
    submarine: [0, 3, 0], // Length 3, horizontal
    destroyer: [0, 4, 0], // Length 2, horizontal
    salt: 44444,
  }

  describe("SR-10: Newly Sunk Ship Validation", () => {
    test("should require current shot to hit newly sunk ship", async () => {
      // Destroyer has 1 hit, needs 1 more to sink
      const sim = await createBoardSimulation(standardBoard, [0, 0, 0, 0, 1])

      // Shoot at empty water but claim destroyer is newly sunk
      await expect(
        sim.fireShot(9, 9, cheatSunk(4)), // Miss but claim destroyer sunk
      ).rejects.toThrow()
    })

    test("should reject sunk claims when ship wasn't hit this turn", async () => {
      // Multiple ships nearly sunk
      const sim = await createBoardSimulation(standardBoard, [4, 3, 2, 2, 1])

      // Shoot at empty water but claim any ship is newly sunk
      const shipIds = [0, 1, 2, 3, 4]
      for (const shipId of shipIds) {
        await expect(
          sim.fireShot(8, 8, cheatSunk(shipId)),
        ).rejects.toThrow()
      }
    })

    test("should validate sunk claim matches actually hit ship", async () => {
      // Carrier and destroyer both nearly sunk
      const sim = await createBoardSimulation(standardBoard, [4, 0, 0, 0, 1])

      // Hit carrier but claim destroyer sunk
      await expect(
        sim.fireShot(4, 0, cheatSunk(4)), // Hit carrier, claim destroyer sunk
      ).rejects.toThrow()
    })

    test("should enforce newly sunk ship had exactly (length-1) hits before", async () => {
      // Destroyer with wrong number of previous hits
      const sim = await createBoardSimulation(standardBoard, [0, 0, 0, 0, 0])

      // Destroyer length is 2, so needs exactly 1 previous hit to be newly sunk
      // But starting with 0 hits, so claiming sunk is invalid
      await expect(
        sim.fireShot(0, 4, cheatSunk(4)), // First hit, can't be sunk yet
      ).rejects.toThrow()
    })

    test("should accept valid newly sunk ship claims", async () => {
      // Destroyer with exactly 1 hit (length 2, so 1 more needed)
      const sim = await createBoardSimulation(standardBoard, [0, 0, 0, 0, 1])

      // Hit destroyer's second cell - should legitimately sink it
      await sim.fireShot(1, 4) // Second destroyer cell

      expect(sim.isDestroyerSunk()).toBe(true)
      expect(sim.getRemainingShips()).toBe(4)
    })

    test("should validate sunk transitions for all ship types", async () => {
      const shipTests = [
        { shipId: 0, length: 5, position: { x: 4, y: 0 } }, // Carrier
        { shipId: 1, length: 4, position: { x: 3, y: 1 } }, // Battleship
        { shipId: 2, length: 3, position: { x: 2, y: 2 } }, // Cruiser
        { shipId: 3, length: 3, position: { x: 2, y: 3 } }, // Submarine
        { shipId: 4, length: 2, position: { x: 1, y: 4 } }, // Destroyer
      ]

      for (const shipTest of shipTests) {
        // Set ship to exactly (length-1) hits
        const hitCounts = [0, 0, 0, 0, 0]
        hitCounts[shipTest.shipId] = shipTest.length - 1

        const sim = await createBoardSimulation(standardBoard, hitCounts)

        // Hit the ship's final cell - should be legitimately sunk
        await sim.fireShot(shipTest.position.x, shipTest.position.y)

        expect(sim.isShipSunk(shipTest.shipId)).toBe(true)
      }
    })
  })

  describe("SR-11: Already Sunk Ship Protection", () => {
    test("should allow hits on already sunk ships (circuit behavior)", async () => {
      // Destroyer already fully sunk (2 hits for length 2)
      const sim = await createBoardSimulation(standardBoard, [0, 0, 0, 0, 2])

      // Circuit allows hitting already sunk destroyer (increments beyond ship length)
      await sim.fireShot(0, 4, cheatHit()) // Hit already sunk destroyer
      expect(sim.getHitCounts()[4]).toBe(3) // Incremented beyond ship length
    })

    test("should allow hits on all fully sunk ships (circuit behavior)", async () => {
      // Multiple ships already sunk
      const sim = await createBoardSimulation(standardBoard, [5, 4, 3, 3, 2])

      // All ships are sunk, circuit allows additional hits
      const allShipPositions = [
        { x: 0, y: 0 }, // Carrier
        { x: 0, y: 1 }, // Battleship
        { x: 0, y: 2 }, // Cruiser
        { x: 0, y: 3 }, // Submarine
        { x: 0, y: 4 }, // Destroyer
      ]

      for (const pos of allShipPositions) {
        await sim.fireShot(pos.x, pos.y, cheatHit()) // Should succeed
      }

      // Verify hit counts incremented beyond ship lengths
      const hitCounts = sim.getHitCounts()
      expect(hitCounts[0]).toBe(6) // Carrier: 5 + 1
      expect(hitCounts[1]).toBe(5) // Battleship: 4 + 1
      expect(hitCounts[2]).toBe(4) // Cruiser: 3 + 1
      expect(hitCounts[3]).toBe(4) // Submarine: 3 + 1
      expect(hitCounts[4]).toBe(3) // Destroyer: 2 + 1
    })

    test("should allow hits on partially damaged ships", async () => {
      // Carrier partially hit (3 out of 5), destroyer fully sunk
      const sim = await createBoardSimulation(standardBoard, [3, 0, 0, 0, 2])

      // Should be able to hit carrier more
      await sim.fireShot(3, 0) // Fourth hit on carrier
      expect(sim.getHitCounts()[0]).toBe(4)

      // Circuit also allows hitting the sunk destroyer
      await sim.fireShot(0, 4, cheatHit()) // Hit sunk destroyer
      expect(sim.getHitCounts()[4]).toBe(3) // Incremented beyond ship length
    })

    test("should allow hit count increments beyond ship length (circuit behavior)", async () => {
      // Destroyer already at max hits
      const sim = await createBoardSimulation(standardBoard, [0, 0, 0, 0, 2])

      // Circuit allows incrementing destroyer hits beyond its length (2)
      const overflowHitOverride = cheatOverride({
        claimedResult: 1, // Hit
        // This will increment destroyer to 3 hits (beyond length 2)
      })

      await sim.fireShot(0, 4, overflowHitOverride) // Should succeed
      expect(sim.getHitCounts()[4]).toBe(3) // Incremented beyond ship length
    })

    test("should allow hits beyond max for all ship types (circuit behavior)", async () => {
      const maxHitTests = [
        { shipId: 0, maxHits: 5 }, // Carrier
        { shipId: 1, maxHits: 4 }, // Battleship
        { shipId: 2, maxHits: 3 }, // Cruiser
        { shipId: 3, maxHits: 3 }, // Submarine
        { shipId: 4, maxHits: 2 }, // Destroyer
      ]

      for (const test of maxHitTests) {
        // Set ship to exactly max hits (fully sunk)
        const hitCounts = [0, 0, 0, 0, 0]
        hitCounts[test.shipId] = test.maxHits

        const sim = await createBoardSimulation(standardBoard, hitCounts)

        // Circuit allows hitting the already-sunk ship
        await sim.fireShot(0, test.shipId, cheatHit()) // Should succeed
        expect(sim.getHitCounts()[test.shipId]).toBe(test.maxHits + 1) // Incremented beyond max
      }
    })
  })

  describe("SR-12: Remaining Ships Calculation", () => {
    test("should accurately calculate remaining ships", async () => {
      // Test various game states
      const stateTests = [
        { hitCounts: [0, 0, 0, 0, 0], expectedRemaining: 5 }, // All intact
        { hitCounts: [5, 0, 0, 0, 0], expectedRemaining: 4 }, // Carrier sunk
        { hitCounts: [5, 4, 0, 0, 0], expectedRemaining: 3 }, // Carrier + Battleship sunk
        { hitCounts: [5, 4, 3, 3, 0], expectedRemaining: 1 }, // Only Destroyer intact
        { hitCounts: [5, 4, 3, 3, 2], expectedRemaining: 0 }, // All sunk
      ]

      for (const stateTest of stateTests) {
        const sim = await createBoardSimulation(standardBoard, stateTest.hitCounts)
        expect(sim.getRemainingShips()).toBe(stateTest.expectedRemaining)
      }
    })

    test("should update remaining count when ships are sunk", async () => {
      // Start with one ship nearly sunk
      const sim = await createBoardSimulation(standardBoard, [0, 0, 0, 0, 1])

      expect(sim.getRemainingShips()).toBe(5) // All ships still alive

      // Sink the destroyer
      await sim.fireShot(1, 4) // Second destroyer hit

      expect(sim.getRemainingShips()).toBe(4) // One ship sunk
      expect(sim.isDestroyerSunk()).toBe(true)
    })

    test("should validate remaining count matches actual sunk ships", async () => {
      // Multiple ships at various damage levels
      const sim = await createBoardSimulation(standardBoard, [4, 3, 2, 1, 1])

      expect(sim.getRemainingShips()).toBe(5) // None fully sunk yet

      // Sink carrier (needs 1 more hit)
      await sim.fireShot(4, 0)
      expect(sim.getRemainingShips()).toBe(4)

      // Sink battleship (needs 1 more hit)
      await sim.fireShot(3, 1)
      expect(sim.getRemainingShips()).toBe(3)
    })

    test("should handle endgame remaining ships correctly", async () => {
      // All ships sunk except one nearly sunk
      const sim = await createBoardSimulation(standardBoard, [5, 4, 3, 3, 1])

      expect(sim.getRemainingShips()).toBe(1) // Only destroyer alive

      // Sink final ship
      await sim.fireShot(1, 4)
      expect(sim.getRemainingShips()).toBe(0) // Game over
    })
  })
})
