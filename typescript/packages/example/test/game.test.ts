import { describe, expect, test } from "vitest"
import { cheatHit, cheatHitCounts, cheatMiss, cheatOverride, cheatSunk, createBoardSimulation } from "./simulation.ts"

describe("battleship game simulation", () => {
  test("complete game - normal gameplay", async () => {
    const sim = await createBoardSimulation({
      carrier: [0, 0, 0], // (0,0) to (4,0) horizontal
      battleship: [0, 1, 0], // (0,1) to (3,1) horizontal
      cruiser: [0, 2, 0], // (0,2) to (2,2) horizontal
      submarine: [0, 3, 0], // (0,3) to (2,3) horizontal
      destroyer: [0, 4, 0], // (0,4) to (1,4) horizontal
      salt: 12345,
    })

    // Eliminate carrier (5 hits)
    await sim.fireShot(0, 0) // Hit
    expect(sim.getRemainingShips()).toBe(5)
    expect(sim.isShipSunk(0)).toBe(false)

    await sim.fireShot(1, 0) // Hit
    await sim.fireShot(2, 0) // Hit
    await sim.fireShot(3, 0) // Hit
    await sim.fireShot(4, 0) // Sunk
    expect(sim.getRemainingShips()).toBe(4)
    expect(sim.isShipSunk(0)).toBe(true)

    // Eliminate battleship (4 hits)
    await sim.fireShot(0, 1) // Hit
    await sim.fireShot(1, 1) // Hit
    await sim.fireShot(2, 1) // Hit
    await sim.fireShot(3, 1) // Sunk
    expect(sim.getRemainingShips()).toBe(3)
    expect(sim.isShipSunk(1)).toBe(true)

    // Eliminate cruiser (3 hits)
    await sim.fireShot(0, 2) // Hit
    await sim.fireShot(1, 2) // Hit
    await sim.fireShot(2, 2) // Sunk
    expect(sim.getRemainingShips()).toBe(2)
    expect(sim.isShipSunk(2)).toBe(true)

    // Eliminate submarine (3 hits)
    await sim.fireShot(0, 3) // Hit
    await sim.fireShot(1, 3) // Hit
    await sim.fireShot(2, 3) // Sunk
    expect(sim.getRemainingShips()).toBe(1)
    expect(sim.isShipSunk(3)).toBe(true)

    // Eliminate destroyer (2 hits)
    await sim.fireShot(0, 4) // Hit
    await sim.fireShot(1, 4) // Sunk - game over
    expect(sim.getRemainingShips()).toBe(0)
    expect(sim.isShipSunk(4)).toBe(true)
  })

  test("miss shots", async () => {
    const sim = await createBoardSimulation({
      carrier: [0, 0, 0],
      battleship: [0, 1, 0],
      cruiser: [0, 2, 0],
      submarine: [0, 3, 0],
      destroyer: [0, 4, 0],
      salt: 54321,
    })

    // Shoot at empty spaces
    await sim.fireShot(5, 5) // Miss
    expect(sim.getRemainingShips()).toBe(5)
    expect(sim.getCellState(5, 5)).toBe("miss")

    await sim.fireShot(9, 9) // Miss
    expect(sim.getRemainingShips()).toBe(5)
    expect(sim.getCellState(9, 9)).toBe("miss")
  })

  test("duplicate shot validation", async () => {
    const sim = await createBoardSimulation({
      carrier: [0, 0, 0],
      battleship: [0, 1, 0],
      cruiser: [0, 2, 0],
      submarine: [0, 3, 0],
      destroyer: [0, 4, 0],
      salt: 98765,
    })

    // First shot should succeed
    await sim.fireShot(0, 0) // Hit carrier
    expect(sim.getCellState(0, 0)).toBe("hit")

    // Second shot to same location should fail
    await expect(sim.fireShot(0, 0)).rejects.toThrow("already been shot")

    // Miss and then shoot same spot again
    await sim.fireShot(5, 5) // Miss
    expect(sim.getCellState(5, 5)).toBe("miss")

    await expect(sim.fireShot(5, 5)).rejects.toThrow("already been shot")
  })

  describe("security tests - cheat attempts", () => {
    test("cheat: claim miss when actually hit", async () => {
      const sim = await createBoardSimulation({
        carrier: [2, 2, 0], // (2,2) to (6,2) horizontal
        battleship: [2, 3, 0], // (2,3) to (5,3) horizontal
        cruiser: [2, 4, 0], // (2,4) to (4,4) horizontal
        submarine: [2, 5, 0], // (2,5) to (4,5) horizontal
        destroyer: [2, 6, 0], // (2,6) to (3,6) horizontal
        salt: 99999,
      })

      // Shot will hit carrier, but we claim miss
      await expect(sim.fireShot(2, 2, cheatMiss())).rejects.toThrow() // Circuit should reject
    })

    test("cheat: claim hit when actually miss", async () => {
      const sim = await createBoardSimulation({
        carrier: [2, 2, 0], // (2,2) to (6,2) horizontal
        battleship: [2, 3, 0], // (2,3) to (5,3) horizontal
        cruiser: [2, 4, 0], // (2,4) to (4,4) horizontal
        submarine: [2, 5, 0], // (2,5) to (4,5) horizontal
        destroyer: [2, 6, 0], // (2,6) to (3,6) horizontal
        salt: 99999,
      })

      // Shot will miss, but we claim hit
      await expect(sim.fireShot(0, 0, cheatHit())).rejects.toThrow() // Circuit should reject
    })

    test("cheat: claim wrong ship hit", async () => {
      const sim = await createBoardSimulation({
        carrier: [2, 2, 0], // (2,2) to (6,2) horizontal
        battleship: [2, 3, 0], // (2,3) to (5,3) horizontal
        cruiser: [2, 4, 0], // (2,4) to (4,4) horizontal
        submarine: [2, 5, 0], // (2,5) to (4,5) horizontal
        destroyer: [2, 6, 0], // (2,6) to (3,6) horizontal
        salt: 99999,
      })

      // Shot hits carrier (ship 0), but claim battleship (ship 1)
      await expect(sim.fireShot(3, 2, cheatHit(1))).rejects.toThrow() // Circuit should reject
    })

    test("cheat: premature sunk claim", async () => {
      const sim = await createBoardSimulation({
        carrier: [2, 2, 0], // (2,2) to (6,2) horizontal
        battleship: [2, 3, 0], // (2,3) to (5,3) horizontal
        cruiser: [2, 4, 0], // (2,4) to (4,4) horizontal
        submarine: [2, 5, 0], // (2,5) to (4,5) horizontal
        destroyer: [2, 6, 0], // (2,6) to (3,6) horizontal
        salt: 99999,
      })

      // First hit on carrier, but claim it's sunk
      await expect(sim.fireShot(4, 2, cheatSunk(0))).rejects.toThrow() // Circuit should reject
    })

    test("cheat: manipulate hit counts", async () => {
      const sim = await createBoardSimulation({
        carrier: [2, 2, 0], // (2,2) to (6,2) horizontal
        battleship: [2, 3, 0], // (2,3) to (5,3) horizontal
        cruiser: [2, 4, 0], // (2,4) to (4,4) horizontal
        submarine: [2, 5, 0], // (2,5) to (4,5) horizontal
        destroyer: [2, 6, 0], // (2,6) to (3,6) horizontal
        salt: 99999,
      })

      // Hit carrier
      await sim.fireShot(2, 2)
      // Try to reset hit counts to 0
      await expect(sim.fireShot(5, 2, cheatHitCounts([0, 0, 0, 0, 0]))).rejects.toThrow() // Circuit should reject wrong previous state
    })

    test("cheat: wrong commitment", async () => {
      const sim = await createBoardSimulation({
        carrier: [2, 2, 0], // (2,2) to (6,2) horizontal
        battleship: [2, 3, 0], // (2,3) to (5,3) horizontal
        cruiser: [2, 4, 0], // (2,4) to (4,4) horizontal
        submarine: [2, 5, 0], // (2,5) to (4,5) horizontal
        destroyer: [2, 6, 0], // (2,6) to (3,6) horizontal
        salt: 99999,
      })

      // Use fake previous commitment
      await expect(sim.fireShot(6, 2, cheatOverride({ previousCommitment: "123456789" }))).rejects.toThrow() // Circuit should reject
    })

    test("cheat: manipulate board commitment", async () => {
      const sim = await createBoardSimulation({
        carrier: [2, 2, 0], // (2,2) to (6,2) horizontal
        battleship: [2, 3, 0], // (2,3) to (5,3) horizontal
        cruiser: [2, 4, 0], // (2,4) to (4,4) horizontal
        submarine: [2, 5, 0], // (2,5) to (4,5) horizontal
        destroyer: [2, 6, 0], // (2,6) to (3,6) horizontal
        salt: 99999,
      })

      // Try to use different board commitment
      await expect(sim.fireShot(1, 1, cheatOverride({ boardCommitment: "123456789" }))).rejects.toThrow() // Circuit should reject
    })
  })
})
