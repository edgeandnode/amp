import { spawn } from "child_process"
import * as path from "path"
import { describe, expect, test } from "vitest"
import type { ProofRequest, ProofResponse } from "./ffi.ts"
import type { BoardCircuitInput, ImpactCircuitInput } from "./utils.ts"
import { poseidonHash, poseidonHashSalt } from "./utils.ts"

/**
 * Call the FFI script with JSON input and return parsed response
 */
async function callFFI(type: "board" | "impact", request: ProofRequest): Promise<ProofResponse> {
  return new Promise((resolve, reject) => {
    const scriptPath = path.resolve(import.meta.dirname, "ffi.ts")
    const child = spawn("node", ["--experimental-transform-types", scriptPath, type, JSON.stringify(request)], {
      stdio: ["pipe", "pipe", "ignore"], // Ignore stderr to suppress experimental warnings
    })

    // Add timeout for the entire test
    const timeout = setTimeout(() => {
      child.kill()
      reject(new Error("FFI call timed out after 35 seconds"))
    }, 35000) // 35 seconds total timeout

    let stdout = ""

    child.stdout.on("data", (data) => {
      stdout += data.toString()
    })

    child.on("close", (code) => {
      clearTimeout(timeout)
      if (code !== 0) {
        reject(new Error(`FFI script failed with code ${code}:\nstdout: ${stdout}`))
        return
      }

      try {
        const response = JSON.parse(stdout.trim())
        resolve(response)
      } catch (error) {
        reject(new Error(`Failed to parse FFI response: ${error}\nRaw output: ${stdout}`))
      }
    })

    child.on("error", (error) => {
      clearTimeout(timeout)
      reject(error)
    })

    // Send JSON input
    child.stdin.write(JSON.stringify(request))
    child.stdin.end()
  })
}

/**
 * Valid ship configuration for testing
 */
const validBoardConfig: BoardCircuitInput = {
  carrier: [0, 0, 0], // (0,0) horizontal, length 5: (0,0)-(4,0)
  battleship: [0, 1, 0], // (0,1) horizontal, length 4: (0,1)-(3,1)
  cruiser: [0, 2, 0], // (0,2) horizontal, length 3: (0,2)-(2,2)
  submarine: [0, 3, 0], // (0,3) horizontal, length 3: (0,3)-(2,3)
  destroyer: [0, 4, 0], // (0,4) horizontal, length 2: (0,4)-(1,4)
  salt: 12345,
}

describe("FFI Script Tests", () => {
  test("should generate board proof", async () => {
    const response = await callFFI("board", validBoardConfig)
    expect(response.success).toBe(true)
    expect(response.error).toBeUndefined()
    expect(response.proof).toBeDefined()

    // Validate proof structure
    const proof = response.proof!
    expect(proof.piA).toHaveLength(2)
    expect(proof.piB).toHaveLength(2)
    expect(proof.piB[0]).toHaveLength(2)
    expect(proof.piB[1]).toHaveLength(2)
    expect(proof.piC).toHaveLength(2)

    // Validate that all proof elements are valid numbers
    expect(() => BigInt(proof.piA[0])).not.toThrow()
    expect(() => BigInt(proof.piA[1])).not.toThrow()
    expect(() => BigInt(proof.piC[0])).not.toThrow()
    expect(() => BigInt(proof.piC[1])).not.toThrow()

    console.log("Board proof generated successfully")
  })

  test("should generate shot proof", async () => {
    // Generate board commitment directly in test using utils
    const boardCommitment = await poseidonHash([
      ...validBoardConfig.carrier,
      ...validBoardConfig.battleship,
      ...validBoardConfig.cruiser,
      ...validBoardConfig.submarine,
      ...validBoardConfig.destroyer,
      validBoardConfig.salt,
    ])

    // Generate initial state commitment (all ships with 0 hits)
    const previousCommitment = await poseidonHashSalt([0, 0, 0, 0, 0], 12345)

    // Create shot input - shoot at water (5,5)
    const shotInput: ImpactCircuitInput = {
      // Public inputs
      previousCommitment,
      targetX: 5,
      targetY: 5,
      claimedResult: 0, // MISS
      claimedShipId: 255, // No ship hit
      boardCommitment,
      // Private inputs
      ships: [
        validBoardConfig.carrier,
        validBoardConfig.battleship,
        validBoardConfig.cruiser,
        validBoardConfig.submarine,
        validBoardConfig.destroyer,
      ],
      previousHitCounts: [0, 0, 0, 0, 0],
      salt: 12345,
    }

    const response = await callFFI("impact", shotInput)
    expect(response.success).toBe(true)
    expect(response.error).toBeUndefined()
    expect(response.proof).toBeDefined()

    // Validate proof structure
    const proof = response.proof!
    expect(proof.piA).toHaveLength(2)
    expect(proof.piB).toHaveLength(2)
    expect(proof.piB[0]).toHaveLength(2)
    expect(proof.piB[1]).toHaveLength(2)
    expect(proof.piC).toHaveLength(2)

    // Validate that all proof elements are valid numbers
    expect(() => BigInt(proof.piA[0])).not.toThrow()
    expect(() => BigInt(proof.piA[1])).not.toThrow()
    expect(() => BigInt(proof.piC[0])).not.toThrow()
    expect(() => BigInt(proof.piC[1])).not.toThrow()

    console.log("Shot proof generated successfully")
  })
})
