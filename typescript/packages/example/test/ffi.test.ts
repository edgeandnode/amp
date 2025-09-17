import { describe, test, expect } from "vitest"
import { spawn } from "child_process"
import * as path from "path"
import type { BoardCircuitInput, ShotCircuitInput } from "./utils.ts"
import { poseidonHash, poseidonHashSalt } from "./utils.ts"
import type { ProofRequest, ProofResponse } from "./ffi.ts"
import type { BoardProofRequest, ShotProofRequest } from "./ffi.ts"

/**
 * Call the FFI script with JSON input and return parsed response
 */
async function callFFI(request: ProofRequest): Promise<ProofResponse> {
  return new Promise((resolve, reject) => {
    const scriptPath = path.resolve(import.meta.dirname, "ffi.ts")
    const child = spawn("node", ["--experimental-transform-types", scriptPath], {
      stdio: ["pipe", "pipe", "ignore"] // Ignore stderr to suppress experimental warnings
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
  carrier: [0, 0, 0],      // (0,0) horizontal, length 5: (0,0)-(4,0)
  battleship: [0, 1, 0],   // (0,1) horizontal, length 4: (0,1)-(3,1)
  cruiser: [0, 2, 0],      // (0,2) horizontal, length 3: (0,2)-(2,2)
  submarine: [0, 3, 0],    // (0,3) horizontal, length 3: (0,3)-(2,3)
  destroyer: [0, 4, 0],    // (0,4) horizontal, length 2: (0,4)-(1,4)
  salt: 12345
}

describe("FFI Script Tests", () => {

  test("should have accessible FFI script", () => {
    const scriptPath = path.resolve(import.meta.dirname, "ffi.ts")
    expect(() => {
      require('fs').accessSync(scriptPath, require('fs').constants.F_OK | require('fs').constants.R_OK)
    }).not.toThrow()
  })

  test("should generate board proof", async () => {
    const request: BoardProofRequest = {
      type: "board",
      input: validBoardConfig
    }

    const response = await callFFI(request)

    expect(response.success).toBe(true)
    expect(response.error).toBeUndefined()
    expect(response.proof).toBeDefined()
    expect(response.publicOutputs?.commitment).toBeDefined()
    expect(response.publicOutputs?.initialStateCommitment).toBeDefined()

    // Validate proof structure
    const proof = response.proof!
    expect(proof.piA).toHaveLength(2)
    expect(proof.piB).toHaveLength(2)
    expect(proof.piB[0]).toHaveLength(2)
    expect(proof.piB[1]).toHaveLength(2)
    expect(proof.piC).toHaveLength(2)
    expect(proof.publicInputs).toHaveLength(2) // Board proof now has 2 public outputs

    // Validate that all proof elements are valid numbers
    expect(() => BigInt(proof.piA[0])).not.toThrow()
    expect(() => BigInt(proof.piA[1])).not.toThrow()
    expect(() => BigInt(proof.piC[0])).not.toThrow()
    expect(() => BigInt(proof.piC[1])).not.toThrow()
    expect(() => BigInt(proof.publicInputs[0])).not.toThrow()

    console.log("Board proof generated successfully")
    console.log("Board commitment:", proof.publicInputs[0])
    console.log("Initial state commitment:", proof.publicInputs[1])
  })

  test("should generate shot proof", async () => {
    // Generate board commitment directly in test using utils
    const boardCommitment = await poseidonHash([
      ...validBoardConfig.carrier,
      ...validBoardConfig.battleship, 
      ...validBoardConfig.cruiser,
      ...validBoardConfig.submarine,
      ...validBoardConfig.destroyer,
      validBoardConfig.salt
    ])

    // Generate initial state commitment (all ships with 0 hits)
    const previousCommitment = await poseidonHashSalt([0, 0, 0, 0, 0], 12345)

    // Create shot input - shoot at water (5,5)
    const shotInput: ShotCircuitInput = {
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
        validBoardConfig.destroyer
      ],
      previousHitCounts: [0, 0, 0, 0, 0],
      salt: 12345
    }

    const request: ShotProofRequest = {
      type: "shot",
      input: shotInput
    }

    const response = await callFFI(request)

    expect(response.success).toBe(true)
    expect(response.error).toBeUndefined()
    expect(response.proof).toBeDefined()
    expect(response.publicOutputs?.newCommitment).toBeDefined()
    expect(response.publicOutputs?.remainingShips).toBe(5)

    // Validate proof structure
    const proof = response.proof!
    expect(proof.piA).toHaveLength(2)
    expect(proof.piB).toHaveLength(2)
    expect(proof.piB[0]).toHaveLength(2)
    expect(proof.piB[1]).toHaveLength(2)
    expect(proof.piC).toHaveLength(2)
    expect(proof.publicInputs).toHaveLength(8) // Shot proof now has 8 total public signals (6 inputs + 2 outputs)

    // Validate that all proof elements are valid numbers
    expect(() => BigInt(proof.piA[0])).not.toThrow()
    expect(() => BigInt(proof.piA[1])).not.toThrow()
    expect(() => BigInt(proof.piC[0])).not.toThrow()
    expect(() => BigInt(proof.piC[1])).not.toThrow()

    console.log("Shot proof generated successfully")
    console.log("New commitment:", response.publicOutputs?.newCommitment)
    console.log("Remaining ships:", response.publicOutputs?.remainingShips)
  })

  test("should handle invalid JSON input", async () => {
    return new Promise<void>((resolve, reject) => {
      const scriptPath = path.resolve(import.meta.dirname, "ffi.ts")
      const child = spawn("node", ["--experimental-transform-types", scriptPath], {
        stdio: ["pipe", "pipe", "ignore"] // Ignore stderr to suppress experimental warnings
      })

      let stdout = ""

      child.stdout.on("data", (data) => {
        stdout += data.toString()
      })

      child.on("close", (code) => {
        expect(code).toBe(1) // Should exit with error code

        try {
          const response = JSON.parse(stdout.trim())
          expect(response.success).toBe(false)
          expect(response.error).toContain("Invalid JSON input")
          resolve()
        } catch (error) {
          reject(new Error(`Failed to parse error response: ${error}`))
        }
      })

      child.on("error", (error) => {
        reject(error)
      })

      // Send invalid JSON
      child.stdin.write("invalid json")
      child.stdin.end()
    })
  })

  test("should handle unknown request type", async () => {
    const request = {
      type: "unknown",
      input: {}
    }

    const response = await callFFI(request as any)

    expect(response.success).toBe(false)
    expect(response.error).toContain("Unknown request type")
  })
})
