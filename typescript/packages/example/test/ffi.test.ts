import { describe, test, expect } from "vitest"
import { spawn } from "child_process"
import * as path from "path"
import type { BoardCircuitInput, ShotCircuitInput } from "./utils.ts"
import { poseidonHash, poseidonHashSalt } from "./utils.ts"

/**
 * Test interfaces matching the FFI script
 */
interface BoardProofRequest {
  type: "board"
  input: BoardCircuitInput
}

interface ShotProofRequest {
  type: "shot"
  input: ShotCircuitInput
}

type FFIRequest = BoardProofRequest | ShotProofRequest

interface ProofResponse {
  success: boolean
  error?: string
  proof?: {
    piA: [string, string]
    piB: [[string, string], [string, string]]
    piC: [string, string]
    publicInputs: string[]
  }
  publicOutputs?: {
    commitment?: string
    newCommitment?: string
    remainingShips?: number
  }
}

/**
 * Call the FFI script with JSON input and return parsed response
 */
async function callFFI(request: FFIRequest): Promise<ProofResponse> {
  return new Promise((resolve, reject) => {
    const scriptPath = path.resolve(import.meta.dirname, "ffi-mock.ts")
    const child = spawn("bun", ["run", scriptPath], {
      stdio: ["pipe", "pipe", "pipe"]
    })

    let stdout = ""
    let stderr = ""

    child.stdout.on("data", (data) => {
      stdout += data.toString()
    })

    child.stderr.on("data", (data) => {
      stderr += data.toString()
    })

    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`FFI script failed with code ${code}:\nstderr: ${stderr}\nstdout: ${stdout}`))
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

    // Validate proof structure
    const proof = response.proof!
    expect(proof.piA).toHaveLength(2)
    expect(proof.piB).toHaveLength(2)
    expect(proof.piB[0]).toHaveLength(2)
    expect(proof.piB[1]).toHaveLength(2)
    expect(proof.piC).toHaveLength(2)
    expect(proof.publicInputs).toHaveLength(1) // Board proof has 1 public input

    // Validate that all proof elements are valid numbers
    expect(() => BigInt(proof.piA[0])).not.toThrow()
    expect(() => BigInt(proof.piA[1])).not.toThrow()
    expect(() => BigInt(proof.piC[0])).not.toThrow()
    expect(() => BigInt(proof.piC[1])).not.toThrow()
    expect(() => BigInt(proof.publicInputs[0])).not.toThrow()

    console.log("Board proof generated successfully")
    console.log("Commitment from proof:", proof.publicInputs[0])
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
    expect(proof.publicInputs).toHaveLength(6) // Shot proof has 6 public inputs

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
      const child = spawn("bun", ["run", scriptPath], {
        stdio: ["pipe", "pipe", "pipe"]
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