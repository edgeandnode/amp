#!/usr/bin/env node
/**
 * FFI Proof Generator for Battleship ZK-SNARK Tests
 *
 * This script is called by Solidity tests via FFI to generate real ZK proofs.
 * It reads JSON input from stdin and outputs proof data as JSON to stdout.
 *
 * Usage: echo '{"type":"board","input":{...}}' | bun run test/ffi.ts
 */

import {
  type BoardCircuitInput,
  loadBoardCircuitTester,
  loadShotCircuitTester,
  type ShotCircuitInput,
} from "./utils.ts"

export interface BoardProofRequest {
  type: "board"
  input: BoardCircuitInput
}

export interface ShotProofRequest {
  type: "shot"
  input: ShotCircuitInput
}

export type ProofRequest = BoardProofRequest | ShotProofRequest

export interface ProofResponse {
  success: boolean
  error?: string
  proof?: {
    piA: [string, string]
    piB: [[string, string], [string, string]]
    piC: [string, string]
    publicInputs: Array<string>
  }
  publicOutputs?: {
    commitment?: string
    initialStateCommitment?: string
    newCommitment?: string
    remainingShips?: number
  }
}

/**
 * Read input from stdin
 */
async function readStdin(): Promise<string> {
  return new Promise((resolve, reject) => {
    let input = ""

    process.stdin.setEncoding("utf8")
    process.stdin.on("data", (chunk) => {
      input += chunk
    })

    process.stdin.on("end", () => {
      resolve(input.trim())
    })

    process.stdin.on("error", (error) => {
      reject(error)
    })
  })
}

/**
 * Generate board proof using existing test utilities
 */
async function generateBoardProof(request: BoardCircuitInput): Promise<ProofResponse> {
  try {
    console.error("Loading board circuit tester...")
    const boardTester = await loadBoardCircuitTester()
    console.error("Generating board proof...")
    const proofOutput = await boardTester.prove(request)
    console.error("Board proof generated successfully")

    return {
      success: true,
      proof: {
        piA: [proofOutput.proof.pi_a[0].toString(), proofOutput.proof.pi_a[1].toString()],
        piB: [
          [proofOutput.proof.pi_b[0][1].toString(), proofOutput.proof.pi_b[0][0].toString()],
          [proofOutput.proof.pi_b[1][1].toString(), proofOutput.proof.pi_b[1][0].toString()],
        ],
        piC: [proofOutput.proof.pi_c[0].toString(), proofOutput.proof.pi_c[1].toString()],
        publicInputs: proofOutput.publicSignals.map((signal) => signal.toString()),
      },
      publicOutputs: {
        commitment: proofOutput.publicSignals[0].toString(),
        initialStateCommitment: proofOutput.publicSignals[1].toString(),
      },
    }
  } catch (error) {
    console.error("Board proof generation failed:", error)
    return {
      success: false,
      error: `Board proof generation failed: ${error}`,
    }
  }
}

/**
 * Generate shot proof using existing test utilities
 */
async function generateShotProof(request: ShotCircuitInput): Promise<ProofResponse> {
  try {
    console.error("Loading shot circuit tester...")
    const shotTester = await loadShotCircuitTester()
    console.error("Generating shot proof...")
    const proofOutput = await shotTester.prove(request)
    console.error("Shot proof generated successfully")

    return {
      success: true,
      proof: {
        piA: [proofOutput.proof.pi_a[0].toString(), proofOutput.proof.pi_a[1].toString()],
        piB: [
          [proofOutput.proof.pi_b[0][1].toString(), proofOutput.proof.pi_b[0][0].toString()],
          [proofOutput.proof.pi_b[1][1].toString(), proofOutput.proof.pi_b[1][0].toString()],
        ],
        piC: [proofOutput.proof.pi_c[0].toString(), proofOutput.proof.pi_c[1].toString()],
        publicInputs: proofOutput.publicSignals.map((signal) => signal.toString()),
      },
      publicOutputs: {
        newCommitment: proofOutput.publicSignals[0].toString(), // Index 0 contains newCommitment
        remainingShips: Number(proofOutput.publicSignals[1].toString()),
      },
    }
  } catch (error) {
    console.error("Shot proof generation failed:", error)
    return {
      success: false,
      error: `Shot proof generation failed: ${error}`,
    }
  }
}

/**
 * Main function
 */
async function main() {
  try {
    // Read and parse input
    const inputStr = await readStdin()
    let request: any

    try {
      request = JSON.parse(inputStr)
    } catch (error) {
      const response: ProofResponse = {
        success: false,
        error: `Invalid JSON input: ${error}`,
      }
      console.log(JSON.stringify(response))
      process.exit(1)
    }

    // Generate proof or commitment based on type
    let response: ProofResponse

    if (request.type === "board") {
      response = await generateBoardProof(request.input)
    } else if (request.type === "shot") {
      response = await generateShotProof(request.input)
    } else {
      response = {
        success: false,
        error: `Unknown request type: ${request.type}`,
      }
    }

    // Output result
    console.log(JSON.stringify(response))
    process.exit(0)
  } catch (error) {
    const response: ProofResponse = {
      success: false,
      error: `Unexpected error: ${error}`,
    }
    console.log(JSON.stringify(response))
    process.exit(1)
  }
}

// Run main function
main()
