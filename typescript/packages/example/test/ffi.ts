#!/usr/bin/env node

/**
 * FFI Proof Generator for Battleship ZK-SNARK Tests
 *
 * This script is called by Solidity tests via FFI to generate real ZK proofs.
 * It reads JSON input from stdin and outputs proof data as JSON to stdout.
 */

import {
  type BoardCircuitInput,
  type ImpactCircuitInput,
  loadBoardCircuitTester,
  loadImpactCircuitTester,
} from "./utils.ts"

export type ProofRequest = BoardCircuitInput | ImpactCircuitInput

export interface ProofResponse {
  success: boolean
  error?: string
  proof?: {
    piA: [string, string]
    piB: [[string, string], [string, string]]
    piC: [string, string]
    publicSignals: Array<string>
  }
}

/**
 * Generate board proof using existing test utilities
 */
async function generateBoardProof(request: BoardCircuitInput): Promise<ProofResponse> {
  try {
    const boardTester = await loadBoardCircuitTester()
    const proofOutput = await boardTester.prove(request)

    return {
      success: true,
      proof: {
        piA: [proofOutput.proof.pi_a[0].toString(), proofOutput.proof.pi_a[1].toString()],
        piB: [
          [proofOutput.proof.pi_b[0][1].toString(), proofOutput.proof.pi_b[0][0].toString()],
          [proofOutput.proof.pi_b[1][1].toString(), proofOutput.proof.pi_b[1][0].toString()],
        ],
        piC: [proofOutput.proof.pi_c[0].toString(), proofOutput.proof.pi_c[1].toString()],
        publicSignals: proofOutput.publicSignals.map((signal) => signal.toString()),
      },
    }
  } catch (error) {
    return {
      success: false,
      error: `Board proof generation failed: ${error}`,
    }
  }
}

/**
 * Generate shot proof using existing test utilities
 */
async function generateImpactProof(request: ImpactCircuitInput): Promise<ProofResponse> {
  try {
    const shotTester = await loadImpactCircuitTester()
    const proofOutput = await shotTester.prove(request)

    return {
      success: true,
      proof: {
        piA: [proofOutput.proof.pi_a[0].toString(), proofOutput.proof.pi_a[1].toString()],
        piB: [
          [proofOutput.proof.pi_b[0][1].toString(), proofOutput.proof.pi_b[0][0].toString()],
          [proofOutput.proof.pi_b[1][1].toString(), proofOutput.proof.pi_b[1][0].toString()],
        ],
        piC: [proofOutput.proof.pi_c[0].toString(), proofOutput.proof.pi_c[1].toString()],
        publicSignals: proofOutput.publicSignals.map((signal) => signal.toString()),
      },
    }
  } catch (error) {
    return {
      success: false,
      error: `Impact proof generation failed: ${error}`,
    }
  }
}

/**
 * Main function
 */
async function main() {
  try {
    const inputStr = process.argv[2]
    const inputJson = process.argv[3]
    let request: any

    try {
      request = JSON.parse(inputJson)
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

    if (inputStr === "board") {
      response = await generateBoardProof(request)
    } else if (inputStr === "impact") {
      response = await generateImpactProof(request)
    } else {
      response = {
        success: false,
        error: `Unknown request type: ${inputStr}`,
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
