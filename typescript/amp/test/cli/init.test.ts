import { describe, expect, it } from "vitest"
import { localEvmRpc } from "../../src/cli/templates/local-evm-rpc.ts"
import type { TemplateAnswers } from "../../src/cli/templates/Template.ts"
import { resolveTemplateFile } from "../../src/cli/templates/Template.ts"

describe("amp init command", () => {
  describe("template file resolution", () => {
    it("should resolve static template files", () => {
      const staticFile = "static content"
      const answers: TemplateAnswers = {
        datasetName: "test_dataset",
        datasetVersion: "1.0.0",
        projectName: "Test Project",
        network: "anvil",
      }

      const result = resolveTemplateFile(staticFile, answers)
      expect(result).toBe("static content")
    })

    it("should resolve dynamic template files with answers", () => {
      const dynamicFile = (answers: TemplateAnswers) =>
        `Dataset: ${answers.datasetName}, Version: ${answers.datasetVersion}`

      const answers: TemplateAnswers = {
        datasetName: "test_dataset",
        datasetVersion: "1.0.0",
        projectName: "Test Project",
        network: "anvil",
      }

      const result = resolveTemplateFile(dynamicFile, answers)
      expect(result).toBe("Dataset: test_dataset, Version: 1.0.0")
    })
  })

  describe("local-evm-rpc template", () => {
    it("should have the correct template metadata", () => {
      expect(localEvmRpc.name).toBe("local-evm-rpc")
      expect(localEvmRpc.description).toBe("Local development with Anvil")
      expect(localEvmRpc.files).toBeDefined()
    })

    it("should generate amp.config.ts with custom dataset name and version", () => {
      const answers: TemplateAnswers = {
        datasetName: "custom_data",
        datasetVersion: "2.0.0",
        projectName: "Custom Project",
        network: "anvil",
      }

      const configFile = localEvmRpc.files["amp.config.ts"]
      const content = resolveTemplateFile(configFile, answers)

      expect(content).toContain("name: \"custom_data\"")
      expect(content).toContain("version: \"2.0.0\"")
      expect(content).toContain("network: \"anvil\"")
    })

    it("should generate README-local-evm-rpc.md with project name", () => {
      const answers: TemplateAnswers = {
        datasetName: "my_dataset",
        datasetVersion: "1.0.0",
        projectName: "My Cool Project",
        network: "anvil",
      }

      const readmeFile = localEvmRpc.files["README-local-evm-rpc.md"]
      const content = resolveTemplateFile(readmeFile, answers)

      expect(content).toContain("# My Cool Project")
      expect(content).toContain("**Dataset**: `my_dataset` (version `1.0.0`)")
      expect(content).toContain("**Network**: Anvil local testnet")
    })

    it("should include all required files", () => {
      const requiredFiles = [
        "amp.config.ts",
        "README-local-evm-rpc.md",
        "contracts/foundry.toml",
        "contracts/src/Counter.sol",
        "contracts/script/Deploy.s.sol",
        "contracts/remappings.txt",
        ".gitignore",
      ]

      for (const file of requiredFiles) {
        expect(localEvmRpc.files[file]).toBeDefined()
      }
    })

    it("should generate valid Solidity contract", () => {
      const answers: TemplateAnswers = {
        datasetName: "test_dataset",
        datasetVersion: "1.0.0",
        projectName: "Test",
        network: "anvil",
      }

      const counterFile = localEvmRpc.files["contracts/src/Counter.sol"]
      const content = resolveTemplateFile(counterFile, answers)

      expect(content).toContain("pragma solidity")
      expect(content).toContain("contract Counter")
      expect(content).toContain("event Count(uint256 count)")
      expect(content).toContain("event Transfer(address indexed from, address indexed to, uint256 value)")
    })
  })

  describe("dataset name validation", () => {
    it("should accept valid dataset names", () => {
      const validNames = [
        "my_dataset",
        "dataset_123",
        "a",
        "_underscore",
        "lower_case_only",
      ]

      for (const name of validNames) {
        const regex = /^[a-z_][a-z0-9_]*$/
        expect(regex.test(name)).toBe(true)
      }
    })

    it("should reject invalid dataset names", () => {
      const invalidNames = [
        "MyDataset", // uppercase
        "123dataset", // starts with number
        "my-dataset", // contains hyphen
        "my dataset", // contains space
        "dataset!", // special character
        "UPPERCASE",
      ]

      for (const name of invalidNames) {
        const regex = /^[a-z_][a-z0-9_]*$/
        expect(regex.test(name)).toBe(false)
      }
    })
  })
})
