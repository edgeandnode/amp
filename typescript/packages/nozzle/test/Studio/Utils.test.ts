import { describe, it } from "vitest"
import * as Utils from "../../src/Studio/Utils.js"

describe("Nozzle/Utils", () => {
  describe("foundryOutputPathExcluded", () => {
    it("should return true if the path is allowed", ({ expect }) => {
      const actual = Utils.foundryOutputPathIncluded("Counter.sol/Counter.json")
      expect(actual).toBe(true)
    })
    it("should exclude paths that match the foundry exclusion list", ({ expect }) => {
      expect(Utils.foundryOutputPathIncluded("build-info/abc123.json")).toBe(false)
      expect(Utils.foundryOutputPathIncluded("console.sol/console.json")).toBe(false)
      expect(Utils.foundryOutputPathIncluded("Test.sol/Test.json")).toBe(false)
      // even though we want to include Counter, we want to exclude the script and test solidity files from the ouput Counter contract
      expect(Utils.foundryOutputPathIncluded("Counter.s.sol/CounterScript.json")).toBe(false)
      expect(Utils.foundryOutputPathIncluded("Counter.t.sol/CounterTest.json")).toBe(false)
    })
  })
})
