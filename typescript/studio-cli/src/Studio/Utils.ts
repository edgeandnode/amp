import { HashSet } from "effect"
import { minimatch } from "minimatch"
import { extname } from "node:path"

import { foundryOutputExludeList } from "./Constants.js"

/**
 * Checks if a relative path should be included (not excluded) based on the exclude patterns.
 * Uses the minimatch library for proper glob pattern matching.
 *
 * @param relativePath The relative path to check
 * @param excludePatterns The set of patterns to match against
 * @returns true if the path should be included (does not match any exclude pattern), false if it should be excluded
 */
export function foundryOutputPathIncluded(
  relativePath: string,
  excludePatterns: HashSet.HashSet<string> = foundryOutputExludeList,
) {
  // Only include .json files
  if (!/\.(json)$/.test(extname(relativePath))) {
    return false
  }
  return !HashSet.some(excludePatterns, (pattern) => minimatch(relativePath, pattern))
}
