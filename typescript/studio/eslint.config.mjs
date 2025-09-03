//  @ts-check

import { tanstackConfig } from "@tanstack/eslint-config"

export default [
  ...tanstackConfig,
  {
    ignores: ["prettier.config.js", "playwright-report/**", "test-results/**"],
  },
  {
    rules: {
      // Disable @effect/dprint to avoid conflicts with import sorting
      "@effect/dprint": "off",
      // Disable tanstack's import/order rule
      "import/order": "off",
      "import-x/order": "off",
      // Disable sort-imports
      "sort-imports": "off",
      // Disable strict type checking that interferes with Monaco type imports
      "@typescript-eslint/consistent-type-imports": "off",
      "@typescript-eslint/no-unnecessary-condition": "off",
    },
  },
]
