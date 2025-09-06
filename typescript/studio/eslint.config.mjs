//  @ts-check

import { tanstackConfig } from "@tanstack/eslint-config"

import rootConfig from "../../eslint.config.mjs"

export default [
  ...rootConfig,
  ...tanstackConfig,
  {
    ignores: ["prettier.config.js", "eslint.config.mjs"],
  },
  {
    languageOptions: {
      parserOptions: {
        tsconfigRootDir: import.meta.dirname,
        project: "./tsconfig.json",
      },
    },
    rules: {
      // Disable tanstack's import/order rule to avoid conflicts with @effect/dprint
      "import/order": "off",
      "import-x/order": "off",
      // Disable sort-imports to avoid conflicts with @effect/dprint formatting
      "sort-imports": "off",
    },
  },
]
