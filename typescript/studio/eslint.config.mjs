//  @ts-check

import { tanstackConfig } from "@tanstack/eslint-config"

import rootConfig from "../../eslint.config.mjs"

// Filter out tanstack configs that conflict with root config plugins
const filteredTanstackConfig = tanstackConfig.filter((config) => {
  // Skip configs that define the @typescript-eslint plugin since it's already in rootConfig
  if (config.plugins && "@typescript-eslint" in config.plugins) {
    return false
  }
  return true
})

export default [
  ...rootConfig,
  ...filteredTanstackConfig,
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
      "import-x/newline-after-import": "off",
      // Disable sort-imports to avoid conflicts with @effect/dprint formatting
      "sort-imports": "off",
    },
  },
]
