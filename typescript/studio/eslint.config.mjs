//  @ts-check

import { tanstackConfig } from "@tanstack/eslint-config"

import rootConfig from "../../eslint.config.mjs"

export default [
  ...rootConfig,
  ...tanstackConfig,
  {
    ignores: ["prettier.config.js"],
  },
  {
    rules: {
      // Disable @effect/dprint to avoid conflicts with import sorting
      "@effect/dprint": "off",
      // Disable tanstack's import/order rule to avoid conflicts with simple-import-sort
      "import/order": "off",
      "import-x/order": "off",
      // Disable sort-imports to avoid conflicts
      "sort-imports": "off",
      // Enable simple-import-sort for import sorting
      "simple-import-sort/imports": [
        "error",
        {
          groups: [
            // Node.js builtins
            ["^node:"],
            // External packages
            ["^@?\\w"],
            // Internal packages
            [
              "^(@|components|utils|config|hooks|services|types|lib|styles)(/.*|$)",
            ],
            // Side effect imports
            ["^\\u0000"],
            // Parent imports
            ["^\\.\\.(?!/?$)", "^\\.\\./?$"],
            // Other relative imports
            ["^\\./(?=.*/)(?!/?$)", "^\\.(?!/?$)", "^\\./?$"],
            // Type imports
            ["^.+\\.?types$", "^.+\\?types$"],
          ],
        },
      ],
      "simple-import-sort/exports": "error",
    },
  },
]
