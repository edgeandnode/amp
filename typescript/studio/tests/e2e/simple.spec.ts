/**
 * Simple E2E Test to verify Playwright setup
 */

import { test, expect } from "@playwright/test"

test("application loads", async ({ page }) => {
  // Navigate to the application
  await page.goto("/")

  // Wait for the page to load
  await page.waitForLoadState("networkidle")

  // Check if the page loaded successfully
  const title = await page.title()
  console.log("Page title:", title)

  // Take a screenshot for debugging
  await page.screenshot({ path: "debug-app-load.png" })

  // Basic assertion
  expect(title).toBeTruthy()
})
