/**
 * Global type declarations for Nozzle Studio
 */

declare global {
  interface Window {
    monacoInstance?: typeof import("monaco-editor")
  }
}

export {}
