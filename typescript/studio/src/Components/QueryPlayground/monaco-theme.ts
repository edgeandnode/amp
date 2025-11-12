import type * as monacoType from "monaco-editor"

export const MONACO_AMP_DARK = "amp-dark-theme"

export const customThemeDefinition: monacoType.editor.IStandaloneThemeData = {
  base: "vs-dark",
  inherit: false,
  rules: [
    {
      token: "keyword",
      foreground: "ffef60",
      fontStyle: "bold",
    },
    {
      token: "string",
      foreground: "70b8ff",
    },
    {
      token: "comment",
      foreground: "848192",
      fontStyle: "italic",
    },
    {
      token: "number",
      foreground: "79b8ff",
    },
    {
      token: "variable",
      foreground: "f4f6ff",
    },
    {
      token: "identifier",
      foreground: "f4f6ff",
    },
  ],
  colors: {
    "editor.background": "#06060b",
    "editor.foreground": "#f4f6ff",
    "editor.selectionBackground": "#281C5C",
    "editor.lineHighlightBackground": "#0a0a10",
    "editorCursor.foreground": "#ffef60",
    "editorLineNumber.foreground": "#848192",
    "editorLineNumber.activeForeground": "#f4f6ff",
    "editorHoverWidget.background": "#1a1b1f",
    "editorHoverWidget.foreground": "#f4f6ff",
    "editorHoverWidget.border": "#333",
    "editorHoverWidget.statusBarBackground": "#1a1b1f",
    "editorSuggestWidget.background": "#1a1b1f",
    "editorSuggestWidget.border": "#333",
    "editorSuggestWidget.foreground": "#f4f6ff",
    "editorSuggestWidget.selectedBackground": "#1a1b1f",
    "editorSuggestWidget.highlightForeground": "#ffef60",
    "editorWidget.background": "#1a1b1f",
    "editorWidget.foreground": "#f4f6ff",
    "editorWidget.resizeBorder": "#333",
    "input.background": "#1a1b1f",
    "input.foreground": "#f4f6ff",
    "input.border": "#333",
    "scrollbarSlider.background": "#16132A",
    "scrollbarSlider.hoverBackground": "#242236",
    "scrollbarSlider.activeBackground": "#16132A",
  },
}

export function registerCustomTheme(monaco: typeof monacoType) {
  monaco.editor.defineTheme(MONACO_AMP_DARK, customThemeDefinition)
}
