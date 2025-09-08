/**
 * Mock Monaco Editor for testing
 * Provides minimal implementation of Monaco Editor interfaces needed for tests
 */

// Mock Monaco Editor types and classes
export namespace languages {
  export enum CompletionTriggerKind {
    Invoke = 0,
    TriggerCharacter = 1,
    TriggerForIncompleteCompletions = 2
  }

  export enum CompletionItemKind {
    Method = 0,
    Function = 1,
    Constructor = 2,
    Field = 3,
    Variable = 4,
    Class = 5,
    Struct = 6,
    Interface = 7,
    Module = 8,
    Property = 9,
    Event = 10,
    Operator = 11,
    Unit = 12,
    Value = 13,
    Constant = 14,
    Enum = 15,
    EnumMember = 16,
    Keyword = 17,
    Text = 18,
    Color = 19,
    File = 20,
    Reference = 21,
    Customcolor = 22,
    Folder = 23,
    TypeParameter = 24,
    User = 25,
    Issue = 26,
    Snippet = 27
  }

  export enum CompletionItemTag {
    Deprecated = 1
  }

  export enum CompletionItemInsertTextRule {
    None = 0,
    KeepWhitespace = 1,
    InsertAsSnippet = 4
  }

  export interface CompletionContext {
    triggerKind: CompletionTriggerKind
    triggerCharacter?: string
  }

  export interface CompletionItem {
    label: string | { label: string; detail?: string }
    kind?: CompletionItemKind
    detail?: string
    documentation?: string | IMarkdownString
    sortText?: string
    filterText?: string
    preselect?: boolean
    insertText?: string
    insertTextRules?: CompletionItemInsertTextRule
    range?: Range
    command?: Command
    commitCharacters?: string[]
    additionalTextEdits?: any[]
    tags?: CompletionItemTag[]
  }

  export interface CompletionList {
    suggestions: CompletionItem[]
    incomplete?: boolean
  }

  export interface CompletionItemProvider {
    provideCompletionItems(
      model: editor.ITextModel,
      position: Position,
      context: CompletionContext,
      token: CancellationToken
    ): CompletionList | Promise<CompletionList>
  }

  export interface Hover {
    contents: IMarkdownString[]
    range?: Range
  }

  export interface HoverProvider {
    provideHover(
      model: editor.ITextModel,
      position: Position
    ): Hover | null | Promise<Hover | null>
  }

  export interface SignatureHelpResult {
    value: {
      signatures: any[]
      activeSignature: number
      activeParameter: number
    }
    dispose(): void
  }

  export interface SignatureHelpProvider {
    signatureHelpTriggerCharacters?: string[]
    signatureHelpRetriggerCharacters?: string[]
    provideSignatureHelp(
      model: editor.ITextModel,
      position: Position
    ): SignatureHelpResult | null | Promise<SignatureHelpResult | null>
  }

  export interface Command {
    id: string
    title: string
    arguments?: any[]
  }

  // Mock provider registration functions
  export function registerCompletionItemProvider(
    languageSelector: string,
    provider: CompletionItemProvider
  ): IDisposable {
    return {
      dispose: () => {}
    }
  }

  export function registerHoverProvider(
    languageSelector: string,
    provider: HoverProvider
  ): IDisposable {
    return {
      dispose: () => {}
    }
  }

  export function registerSignatureHelpProvider(
    languageSelector: string,
    provider: SignatureHelpProvider
  ): IDisposable {
    return {
      dispose: () => {}
    }
  }
}

// Mock editor namespace  
export namespace editor {
  export interface ITextModel {
    getValue(): string
    getOffsetAt(position: Position): number
    getLineContent(lineNumber: number): string
    getWordAtPosition(position: Position): { word: string; startColumn: number; endColumn: number } | null
    dispose(): void
  }

  // Add setModelMarkers mock
  export function setModelMarkers(model: ITextModel, owner: string, markers: IMarkerData[]): void {
    // Mock implementation - in real tests, this would update the editor's markers
  }

  export function createModel(content: string, language?: string): ITextModel {
    const lines = content.split('\n')
    
    return {
      getValue: () => content,
      getOffsetAt: (position: Position) => {
        let offset = 0
        for (let i = 0; i < position.lineNumber - 1; i++) {
          offset += lines[i].length + 1 // +1 for newline
        }
        return offset + position.column - 1
      },
      getLineContent: (lineNumber: number) => {
        return lines[lineNumber - 1] || ''
      },
      getWordAtPosition: (position: Position) => {
        const line = lines[position.lineNumber - 1] || ''
        const text = line.substring(0, position.column - 1)
        const match = text.match(/(\w+)$/)
        if (match) {
          return {
            word: match[1],
            startColumn: position.column - match[1].length,
            endColumn: position.column
          }
        }
        return null
      },
      dispose: () => {}
    }
  }
}

// Mock Position class
export class Position {
  constructor(
    public lineNumber: number,
    public column: number
  ) {}
}

// Mock Range class
export class Range {
  constructor(
    public startLineNumber: number,
    public startColumn: number,
    public endLineNumber: number,
    public endColumn: number
  ) {}
}

// Mock interfaces
export interface IMarkdownString {
  value: string
  isTrusted?: boolean
}

export interface IDisposable {
  dispose(): void
}

export interface CancellationToken {
  isCancellationRequested: boolean
  onCancellationRequested(listener: () => void): IDisposable
}

// Mock MarkerSeverity for validation tests
export enum MarkerSeverity {
  Hint = 1,
  Info = 2,
  Warning = 4,
  Error = 8
}

// Mock IMarkerData interface
export interface IMarkerData {
  code?: string
  severity: MarkerSeverity
  message: string
  source?: string
  startLineNumber: number
  startColumn: number
  endLineNumber: number
  endColumn: number
  relatedInformation?: any[]
  tags?: any[]
}

