"use client"

import type { EditorProps as MonacoEditorProps } from "@monaco-editor/react"
import MonacoEditor from "@monaco-editor/react"
import { useStore } from "@tanstack/react-form"

import { ErrorMessages } from "../Form/ErrorMessages"
import { useFieldContext } from "../Form/form"

export type EditorProps = Omit<
  MonacoEditorProps,
  "defaultLanguage" | "language"
> & {
  id: string
}
export function Editor({
  height = 450,
  id,
  theme = "vs-dark",
  ...rest
}: Readonly<EditorProps>) {
  const field = useFieldContext<string>()
  const errors = useStore(field.store, (state) => state.meta.errors)
  const touched = useStore(field.store, (state) => state.meta.isTouched)
  const hasErrors = errors.length > 0 && touched

  return (
    <div className="w-full h-full p-0 m-0 flex flex-col gap-y-3">
      <MonacoEditor
        {...rest}
        defaultLanguage="sql"
        language="sql"
        height={height}
        theme={theme}
        value={field.state.value}
        onChange={(val) => field.handleChange(val || "")}
        data-state={hasErrors ? "invalid" : undefined}
        aria-invalid={hasErrors ? "true" : undefined}
        aria-describedby={hasErrors ? `${id}-invalid` : undefined}
      />
      {hasErrors ? (
        <ErrorMessages id={`${id}-invalid`} errors={errors} />
      ) : null}
    </div>
  )
}
