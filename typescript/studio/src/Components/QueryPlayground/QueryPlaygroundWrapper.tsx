"use client"

import { Tabs } from "@base-ui-components/react/tabs"
import { PlusIcon, XIcon } from "@graphprotocol/gds-react/icons"
import { createFormHook, useStore } from "@tanstack/react-form"
import { Schema, String as EffectString } from "effect"
import { useEffect, useMemo } from "react"

import { RESERVED_FIELDS } from "@/constants"
import { useDatasetsMutation } from "@/hooks/useDatasetMutation"
import { useOSQuery } from "@/hooks/useOSQuery"
import { classNames } from "@/utils/classnames"

import { fieldContext, formContext } from "../Form/form.ts"
import { SubmitButton } from "../Form/SubmitButton.tsx"

import { Editor } from "./Editor.tsx"
import { SchemaBrowser } from "./SchemaBrowser.tsx"
import { SourcesBrowser } from "./SourcesBrowser.tsx"
import { UDFBrowser } from "./UDFBrowser.tsx"

export const { useAppForm } = createFormHook({
  fieldComponents: {
    Editor,
  },
  formComponents: {
    SubmitButton,
  },
  fieldContext,
  formContext,
})

const NozzleStudioQueryEditorForm = Schema.Struct({
  activeTab: Schema.NonNegativeInt,
  queries: Schema.Array(
    Schema.Struct({
      query: Schema.String,
      tab: Schema.String,
    }),
  ),
})
type NozzleStudioQueryEditorForm = typeof NozzleStudioQueryEditorForm.Type

const defaultValues: NozzleStudioQueryEditorForm = {
  activeTab: 0,
  /** @todo figure out default */
  queries: [{ query: "", tab: "Dataset Query" }],
}

export function QueryPlaygroundWrapper() {
  const { data: os } = useOSQuery()
  const correctKey = os === "MacOS" ? "CMD" : "CTRL"

  const { data, mutateAsync, status } = useDatasetsMutation({
    onError(error) {
      console.error("Failure performing dataset query", { error })
    },
  })

  const form = useAppForm({
    defaultValues,
    validators: {
      onChange: Schema.standardSchemaV1(NozzleStudioQueryEditorForm),
    },
    async onSubmit({ value }) {
      const active = value.queries[value.activeTab]
      if (EffectString.isEmpty(active.query)) {
        return
      }
      await mutateAsync({
        query: EffectString.trim(active.query),
      })
    },
  })
  const activeTab = useStore(form.store, (state) => state.values.activeTab)

  // Memoize column extraction and formatting
  const tableData = useMemo(() => {
    if (!data || data.length === 0) return null

    const firstRecord = data[0]
    const columns = Object.keys(firstRecord)

    // Pre-format column headers once
    const formattedHeaders = columns.map((col) => ({
      key: col,
      display: col
        .split(".")
        .pop()
        ?.replace(/\[|\]/g, " ")
        .replace(/_/g, " ")
        .trim()
        .toUpperCase() || col.toUpperCase(),
    }))

    return { columns, formattedHeaders, rows: data }
  }, [data])

  // Add keyboard listener for CMD+ENTER / CTRL+ENTER when focus is outside editor
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      // Only handle if the target is not the Monaco editor
      const target = e.target as HTMLElement
      if (!target.closest(".monaco-editor")) {
        if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
          e.preventDefault()
          void form.handleSubmit()
        }
      }
    }

    window.addEventListener("keydown", handleKeyDown)
    return () => window.removeEventListener("keydown", handleKeyDown)
  }, [form])

  return (
    <form
      noValidate
      className="grid grid-cols-1 md:grid-cols-3 xl:grid-cols-4 h-full min-h-screen"
      onSubmit={(e) => {
        e.preventDefault()
        e.stopPropagation()

        void form.handleSubmit()
      }}
    >
      <div className="md:col-span-2 xl:col-span-3">
        <div className="w-full h-full flex flex-col border border-space-1500 rounded-lg divide-y divide-space-1400">
          <form.AppField name="queries" mode="array">
            {(queryField) => (
              <Tabs.Root
                className="w-full flex flex-col divide-y divide-space-1400"
                value={activeTab}
                onValueChange={(idx: number) => form.setFieldValue("activeTab", idx)}
              >
                <Tabs.List className="w-full flex items-baseline relative bg-transparent px-2 pt-2 pb-0">
                  {queryField.state.value.map((query, idx) => (
                    <Tabs.Tab
                      key={`queries[${idx}].tab`}
                      value={idx}
                      className="inline-flex items-center justify-center gap-x-1.5 px-4 h-8 text-14 bg-transparent text-white/65 border-b border-transparent data-[selected]:text-white data-[selected]:border-purple-500 hover:dark:text-white hover:border-purple-500 cursor-pointer"
                      nativeButton={false}
                      render={
                        <div>
                          {query.tab || ""}
                          {queryField.state.value.length > 1 ?
                            (
                              <button
                                type="button"
                                className="size-fit p-1.5 rounded-full inline-flex items-center justify-center bg-transparent hover:bg-transparent cursor-pointer"
                                onClick={() => {
                                  queryField.removeValue(idx)
                                  // set the active tab to curr - 1
                                  form.setFieldValue(
                                    "activeTab",
                                    Math.max(idx - 1, 0),
                                  )
                                }}
                              >
                                <XIcon
                                  size={3}
                                  alt=""
                                  className="text-white"
                                  aria-hidden="true"
                                />
                              </button>
                            ) :
                            null}
                        </div>
                      }
                    />
                  ))}
                  <Tabs.Tab
                    key="queries.tab.new"
                    className="inline-flex items-center justify-center px-4 h-8 gap-x-2 text-14 text-white/65 cursor-pointer text-xs hover:text-white border-b border-space-1500 mb-0 pb-0"
                    onClick={() => {
                      // add a new tab to queryTabs array
                      queryField.pushValue({
                        query: "",
                        tab: "New...",
                      } as never)
                    }}
                  >
                    <PlusIcon size={3} aria-hidden="true" alt="Add tab" />
                    New
                  </Tabs.Tab>
                </Tabs.List>
                {queryField.state.value.map((_, idx) => (
                  <Tabs.Panel
                    key={`queries[${idx}].editor_panel`}
                    className="w-full h-full overflow-hidden p-4"
                  >
                    <form.AppField
                      name={`queries[${idx}].query` as const}
                      listeners={{
                        onChangeDebounceMs: 300,
                        onChange({ value }) {
                          const generateQueryTitle = (query: string) => {
                            const trimmed = query.trim().toLowerCase()
                            if (!trimmed) return "New Query"

                            // Extract the main SQL operation
                            const operation = trimmed.split(/\s+/)[0]?.toUpperCase() || "QUERY"

                            // Try to extract table name from FROM clause
                            const fromMatch = trimmed.match(/from\s+([^\s,;]+)/i)
                            const tableName = fromMatch?.[1] || ""

                            if (tableName) {
                              return `${operation} ... ${tableName}`
                            }

                            // Fallback to just the operation if no table found
                            return operation
                          }

                          const title = generateQueryTitle(value)
                          form.setFieldValue("queries", (curr) => {
                            const updated = [...curr]
                            updated[activeTab] = {
                              ...updated[activeTab],
                              tab: title,
                            }
                            return updated
                          })
                        },
                      }}
                    >
                      {(field) => (
                        <field.Editor
                          id={`queries[${idx}].query` as const}
                          onSubmit={() => {
                            void form.handleSubmit()
                          }}
                        />
                      )}
                    </form.AppField>
                  </Tabs.Panel>
                ))}
              </Tabs.Root>
            )}
          </form.AppField>
          <div className="w-full flex items-center justify-between h-16 px-4">
            <span className="text-12 text-space-700">
              Enter to new line, {correctKey} + ENTER to run
            </span>
            <form.AppForm>
              <form.SubmitButton status={"idle"}>Run</form.SubmitButton>
            </form.AppForm>
          </div>
          <div>
            <div
              data-query-result-status={status}
              className="w-full flex items-center gap-x-1.5 h-16 px-4"
            >
              <span className="text-14 text-space-400">Result</span>
              <span
                data-query-result-status={status}
                className={classNames(
                  "group inline-flex items-center gap-x-1.5 rounded-4 px-1.5 py-0.5 text-14 font-medium inset-ring",
                  // idle || pending
                  "text-space-700 inset-ring-space-700",
                  // success
                  "data-[query-result-status=success]:text-shadow-starfield-700 data-[query-result-status=success]:inset-ring-starfield-700",
                  // error
                  "data-[query-result-status=error]:text-shadow-sonja-600 data-[query-result-status=error]:inset-ring-sonja-700",
                )}
              >
                <svg
                  viewBox="0 0 6 6"
                  aria-hidden="true"
                  className="size-1.5 fill-space-600 group-data-[query-result-status=error]:fill-sonja-700 group-data-[query-result-status=success]:fill-starfield-700"
                >
                  <circle r={3} cx={3} cy={3} />
                </svg>
                {status === "success"
                  ? "Success"
                  : status === "error"
                  ? "Failure"
                  : status === "pending"
                  ? "Querying..."
                  : "Waiting"}
              </span>
            </div>
            {tableData ?
              (
                <div className="flow-root">
                  <div className="overflow-x-auto">
                    <div className="inline-block min-w-full py-2 align-middle px-4">
                      <table className="relative min-w-full divide-y divide-white/10">
                        <thead>
                          <tr className="divide-x divide-white/10">
                            {tableData.formattedHeaders.map((header, colIdx) => (
                              <th
                                key={header.key}
                                scope="col"
                                className={classNames(
                                  "py-3.5 text-left text-space-500 text-10 font-medium",
                                  colIdx === 0 ? "pr-3 pl-4 sm:pl-0" : "px-3",
                                )}
                              >
                                {header.display}
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-white/10">
                          {tableData.rows.map((record, rowIdx) => (
                            <tr key={rowIdx} className="divide-x divide-white/10">
                              {tableData.columns.map((column, colIdx) => {
                                const value = record[column]

                                return (
                                  <td
                                    key={column}
                                    className={classNames(
                                      "py-4 whitespace-nowrap",
                                      colIdx === 0
                                        ? "pr-3 pl-4 text-16 sm:pl-0 text-white"
                                        : "px-3 text-14 text-space-500",
                                    )}
                                  >
                                    {value == null
                                      ? null
                                      : typeof value === "object"
                                      ? JSON.stringify(value)
                                      : String(value)}
                                  </td>
                                )
                              })}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </div>
              ) :
              data != null && data.length === 0 ?
              (
                <div className="flex items-center justify-center py-8 text-space-500">
                  No results found
                </div>
              ) :
              null}
          </div>
        </div>
      </div>
      <div className="h-full border-l border-space-1500 flex flex-col gap-y-4 overflow-y-auto">
        <SchemaBrowser
          onEventSelected={(event) => {
            const query =
              `SELECT tx_hash, block_num, evm_decode_log(topic1, topic2, topic3, data, '${event.signature}') as event
FROM anvil.logs
WHERE topic0 = evm_topic('${event.signature}');`.trim()
            const tab = `SELECT ... ${event.name}`
            // update the query at the active tab to query the selected event
            let setActiveTab = false
            form.setFieldValue("queries", (curr) => {
              const updatedQueries = [...curr]
              const active = curr[activeTab]
              if (EffectString.isEmpty(active.query)) {
                updatedQueries[activeTab] = { tab, query }
              } else {
                updatedQueries.push({ tab, query })
                // set new tab the active tab
                setActiveTab = true
              }

              return updatedQueries
            })
            if (setActiveTab) {
              form.setFieldValue("activeTab", (curr) => curr + 1)
            }
          }}
        />
        <SourcesBrowser
          onSourceSelected={(source) => {
            const columns = source.metadata_columns.map((col) => {
              if (RESERVED_FIELDS.has(col.name)) {
                return `"${col.name}"`
              }
              return col.name
            }).join(",\n  ")
            const query = `SELECT
  ${columns}
FROM ${source.source}
LIMIT 10;`.trim()
            const tab = `SELECT ... ${source.source}`
            // update the query with the selected table and columns
            let setActiveTab = false
            form.setFieldValue("queries", (curr) => {
              const updatedQueries = [...curr]
              const active = curr[activeTab]
              if (EffectString.isEmpty(active.query)) {
                updatedQueries[activeTab] = { tab, query }
              } else {
                updatedQueries.push({ tab, query })
                // set new tab the active tab
                setActiveTab = true
              }
              return updatedQueries
            })
            if (setActiveTab) {
              form.setFieldValue("activeTab", (curr) => curr + 1)
            }
          }}
        />
        <UDFBrowser />
      </div>
    </form>
  )
}
