"use client"

import { Menu } from "@base-ui-components/react/menu"
import { Tabs } from "@base-ui-components/react/tabs"
import { Toast } from "@base-ui-components/react/toast"
import {
  CaretDownIcon,
  CheckCircleIcon,
  CopySimpleIcon,
  PlusIcon,
  WarningIcon,
  XIcon,
} from "@graphprotocol/gds-react/icons"
import { createFormHook, useStore } from "@tanstack/react-form"
import { Schema, String as EffectString } from "effect"
import { useEffect, useMemo } from "react"

import { RESERVED_FIELDS } from "@/constants"
import { useDefaultQuery } from "@/hooks/useDefaultQuery"
import { useOSQuery } from "@/hooks/useOSQuery"
import { useDatasetsMutation } from "@/hooks/useQueryDatasetMutation"
import { classNames } from "@/utils/classnames"

import { ErrorMessages } from "../Form/ErrorMessages.tsx"
import { fieldContext, formContext } from "../Form/form.ts"
import { SubmitButtonGroup } from "../Form/SubmitButton.tsx"

import { Editor } from "./Editor.tsx"
import { NozzleConfigBrowser } from "./NozzleConfigBrowser.tsx"
import { SchemaBrowser } from "./SchemaBrowser.tsx"
import { SourcesBrowser } from "./SourcesBrowser.tsx"
import { UDFBrowser } from "./UDFBrowser.tsx"

export const { useAppForm } = createFormHook({
  fieldComponents: {
    Editor,
  },
  formComponents: {
    SubmitButtonGroup,
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

export function QueryPlaygroundWrapper() {
  const { data: os } = useOSQuery()
  const correctKey = os === "MacOS" ? "CMD" : "CTRL"

  const toastManager = Toast.useToastManager()

  const { data: defQuery } = useDefaultQuery()

  const { data, error, mutateAsync, status } = useDatasetsMutation({
    onError(error) {
      console.error("Failure performing dataset query", { error })
      toastManager.add({
        timeout: 5_000,
        title: "Query failure",
        description: "Failure performing this query. Please double-check your query and try again",
        type: "error",
      })
    },
  })

  const defaultValues: NozzleStudioQueryEditorForm = {
    activeTab: 0,
    queries: [{ query: defQuery.query, tab: defQuery.title }],
  }
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
  const activeTabQuery = useStore(form.store, (state) => state.values.queries[state.values.activeTab])

  const setQueryTabFromSelected = (tab: string, query: string) => {
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
  }

  // Memoize column extraction and formatting
  const tableData = useMemo(() => {
    if (!data || data.length === 0) return null

    const firstRecord = data[0]
    const columns = Object.keys(firstRecord)

    // Pre-format column headers once
    const formattedHeaders = columns.map((col) => ({
      key: col,
      display: col.split(".").pop()?.replace(/\[|\]/g, " ").replace(/_/g, " ").trim().toUpperCase() ||
        col.toUpperCase(),
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
    <div>
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
                                    form.setFieldValue("activeTab", Math.max(idx - 1, 0))
                                  }}
                                >
                                  <XIcon size={3} alt="" className="text-white" aria-hidden="true" />
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
                    <Tabs.Panel key={`queries[${idx}].editor_panel`} className="w-full h-full overflow-hidden p-4">
                      <form.AppField
                        name={`queries[${idx}].query` as const}
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
              <span className="text-12 text-space-700">Enter to new line, {correctKey} + ENTER to run</span>
              <div className="inline-flex rounded-4 shadow">
                <form.AppForm>
                  <form.SubmitButtonGroup status={"idle"}>Run</form.SubmitButtonGroup>
                </form.AppForm>
                <Menu.Root>
                  <div className="relative -ml-px block">
                    <Menu.Trigger className="relative inline-flex items-center rounded-r-4 bg-space-1400 p-2 text-white inset-ring-1 inset-ring-space-1200 hover:bg-space-1200 focus:z-10 active:bg-space-1200 data-[popup-open]:bg-space-1200 cursor-pointer">
                      <span className="sr-only">Open options</span>
                      <CaretDownIcon size={4} alt="" aria-hidden="true" />
                    </Menu.Trigger>
                    <Menu.Portal>
                      <Menu.Positioner className="outline-none" sideOffset={4} align="end">
                        <Menu.Popup className="origin-[var(--transform-origin)] rounded-4 bg-[canvas] py-1 text-space-300 shadow-2xl shadow-space-1500 outline outline-space-1400 transition-[transform,scale,opacity] data-[ending-style]:scale-90 data-[ending-style]:opacity-0 data-[starting-style]:scale-90 data-[starting-style]:opacity-0">
                          <Menu.Item
                            className="flex cursor-pointer items-center gap-x-1 py-2 pr-8 pl-4 text-12 leading-4 outline-none select-none data-[highlighted]:relative data-[highlighted]:z-0 data-[highlighted]:text-space-100 data-[highlighted]:before:absolute data-[highlighted]:before:inset-x-1 data-[highlighted]:before:inset-y-0 data-[highlighted]:before:z-[-1] data-[highlighted]:before:rounded-sm data-[highlighted]:before:bg-space-1400"
                            onClick={async () => {
                              if (!activeTabQuery?.query) {
                                return
                              }
                              await navigator.clipboard.writeText(activeTabQuery.query).then(() => {
                                toastManager.add({
                                  title: "SQL copied",
                                  description: "SQL command successfully copied to your clipboard",
                                  timeout: 5_000,
                                  type: "success",
                                })
                              })
                            }}
                          >
                            <CopySimpleIcon alt="Copy" size={4} aria-hidden="true" />
                            Copy SQL
                          </Menu.Item>
                        </Menu.Popup>
                      </Menu.Positioner>
                    </Menu.Portal>
                  </div>
                </Menu.Root>
              </div>
            </div>
            <div>
              <div data-query-result-status={status} className="w-full flex items-center gap-x-1.5 h-16 px-4">
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
                <div className="flex items-center justify-center py-8 text-space-500">No results found</div> :
                null}
              {error != null ?
                (
                  <div className="w-full px-4">
                    <ErrorMessages id="data" errors={[{ message: error.message }]} />
                  </div>
                ) :
                null}
            </div>
          </div>
        </div>
        <div className="h-full border-l border-space-1500 divide-y divide-space-1500 flex flex-col gap-y-4 overflow-y-auto">
          <NozzleConfigBrowser
            onTableSelected={(table, def) => {
              const query = def.input.sql.trim()
              const tab = `SELECT ... ${table}`
              setQueryTabFromSelected(tab, query)
            }}
          />
          <SchemaBrowser
            onEventSelected={(event) => {
              const query =
                `SELECT tx_hash, block_num, evm_decode_log(topic1, topic2, topic3, data, '${event.signature}') as event
FROM anvil.logs
WHERE topic0 = evm_topic('${event.signature}');`.trim()
              const tab = `SELECT ... ${event.name}`
              setQueryTabFromSelected(tab, query)
            }}
          />
          <SourcesBrowser
            onSourceSelected={(source) => {
              const columns = source.metadata_columns
                .map((col) => {
                  if (RESERVED_FIELDS.has(col.name)) {
                    return `"${col.name}"`
                  }
                  return col.name
                })
                .join(",\n  ")
              const query = `SELECT
  ${columns}
FROM ${source.source}
LIMIT 10;`.trim()
              const tab = `SELECT ... ${source.source}`
              setQueryTabFromSelected(tab, query)
            }}
          />
          <UDFBrowser />
        </div>
      </form>
      <Toast.Portal>
        <Toast.Viewport className="fixed bottom-[1rem] right-[1rem] top-auto z-10 mx-auto flex w-[350px] sm:bottom-[2rem] sm:right-[2rem] sm:w-[300px]">
          {toastManager.toasts.map((toast) => (
            <Toast.Root
              key={toast.id}
              toast={toast}
              className="rounded-6 border-space-1300 bg-space-1500 absolute bottom-0 left-auto right-0 z-[calc(1000-var(--toast-index))] mr-0 w-full select-none rounded-lg border bg-clip-padding p-4 shadow transition-all duration-500 ease-[cubic-bezier(0.22,1,0.36,1)] [transform:translateX(var(--toast-swipe-movement-x))_translateY(calc(var(--toast-swipe-movement-y)+calc(min(var(--toast-index),10)*-15px)))_scale(calc(max(0,1-(var(--toast-index)*0.1))))] [transition-property:opacity,transform] after:absolute after:bottom-full after:left-0 after:h-[calc(var(--gap)+1px)] after:w-full after:content-[''] data-[ending-style]:opacity-0 data-[limited]:opacity-0 data-[ending-style]:data-[swipe-direction=right]:[transform:translateX(calc(var(--toast-swipe-movement-x)+150%))_translateY(var(--offset-y))] data-[expanded]:data-[ending-style]:data-[swipe-direction=right]:[transform:translateX(calc(var(--toast-swipe-movement-x)+150%))_translateY(var(--offset-y))] data-[ending-style]:data-[swipe-direction=left]:[transform:translateX(calc(var(--toast-swipe-movement-x)-150%))_translateY(var(--offset-y))] data-[expanded]:data-[ending-style]:data-[swipe-direction=left]:[transform:translateX(calc(var(--toast-swipe-movement-x)-150%))_translateY(var(--offset-y))] data-[expanded]:[transform:translateX(var(--toast-swipe-movement-x))_translateY(calc(var(--toast-offset-y)*-1+calc(var(--toast-index)*var(--gap)*-1)+var(--toast-swipe-movement-y)))] data-[starting-style]:[transform:translateY(150%)] data-[ending-style]:data-[swipe-direction=down]:[transform:translateY(calc(var(--toast-swipe-movement-y)+150%))] data-[expanded]:data-[ending-style]:data-[swipe-direction=down]:[transform:translateY(calc(var(--toast-swipe-movement-y)+150%))] data-[ending-style]:data-[swipe-direction=up]:[transform:translateY(calc(var(--toast-swipe-movement-y)-150%))] data-[expanded]:data-[ending-style]:data-[swipe-direction=up]:[transform:translateY(calc(var(--toast-swipe-movement-y)-150%))] [&[data-ending-style]:not([data-limited]):not([data-swipe-direction])]:[transform:translateY(150%)]"
              style={{
                ["--gap" as string]: "1rem",
                ["--offset-y" as string]:
                  "calc(var(--toast-offset-y) * -1 + (var(--toast-index) * var(--gap) * -1) + var(--toast-swipe-movement-y))",
              }}
            >
              <div className="flex items-start">
                <div className="size-6">
                  {toast.type === "error" ?
                    (
                      <WarningIcon
                        alt=""
                        size={4}
                        variant="fill"
                        className="text-sonja-600"
                        aria-hidden="true"
                      />
                    ) :
                    (
                      <CheckCircleIcon
                        alt=""
                        size={4}
                        variant="fill"
                        className="text-starfield-600"
                        aria-hidden="true"
                      />
                    )}
                </div>
                <div className="ml-2 flex w-0 flex-1 flex-col gap-y-1.5">
                  <Toast.Title className="text-14 font-medium leading-5" />
                  <Toast.Description className="text-12 text-shadow-space-200" />
                </div>
                <div className="ml-3 flex shrink-0">
                  <Toast.Close
                    aria-label="Close"
                    className="text-shadow-space-300 flex size-5 cursor-pointer items-center justify-center rounded rounded-full border-none bg-transparent hover:bg-white/10 hover:text-white"
                  >
                    <XIcon size={4} className="size-4" alt="" aria-hidden="true" />
                  </Toast.Close>
                </div>
              </div>
            </Toast.Root>
          ))}
        </Toast.Viewport>
      </Toast.Portal>
    </div>
  )
}
