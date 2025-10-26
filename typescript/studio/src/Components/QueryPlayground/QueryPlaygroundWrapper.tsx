"use client"

import { Tabs } from "@base-ui-components/react/tabs"
import { Toast } from "@base-ui-components/react/toast"
import { Button } from "@graphprotocol/gds-react"
import {
  CheckCircleIcon,
  CopySimpleIcon,
  PlusIcon,
  SidebarRightIcon,
  WarningIcon,
  XIcon,
} from "@graphprotocol/gds-react/icons"
import { createFormHook, useStore } from "@tanstack/react-form"
import { Schema, String as EffectString } from "effect"
import { useEffect, useMemo, useState } from "react"

import { RESERVED_FIELDS } from "@/constants"
import { useDefaultQuery } from "@/hooks/useDefaultQuery"
import { useOSQuery } from "@/hooks/useOSQuery"
import { useDatasetsMutation } from "@/hooks/useQueryDatasetMutation"
import { classNames } from "@/utils/classnames"

import { ErrorMessages } from "../Form/ErrorMessages.tsx"
import { fieldContext, formContext } from "../Form/form.ts"
import { SubmitButton } from "../Form/SubmitButton.tsx"

import { AmpConfigBrowser } from "./AmpConfigBrowser.tsx"
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

const AmpStudioQueryEditorForm = Schema.Struct({
  activeTab: Schema.NonNegativeInt,
  queries: Schema.Array(
    Schema.Struct({
      query: Schema.String,
      tab: Schema.String,
    }),
  ),
})
type AmpStudioQueryEditorForm = typeof AmpStudioQueryEditorForm.Type

export function QueryPlaygroundWrapper() {
  const { data: os } = useOSQuery()
  const ctrlKey = os === "MacOS" ? "⌘" : "CTRL"

  const toastManager = Toast.useToastManager()

  const [navbarOpen, setNavbarOpen] = useState(true)

  const { data: defQuery } = useDefaultQuery()

  const { data, error, mutateAsync } = useDatasetsMutation({
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

  const defaultValues: AmpStudioQueryEditorForm = {
    activeTab: 0,
    queries: [{ query: defQuery.query, tab: defQuery.title }],
  }
  const form = useAppForm({
    defaultValues,
    validators: {
      onChange: Schema.standardSchemaV1(AmpStudioQueryEditorForm),
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
        className="h-full min-h-screen flex flex-col"
        onSubmit={(e) => {
          e.preventDefault()
          e.stopPropagation()

          void form.handleSubmit()
        }}
      >
        <form.AppField name="queries" mode="array">
          {(queryField) => (
            <Tabs.Root
              className="w-full flex flex-col h-full"
              value={activeTab}
              onValueChange={(idx: number) => form.setFieldValue("activeTab", idx)}
            >
              <Tabs.List className="w-full flex items-end relative bg-transparent px-2 h-16 border-b border-space-1500">
                {queryField.state.value.map((query, idx) => (
                  <Tabs.Tab
                    key={`queries[${idx}].tab`}
                    value={idx}
                    className="inline-flex items-center justify-center gap-x-1.5 px-4 h-full text-14 bg-transparent text-white/65 border-b-2 border-transparent data-selected:text-white data-selected:border-purple-500 hover:dark:text-white hover:border-purple-500 cursor-pointer -mb-px"
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
                  className="inline-flex items-center justify-center px-4 h-full gap-x-2 text-14 text-white/65 cursor-pointer text-xs hover:text-white border-b-2 border-transparent -mb-px"
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
                <div className="ml-auto pr-2 h-full flex flex-col items-center justify-center">
                  <button
                    type="button"
                    className="rounded-full p-2 hover:bg-space-1500 cursor-pointer inline-flex items-center justify-center shadow"
                    onClick={() => setNavbarOpen((curr) => !curr)}
                  >
                    <SidebarRightIcon size={5} alt="" aria-hidden="true" />
                  </button>
                </div>
              </Tabs.List>
              <div
                className={classNames(
                  "grid flex-1",
                  navbarOpen ? "grid-cols-1 md:grid-cols-3 xl:grid-cols-4" : "grid-cols-1",
                )}
              >
                <div className={classNames(navbarOpen ? "md:col-span-2 xl:col-span-3" : "col-span-1", "flex flex-col")}>
                  <div className="w-full shrink-0 flex flex-col rounded-lg">
                    {queryField.state.value.map((_, idx) => (
                      <Tabs.Panel
                        key={`queries[${idx}].editor_panel`}
                        className="w-full h-[400px] overflow-visible p-4"
                      >
                        <form.AppField name={`queries[${idx}].query` as const}>
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
                    <div className="w-full flex items-center justify-between h-16 px-4 border-b border-space-1500">
                      <div className="text-12 text-space-700 flex items-center gap-x-1 w-fit">
                        <span className="rounded-4 border border-space-1300 text-white py-1 px-1.5 flex flex-col items-center justify-center">
                          ⏎
                        </span>
                        <span className="text-12 text-space-700 w-fit">to new line</span>
                        <span className="ml-4 rounded-4 border border-space-1300 text-white py-1 px-1.5 flex flex-col items-center justify-center">
                          {ctrlKey}⏎
                        </span>
                        <span className="text-12 text-space-700">to run</span>
                      </div>
                      <div className="flex items-center gap-x-1 rounded-4 shadow ml-auto w-fit">
                        <Button
                          type="button"
                          variant="tertiary"
                          addonAfter={CopySimpleIcon}
                          className="w-fit"
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
                          Copy SQL
                        </Button>
                        <form.AppForm>
                          <form.SubmitButton status={"idle"}>Run</form.SubmitButton>
                        </form.AppForm>
                      </div>
                    </div>
                  </div>
                  <div className="flex-1 overflow-auto">
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
                <div
                  className={classNames(
                    "border-l border-space-1500 divide-y divide-space-1500 flex flex-col gap-y-4 h-full",
                    "transition-[opacity,transform] duration-300 ease-in-out",
                    navbarOpen
                      ? "opacity-100 translate-x-0"
                      : "opacity-0 translate-x-full md:translate-x-8 xl:translate-x-8 pointer-events-none",
                  )}
                  style={{
                    overflow: navbarOpen ? "auto" : "hidden",
                  }}
                >
                  <AmpConfigBrowser
                    onTableSelected={(dataset, table) => {
                      const query = `SELECT * FROM "${dataset}"."${table}"`
                      const tab = `SELECT ... ${dataset}.${table}`
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
              </div>
            </Tabs.Root>
          )}
        </form.AppField>
      </form>
      <Toast.Portal>
        <Toast.Viewport className="fixed bottom-4 right-4 top-auto z-10 mx-auto flex w-[350px] sm:bottom-8 sm:right-8 sm:w-[300px]">
          {toastManager.toasts.map((toast) => (
            <Toast.Root
              key={toast.id}
              toast={toast}
              className="rounded-6 border-space-1300 bg-space-1500 absolute bottom-0 left-auto right-0 z-[calc(1000-var(--toast-index))] mr-0 w-full select-none rounded-lg border bg-clip-padding p-4 shadow transition-all duration-500 ease-[cubic-bezier(0.22,1,0.36,1)] transform-[translateX(var(--toast-swipe-movement-x))_translateY(calc(var(--toast-swipe-movement-y)+calc(min(var(--toast-index),10)*-15px)))_scale(calc(max(0,1-(var(--toast-index)*0.1))))] [transition-property:opacity,transform] after:absolute after:bottom-full after:left-0 after:h-[calc(var(--gap)+1px)] after:w-full after:content-[''] data-ending-style:opacity-0 data-limited:opacity-0 data-ending-style:data-[swipe-direction=right]:transform-[translateX(calc(var(--toast-swipe-movement-x)+150%))_translateY(var(--offset-y))] data-expanded:data-ending-style:data-[swipe-direction=right]:transform-[translateX(calc(var(--toast-swipe-movement-x)+150%))_translateY(var(--offset-y))] data-ending-style:data-[swipe-direction=left]:transform-[translateX(calc(var(--toast-swipe-movement-x)-150%))_translateY(var(--offset-y))] data-expanded:data-ending-style:data-[swipe-direction=left]:transform-[translateX(calc(var(--toast-swipe-movement-x)-150%))_translateY(var(--offset-y))] data-expanded:transform-[translateX(var(--toast-swipe-movement-x))_translateY(calc(var(--toast-offset-y)*-1+calc(var(--toast-index)*var(--gap)*-1)+var(--toast-swipe-movement-y)))] data-starting-style:transform-[translateY(150%)] data-ending-style:data-[swipe-direction=down]:transform-[translateY(calc(var(--toast-swipe-movement-y)+150%))] data-expanded:data-ending-style:data-[swipe-direction=down]:transform-[translateY(calc(var(--toast-swipe-movement-y)+150%))] data-ending-style:data-[swipe-direction=up]:transform-[translateY(calc(var(--toast-swipe-movement-y)-150%))] data-expanded:data-ending-style:data-[swipe-direction=up]:transform-[translateY(calc(var(--toast-swipe-movement-y)-150%))] [&[data-ending-style]:not([data-limited]):not([data-swipe-direction])]:transform-[translateY(150%)]"
              style={{
                ["--gap" as string]: "1rem",
                ["--offset-y" as string]:
                  "calc(var(--toast-offset-y) * -1 + (var(--toast-index) * var(--gap) * -1) + var(--toast-swipe-movement-y))",
              }}
            >
              <div className="flex items-start">
                <div className="size-6">
                  {toast.type === "error" ?
                    <WarningIcon alt="" size={4} variant="fill" className="text-sonja-600" aria-hidden="true" /> :
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
