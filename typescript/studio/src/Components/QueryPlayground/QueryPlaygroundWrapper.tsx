"use client"

import { Toast } from "@base-ui-components/react/toast"
import { Button, ButtonGroup, CopyButton, Keyboard, TabSet, ToggleButton } from "@graphprotocol/gds-react"
import {
  CheckCircleIcon,
  PlusIcon,
  SidebarRightInteractiveIcon,
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
      display: col.split(".").pop()?.replace(/\[|\]/g, " ").replace(/_/g, " ").trim(),
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
    <>
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
            <TabSet
              className="flex size-full min-h-screen flex-col"
              divider={false}
              value={activeTab}
              onChange={(idx: number) => form.setFieldValue("activeTab", idx)}
            >
              <div className="border-border-muted bg-bg-subtle flex h-12 items-center justify-between border-b pr-4 pl-1">
                <TabSet.Tabs className="-mb-px h-full">
                  {queryField.state.value.map((query, idx) => (
                    <div key={`queries[${idx}].tab`}>
                      <TabSet.Tab className="pe-6" value={idx}>
                        {query.tab || ""}
                      </TabSet.Tab>
                      {queryField.state.value.length > 1 ?
                        (
                          <Button
                            variant="naked"
                            className="absolute inset-y-0 end-0 my-auto"
                            onClick={() => {
                              queryField.removeValue(idx)
                              // set the active tab to curr - 1
                              form.setFieldValue("activeTab", Math.max(idx - 1, 0))
                            }}
                          >
                            <XIcon size={3} alt="Close tab" />
                          </Button>
                        ) :
                        null}
                    </div>
                  ))}
                  <TabSet.Tab
                    key="queries.tab.new"
                    addonBefore={<PlusIcon size={3} alt="Add tab" />}
                    onClick={() => {
                      // add a new tab to queryTabs array
                      queryField.pushValue({
                        query: "",
                        tab: "New...",
                      } as never)
                    }}
                  >
                    New
                  </TabSet.Tab>
                </TabSet.Tabs>
                <ToggleButton
                  checked={navbarOpen}
                  variant="naked"
                  size="large"
                  onClick={() => setNavbarOpen((curr) => !curr)}
                >
                  <SidebarRightInteractiveIcon alt="Toggle" />
                </ToggleButton>
              </div>
              <div
                className={classNames(
                  "grid flex-1",
                  navbarOpen ? "grid-cols-1 md:grid-cols-5 2xl:grid-cols-4" : "grid-cols-1",
                )}
              >
                <div
                  className={classNames("flex flex-col", navbarOpen ? "md:col-span-3 2xl:col-span-3" : "col-span-1")}
                >
                  <TabSet.Panels className="h-[400px] w-full overflow-visible">
                    {queryField.state.value.map((_, idx) => (
                      <TabSet.Panel key={`queries[${idx}].editor_panel`} className="w-full overflow-visible p-4">
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
                      </TabSet.Panel>
                    ))}
                  </TabSet.Panels>
                  <div className="border-border-muted flex w-full flex-col gap-y-3 border-y px-4 py-3 sm:flex-row sm:items-center sm:justify-between sm:gap-y-0">
                    <div className="text-12 text-fg-elevated hidden items-center gap-2 sm:flex">
                      <Keyboard className="mr-1">⏎</Keyboard>
                      <span>to new line</span>
                      <Keyboard className="mr-1">{ctrlKey}⏎</Keyboard>
                      <span>to run</span>
                    </div>
                    <ButtonGroup className="prop-full-width-true prop-variant-tertiary sm:prop-full-width-false">
                      <CopyButton
                        variant="tertiary"
                        onClick={async () => {
                          if (!activeTabQuery?.query) {
                            return
                          }
                          await navigator.clipboard.writeText(activeTabQuery.query)
                        }}
                      >
                        Copy SQL
                      </CopyButton>
                      <form.AppForm>
                        <form.SubmitButton status="idle">Run</form.SubmitButton>
                      </form.AppForm>
                    </ButtonGroup>
                  </div>

                  <div className="flex-1 overflow-auto">
                    {tableData ?
                      (
                        <table className="m-4 w-full">
                          <thead className="border-border-muted border-b">
                            <tr className="divide-border-muted divide-x">
                              {tableData.formattedHeaders.map((header, colIdx) => (
                                <th
                                  key={header.key}
                                  scope="col"
                                  className={classNames(
                                    "text-fg-muted text-12 py-2 font-mono capitalize",
                                    colIdx === 0 ? "pr-3 pl-4 sm:pl-0" : "px-3",
                                  )}
                                >
                                  {header.display}
                                </th>
                              ))}
                            </tr>
                          </thead>
                          <tbody className="divide-border-muted divide-y">
                            {tableData.rows.map((record, rowIdx) => (
                              <tr key={rowIdx} className="divide-border-muted divide-x">
                                {tableData.columns.map((column, colIdx) => {
                                  const value = record[column]

                                  return (
                                    <td
                                      key={column}
                                      className={classNames(
                                        "py-4 whitespace-nowrap",
                                        colIdx === 0
                                          ? "text-16 text-fg-default pr-3 pl-4 sm:pl-0"
                                          : "text-14 text-fg-elevated px-3",
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
                      ) :
                      data != null && data.length === 0 ?
                      <div className="flex items-center justify-center py-8 text-fg-muted">No results found</div> :
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
                    "border-l border-border-muted divide-y divide-border-muted bg-bg-subtle flex flex-col gap-y-4 h-full md:col-span-2 2xl:col-span-1",
                    "transition-[opacity,transform] duration-300 ease-in-out",
                    navbarOpen
                      ? "opacity-100 translate-x-0"
                      : "opacity-0 translate-x-full pointer-events-none hidden md:block",
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
            </TabSet>
          )}
        </form.AppField>
      </form>
      <Toast.Portal>
        <Toast.Viewport className="fixed bottom-4 right-4 top-auto z-10 mx-auto flex w-80 sm:bottom-8 sm:right-8 sm:w-72">
          {toastManager.toasts.map((toast) => (
            <Toast.Root
              key={toast.id}
              toast={toast}
              className="rounded-6 border-border-muted bg-bg-muted absolute bottom-0 left-auto right-0 z-[calc(1000-var(--toast-index))] mr-0 w-full select-none rounded-lg border bg-clip-padding p-4 shadow transition-all duration-500 ease-[cubic-bezier(0.22,1,0.36,1)] transform-[translateX(var(--toast-swipe-movement-x))_translateY(calc(var(--toast-swipe-movement-y)+calc(min(var(--toast-index),10)*-15px)))_scale(calc(max(0,1-(var(--toast-index)*0.1))))] [transition-property:opacity,transform] after:absolute after:bottom-full after:left-0 after:h-[calc(var(--gap)+1px)] after:w-full after:content-[''] data-ending-style:opacity-0 data-limited:opacity-0 data-ending-style:data-[swipe-direction=right]:transform-[translateX(calc(var(--toast-swipe-movement-x)+150%))_translateY(var(--offset-y))] data-expanded:data-ending-style:data-[swipe-direction=right]:transform-[translateX(calc(var(--toast-swipe-movement-x)+150%))_translateY(var(--offset-y))] data-ending-style:data-[swipe-direction=left]:transform-[translateX(calc(var(--toast-swipe-movement-x)-150%))_translateY(var(--offset-y))] data-expanded:data-ending-style:data-[swipe-direction=left]:transform-[translateX(calc(var(--toast-swipe-movement-x)-150%))_translateY(var(--offset-y))] data-expanded:transform-[translateX(var(--toast-swipe-movement-x))_translateY(calc(var(--toast-offset-y)*-1+calc(var(--toast-index)*var(--gap)*-1)+var(--toast-swipe-movement-y)))] data-starting-style:transform-[translateY(150%)] data-ending-style:data-[swipe-direction=down]:transform-[translateY(calc(var(--toast-swipe-movement-y)+150%))] data-expanded:data-ending-style:data-[swipe-direction=down]:transform-[translateY(calc(var(--toast-swipe-movement-y)+150%))] data-ending-style:data-[swipe-direction=up]:transform-[translateY(calc(var(--toast-swipe-movement-y)-150%))] data-expanded:data-ending-style:data-[swipe-direction=up]:transform-[translateY(calc(var(--toast-swipe-movement-y)-150%))] [&[data-ending-style]:not([data-limited]):not([data-swipe-direction])]:transform-[translateY(150%)]"
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
                  <Toast.Title className="text-14" />
                  <Toast.Description className="text-12 text-fg-muted" />
                </div>
                <div className="ml-3 flex shrink-0">
                  <Toast.Close
                    aria-label="Close"
                    className="text-fg-elevated flex p-1 items-center justify-center rounded-6 bg-transparent hover:bg-bg-muted hover:text-fg-default"
                  >
                    <XIcon size={4} className="size-4" alt="" aria-hidden="true" />
                  </Toast.Close>
                </div>
              </div>
            </Toast.Root>
          ))}
        </Toast.Viewport>
      </Toast.Portal>
    </>
  )
}
