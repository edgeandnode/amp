"use client"

import { Tabs } from "@base-ui-components/react/tabs"
import { PlusIcon } from "@graphprotocol/gds-react/icons"
import { createFormHook, useStore } from "@tanstack/react-form"
import { Schema } from "effect"
import { useMemo } from "react"

import { useDatasetsMutation } from "../../hooks/useDatasetMutation"
import { useOSQuery } from "../../hooks/useOSQuery"
import { classNames } from "../../utils/classnames"
import { fieldContext, formContext } from "../Form/form"
import { SubmitButton } from "../Form/SubmitButton"
import { Editor } from "./Editor"

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
  queries: [{ query: "SELECT * FROM example.counts", tab: "Dataset Query" }],
}

export function QueryPlaygroundWrapper() {
  const { data: os } = useOSQuery()
  const correctKey = os === "MacOS" ? "CMD" : "CTRL"

  const { mutateAsync, data, status } = useDatasetsMutation({
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
      const query = value.queries[value.activeTab]
      console.log(query.query)
      await mutateAsync({
        query:
          "SELECT event['to'], event['from'], event['value'] from mainnet_graph_token.transfer LIMIT 10",
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
      display:
        col
          .split(".")
          .pop()
          ?.replace(/\[|\]/g, " ")
          .replace(/_/g, " ")
          .trim()
          .toUpperCase() || col.toUpperCase(),
    }))

    return { columns, formattedHeaders, rows: data }
  }, [data])

  return (
    <form
      noValidate
      className="w-full h-full flex flex-col border border-space-1500 rounded-lg divide-y divide-space-1400"
      onSubmit={(e) => {
        e.preventDefault()
        e.stopPropagation()

        void form.handleSubmit()
      }}
    >
      <form.AppField name="queries" mode="array">
        {(queryField) => (
          <Tabs.Root
            className="w-full flex flex-col divide-y divide-space-1400"
            value={activeTab}
            onValueChange={(idx: number) =>
              form.setFieldValue("activeTab", idx)
            }
          >
            <Tabs.List className="w-full flex items-baseline relative bg-gray-100 dark:bg-slate-900 px-2 pt-2 pb-0">
              {queryField.state.value.map((query, idx) => (
                <Tabs.Tab
                  key={`queries[${idx}].tab`}
                  value={idx}
                  className="inline-flex items-center justify-center px-4 h-8 text-14 bg-space-1500 text-white/65 border-b border-transparent data-[selected]:text-white data-[selected]:border-purple-500 hover:dark:text-white hover:border-purple-500 cursor-pointer"
                >
                  {query.tab || ""}
                </Tabs.Tab>
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
                    onChange() {
                      // set the query tab title to the query
                    },
                  }}
                >
                  {(field) => (
                    <field.Editor
                      id={`queries[${idx}].query` as const}
                      name={`queries[${idx}].query` as const}
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
        {tableData ? (
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
        ) : data != null && data.length === 0 ? (
          <div className="flex items-center justify-center py-8 text-space-500">
            No results found
          </div>
        ) : null}
      </div>
    </form>
  )
}
