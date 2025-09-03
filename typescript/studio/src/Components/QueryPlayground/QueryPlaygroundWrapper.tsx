"use client"

import { Tabs } from "@base-ui-components/react/tabs"
import { PlusIcon } from "@phosphor-icons/react"
import { createFormHook, useStore } from "@tanstack/react-form"
import { Schema } from "effect"

import { useOSQuery } from "../../hooks/useOSQuery"
import { fieldContext, formContext } from "../Form/form"

import { SubmitButton } from "../Form/SubmitButton"
import { DatasetQueryResultTable } from "./DatasetQueryResultTable"
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

  const form = useAppForm({
    defaultValues,
    validators: {
      onChange: Schema.standardSchemaV1(NozzleStudioQueryEditorForm),
    },
    async onSubmit({ value }) {
      const query = value.queries[value.activeTab]
      console.log(query.query)
    },
  })
  const activeTab = useStore(form.store, (state) => state.values.activeTab)

  return (
    <form
      noValidate
      className="w-full h-full flex flex-col border border-gray-300 dark:border-white/10 rounded-lg divide-y divide-gray-300 dark:divide-white/10"
      onSubmit={(e) => {
        e.preventDefault()
        e.stopPropagation()

        void form.handleSubmit()
      }}
    >
      <form.AppField name="queries" mode="array">
        {(queryField) => (
          <Tabs.Root
            className="w-full flex flex-col divide-y divide-gray-300 dark:divide-white/10"
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
                  className="inline-flex items-center justify-center px-4 h-8 text-gray-900 dark:text-white/80 border-b border-transparent data-[selected]:text-gray-950 data-[selected]:dark:text-white data-[selected]:border-purple-800 hover:text-gray-950 hover:dark:text-white hover:border-purple-400 cursor-pointer text-xs"
                >
                  {query.tab || ""}
                </Tabs.Tab>
              ))}
              <Tabs.Tab
                key="queries.tab.new"
                className="inline-flex items-center justify-center px-4 h-8 gap-x-2 text-gray-700 dark:text-white/80 cursor-pointer text-xs hover:text-gray-950 dark:hover:text-white border-b border-transparent mb-0 pb-0"
                onClick={() => {
                  // add a new tab to queryTabs array
                  queryField.pushValue({
                    query: "",
                    tab: "New...",
                  } as never)
                }}
              >
                <PlusIcon className="size-3" aria-hidden="true" />
                New
              </Tabs.Tab>
            </Tabs.List>
            {queryField.state.value.map((_, idx) => (
              <Tabs.Panel
                key={`queries[${idx}].editor_panel`}
                className="w-full h-full overflow-hidden bg-white dark:bg-slate-950 p-4"
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
        <span className="text-gray-500 dark:text-white/65 text-xs font-light">
          Enter to new line, {correctKey} + ENTER to run
        </span>
        <form.AppForm>
          <form.SubmitButton status={"idle"}>Run</form.SubmitButton>
        </form.AppForm>
      </div>
      {/* <DatasetQueryResultTable /> */}
    </form>
  )
}
