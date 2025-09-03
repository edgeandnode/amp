"use client"

import { Tabs } from "@base-ui-components/react/tabs"
import { PlusIcon } from "@phosphor-icons/react"
import { createFormHook } from "@tanstack/react-form"
import { Schema } from "effect"
import { useState } from "react"

import { useOSQuery } from "../../hooks/useOSQuery"
import { fieldContext, formContext } from "../Form/form"

import { DatasetQueryResultTable } from "./DatasetQueryResultTable"
import { Editor } from "./Editor"

export const { useAppForm } = createFormHook({
  fieldComponents: {
    Editor,
  },
  formComponents: {},
  fieldContext,
  formContext,
})

const NozzleStudioQueryEditorForm = Schema.Struct({
  editor: Schema.String,
})
type NozzleStudioQueryEditorForm = typeof NozzleStudioQueryEditorForm.Type

const defaultValues: NozzleStudioQueryEditorForm = {
  editor: "SELECT * FROM example.counts",
}

export function QueryPlaygroundWrapper() {
  const { data: os } = useOSQuery()

  const [queryTabs, setQueryTabs] = useState<Array<string>>(["Dataset Query"])
  const [activeTabIdx, setActiveTabIdx] = useState(0)

  const correctKey = os === "MacOS" ? "CMD" : "CTRL"

  const form = useAppForm({
    defaultValues,
    validators: {
      onChangeAsyncDebounceMs: 100,
      onChange: Schema.standardSchemaV1(NozzleStudioQueryEditorForm),
    },
  })

  return (
    <div className="w-full h-full flex flex-col border border-gray-300 dark:border-white/10 rounded-lg divide-y divide-gray-300 dark:divide-white/10">
      <Tabs.Root
        className="w-full flex flex-col divide-y divide-gray-300 dark:divide-white/10"
        defaultValue={queryTabs[0]}
        value={activeTabIdx}
        onValueChange={(idx: number) => setActiveTabIdx(idx)}
      >
        <Tabs.List className="w-full flex items-baseline relative bg-gray-100 dark:bg-slate-900 px-2 pt-2 pb-0">
          {queryTabs.map((tab, idx) => (
            <Tabs.Tab
              key={`queryTab[${tab}:${idx}]`}
              value={idx}
              className="inline-flex items-center justify-center px-4 h-8 text-gray-900 dark:text-white/80 border-b border-transparent data-[selected]:text-gray-950 data-[selected]:dark:text-white data-[selected]:border-purple-800 hover:text-gray-950 hover:dark:text-white hover:border-purple-400 cursor-pointer text-xs"
            >
              {tab}
            </Tabs.Tab>
          ))}
          <Tabs.Tab
            className="inline-flex items-center justify-center px-4 h-8 gap-x-2 text-gray-700 dark:text-white/80 cursor-pointer text-xs hover:text-gray-950 dark:hover:text-white border-b border-transparent mb-0 pb-0"
            onClick={() => {
              // add a new tab to queryTabs array, set as active
              const currentTabsLength = queryTabs.length
              setQueryTabs((curr) => [...curr, "New Query"])
              setActiveTabIdx(Math.max(currentTabsLength, 0))
            }}
          >
            <PlusIcon className="size-3" aria-hidden="true" />
            New
          </Tabs.Tab>
        </Tabs.List>
        {queryTabs.map((tab, idx) => (
          <Tabs.Panel
            key={`queryPanel[${tab}:${idx}]`}
            className="w-full h-full overflow-hidden bg-white dark:bg-slate-950 p-4"
          >
            <form.AppField name="editor">
              {(field) => <field.Editor id="editor" />}
            </form.AppField>
          </Tabs.Panel>
        ))}
      </Tabs.Root>
      <div className="w-full flex items-center justify-between h-16 px-4">
        <span className="text-gray-500 dark:text-white/65 text-xs font-light">
          Enter to new line, {correctKey} + ENTER to run
        </span>
        {/** @todo turn this into a form submit button */}
        <button
          type="button"
          className="rounded-sm bg-white px-2 py-1 text-sm font-semibold text-gray-900 shadow-xs inset-ring inset-ring-gray-300 hover:bg-gray-50 dark:bg-white/10 dark:text-white dark:shadow-none dark:inset-ring-white/5 dark:hover:bg-white/20 cursor-pointer"
        >
          Run
        </button>
      </div>
      <DatasetQueryResultTable />
    </div>
  )
}
