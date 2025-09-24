"use client"

import { FunctionIcon } from "@graphprotocol/gds-react/icons"

import { USER_DEFINED_FUNCTIONS } from "@/constants"

/** @todo define way to add UDF to playground input */
export type UDFBrowserProps = {}
export function UDFBrowser() {
  return (
    <div className="flex flex-col gap-y-4 p-6">
      <div className="flex flex-col gap-y-1">
        <p className="text-14">User Defined Functions (UDFs)</p>
        <p className="text-12 text-space-700">
          Amp provided "built-in" SQL functions that can be called to manipulate the query data.
        </p>
      </div>
      <ul className="w-full box-border flex flex-col justify-center gap-y-3">
        {USER_DEFINED_FUNCTIONS.map((udf) => (
          <li key={udf.name} className="flex items-start relative w-full gap-x-0.5 px-0 py-2">
            <FunctionIcon className="text-galactic-500" aria-hidden="true" variant="regular" size={5} alt="" />
            <div className="w-full flex flex-col gap-y-1 items-center justify-start">
              <span className="self-start text-14">{udf.name}</span>
              <span className="text-12 text-space-700 self-start text-left whitespace-break-spaces">
                {udf.description}
              </span>
            </div>
          </li>
        ))}
      </ul>
    </div>
  )
}
