import { FunctionIcon } from "@graphprotocol/gds-react/icons"

import { USER_DEFINED_FUNCTIONS } from "@/constants"

/** @todo define way to add UDF to playground input */
export type UDFBrowserProps = {}
export function UDFBrowser() {
  return (
    <div className="flex flex-col gap-4 px-2 py-6">
      <div className="flex flex-col gap-1 px-4">
        <p className="text-14">User Defined Functions (UDFs)</p>
        <p className="text-12 text-fg-muted">
          Amp provided "built-in" SQL functions that can be called to manipulate the query data.
        </p>
      </div>
      <ul className="gap-3 px-4">
        {USER_DEFINED_FUNCTIONS.map((udf) => (
          <li key={udf.name} className="flex w-full gap-1 px-0 py-2">
            <FunctionIcon className="text-galactic-500" aria-hidden="true" size={5} alt="" />
            <div className="flex flex-col gap-1">
              <span className="text-14">{udf.name}</span>
              <span className="text-12 text-fg-muted">{udf.description}</span>
            </div>
          </li>
        ))}
      </ul>
    </div>
  )
}
