"use client"

import { Button, type ButtonProps } from "@graphprotocol/gds-react"
import { CheckIcon, ExclamationMarkIcon } from "@graphprotocol/gds-react/icons"

import { classNames } from "@/utils/classnames"

import { useOSQuery } from "../../hooks/useOSQuery"
import { useFormContext } from "./form"

export type SubmitButtonProps = ButtonProps.ButtonProps & {
  status: "idle" | "error" | "success" | "submitting"
}
export function SubmitButton({ children, status, ...rest }: SubmitButtonProps) {
  const form = useFormContext()

  const { data: os } = useOSQuery()
  const ctrlKey = os === "MacOS" ? "⌘" : "CTRL"

  return (
    <form.Subscribe
      selector={(state) => ({
        canSubmit: state.canSubmit,
        isSubmitting: state.isSubmitting,
        valid: state.isValid && state.errors.length === 0,
        dirty: state.isDirty,
      })}
    >
      {(state) => (
        <Button
          {...rest}
          type="submit"
          disabled={!state.canSubmit || !state.valid}
          data-state={status}
          variant={rest.variant || "secondary"}
          addonAfter={
            <span className="ml-4 rounded-4 border bg-space-1500 border-space-1300 text-white py-1 px-1.5 flex flex-col items-center justify-center">
              {ctrlKey}⏎
            </span>
          }
          className={classNames(
            "data-[state=error]:bg-red-600 data-[state=error]:hover:bg-red-500 data-[state=error]:focus-visible:bg-red-500 data-[state=success]:focus-visible:bg-green-500",
            "data-[state=success]:bg-green-600 data-[state=success]:hover:bg-green-500",
          )}
        >
          {status === "success" ?
            (
              <>
                <CheckIcon className="text-white" aria-hidden="true" size={5} alt="" />
                {children}
              </>
            ) :
            status === "error" ?
            (
              <>
                <ExclamationMarkIcon className="text-white" aria-hidden="true" size={5} alt="" />
                Error
              </>
            ) :
            children}
        </Button>
      )}
    </form.Subscribe>
  )
}
