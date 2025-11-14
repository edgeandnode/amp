"use client"

import { Button, type ButtonProps, Keyboard } from "@graphprotocol/gds-react"
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
          addonAfter={<Keyboard>{ctrlKey}⏎</Keyboard>}
          className={classNames(
            "data-[state=error]:bg-status-error-default data-[state=error]:hover:bg-status-error-elevated",
            "data-[state=success]:bg-status-success-default data-[state=success]:hover:bg-status-success-elevated",
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
