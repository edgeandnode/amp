"use client"

import type { ButtonProps } from "@graphprotocol/gds-react"
import { Button } from "@graphprotocol/gds-react"
import { CheckIcon, ExclamationMarkIcon } from "@graphprotocol/gds-react/icons"
import { forwardRef } from "react"

import { classNames } from "@/utils/classnames"

import { useFormContext } from "./form"

export type SubmitButtonProps = ButtonProps & {
  status: "idle" | "error" | "success" | "submitting"
}
export const SubmitButton = forwardRef<HTMLButtonElement, SubmitButtonProps>(({ children, status, ...rest }, ref) => {
  const form = useFormContext()

  return (
    <form.Subscribe
      selector={(state) => ({
        canSubmit: state.canSubmit,
        isSubmitting: state.isSubmitting,
        valid: state.isValid && state.errors.length === 0,
        dirty: state.isDirty,
      })}
    >
      {({ canSubmit, valid }) => (
        <Button
          ref={ref}
          {...rest}
          type="submit"
          disabled={!canSubmit || !valid}
          data-state={status}
          className={classNames(
            "rounded-6 px-4 py-2.5 text-12 shadow-xs inset-ring inset-ring-space-1200 bg-space-1400 text-white cursor-pointer inline-flex items-center justify-center gap-x-1.5",
            "disabled:bg-space-1500 disabled:text-white/80 disabled:hover:bg-space-1500 disabled:focus-visible:outline-space-1200 disabled:cursor-not-allowed",
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
})
