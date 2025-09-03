"use client"

import { CheckIcon, ExclamationMarkIcon } from "@phosphor-icons/react"
import { type ComponentPropsWithRef, forwardRef } from "react"

import { classNames } from "@/utils/classnames"

import { useFormContext } from "./form"

export type SubmitButtonProps = ComponentPropsWithRef<"button"> & {
  status: "idle" | "error" | "success" | "submitting"
}
export const SubmitButton = forwardRef<HTMLButtonElement, SubmitButtonProps>(
  ({ status, children, ...rest }, ref) => {
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
        {({ canSubmit, isSubmitting, valid, dirty }) => (
          <button
            ref={ref}
            {...rest}
            type="submit"
            disabled={!canSubmit || !valid || !dirty || isSubmitting}
            data-state={status}
            className={classNames(
              "rounded-sm bg-white px-2 py-1 text-sm font-semibold text-gray-900 shadow-xs inset-ring inset-ring-gray-300 hover:bg-gray-50 dark:bg-white/10 dark:text-white dark:shadow-none dark:inset-ring-white/5 dark:hover:bg-white/20 cursor-pointer",
              "disabled:bg-gray-400 disabled:text-gray-900 disabled:hover:bg-gray-400 disabled:focus-visible:outline-gray-400 disabled:cursor-not-allowed",
              "data-[state=error]:bg-red-600 data-[state=error]:hover:bg-red-500 data-[state=error]:focus-visible:bg-red-500 data-[state=success]:focus-visible:bg-green-500",
              "data-[state=success]:bg-green-600 data-[state=success]:hover:bg-green-500",
            )}
          >
            {status === "success" ? (
              <>
                <CheckIcon className="size-5 text-white" aria-hidden="true" />
                {children}
              </>
            ) : status === "error" ? (
              <>
                <ExclamationMarkIcon
                  className="size-5 text-white"
                  aria-hidden="true"
                />
                Error
              </>
            ) : (
              children
            )}
          </button>
        )}
      </form.Subscribe>
    )
  },
)
