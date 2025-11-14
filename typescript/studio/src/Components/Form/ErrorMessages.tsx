export function ErrorMessages({
  errors,
  id,
}: Readonly<{
  id: string | undefined
  errors: Array<string | { message: string }>
}>) {
  return (
    <div id={id} className="mt-2 flex flex-col gap-1">
      {errors.map((error, idx) => {
        const key = `${id}__errorMessage__${idx}`
        return (
          <div key={key} className="text-14 text-status-error-default">
            {typeof error === "string" ? error : error.message}
          </div>
        )
      })}
    </div>
  )
}
