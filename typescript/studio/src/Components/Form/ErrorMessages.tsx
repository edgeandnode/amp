export function ErrorMessages({
  errors,
  id,
}: Readonly<{
  id: string | undefined
  errors: Array<string | { message: string }>
}>) {
  return (
    <div id={id} className="mt-2 flex flex-col gap-y-1 w-full">
      {errors.map((error, idx) => {
        const key = `${id}__errorMessage__${idx}`
        return (
          <div key={key} className="text-sm text-shadow-sonja-600 w-full">
            {typeof error === "string" ? error : error.message}
          </div>
        )
      })}
    </div>
  )
}
