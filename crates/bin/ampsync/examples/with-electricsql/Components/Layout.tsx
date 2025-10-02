import { AmpLogoIcon } from "@graphprotocol/gds-react/icons";

export function Layout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <div className="min-h-full">
      <div className="border-b border-space-1200">
        <div className="mx-auto container">
          <div className="flex h-16 justify-between">
            <div className="flex">
              <div className="flex shrink-0 items-center size-16">
                <AmpLogoIcon
                  alt="Amp"
                  size={10}
                  aria-hidden="true"
                  variant="branded"
                />
              </div>
              <h2 className="text-24 h-16 flex items-center">
                Ampsync | Electric SQL
              </h2>
            </div>
          </div>
        </div>
      </div>

      <div className="py-10">
        <main>
          <div className="mx-auto container">{children}</div>
        </main>
      </div>
    </div>
  );
}
