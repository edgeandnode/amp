import "./globals.css";

import type { Metadata } from "next";

import { Layout } from "@/Components/Layout";

export const metadata: Metadata = {
  title: "Ampsync with Electric SQL",
  description:
    "Demo application showing how to build an app using amp, ampsync and electric sql to create fully reactive apps",
};

export const dynamic = "force-dynamic";

export default async function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html
      lang="en"
      className="m-0 h-full min-h-screen w-full overflow-y-auto overflow-x-hidden p-0"
    >
      <body className="h-full w-full">
        <Layout>{children}</Layout>
      </body>
    </html>
  );
}
