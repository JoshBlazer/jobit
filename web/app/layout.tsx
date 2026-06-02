import type { Metadata } from "next";
import "./globals.css";
import { Providers } from "./providers";
import { NavBar } from "./NavBar";
import { Inter } from "next/font/google";
import { cn } from "@/lib/utils";

const inter = Inter({ subsets: ["latin"], variable: "--font-sans" });

export const metadata: Metadata = {
  title: { template: "%s — Sluice", default: "Sluice" },
  description: "Job scheduler observability",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className={cn("font-sans", inter.variable)}>
      <body className="bg-zinc-950 text-zinc-100 min-h-screen font-sans antialiased">
        <Providers>
          <NavBar />
          <main className="p-6 max-w-7xl mx-auto">{children}</main>
        </Providers>
      </body>
    </html>
  );
}
