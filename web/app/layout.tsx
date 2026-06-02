import type { Metadata } from "next";
import "./globals.css";
import { Providers } from "./providers";
import Link from "next/link";
import { Inter } from "next/font/google";
import { cn } from "@/lib/utils";

const inter = Inter({subsets:['latin'],variable:'--font-sans'});

export const metadata: Metadata = {
  title: "Sluice Dashboard",
  description: "Job scheduler observability",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className={cn("font-sans", inter.variable)}>
      <body className="bg-zinc-950 text-zinc-100 min-h-screen font-sans antialiased">
        <Providers>
          <nav className="border-b border-zinc-800 px-6 py-3 flex items-center gap-6 text-sm">
            <span className="font-semibold text-white mr-2 tracking-tight">⚡ Sluice</span>
            <Link href="/" className="text-zinc-400 hover:text-white transition-colors">Queue</Link>
            <Link href="/runs" className="text-zinc-400 hover:text-white transition-colors">Runs</Link>
            <Link href="/dead-letter" className="text-zinc-400 hover:text-white transition-colors">Dead Letter</Link>
          </nav>
          <main className="p-6 max-w-7xl mx-auto">{children}</main>
        </Providers>
      </body>
    </html>
  );
}
