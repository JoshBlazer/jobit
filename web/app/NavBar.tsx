"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";

const LINKS = [
  { href: "/", label: "Queue" },
  { href: "/runs", label: "Runs" },
  { href: "/dead-letter", label: "Dead Letter" },
];

export function NavBar() {
  const pathname = usePathname();
  const isActive = (href: string) =>
    href === "/" ? pathname === "/" : pathname.startsWith(href);

  return (
    <nav className="border-b border-zinc-800 px-6 py-0 flex items-stretch gap-1 text-sm">
      <span className="font-semibold text-white mr-4 py-3 tracking-tight flex items-center">
        ⚡ Sluice
      </span>
      {LINKS.map(({ href, label }) => (
        <Link
          key={href}
          href={href}
          className={`py-3 px-3 border-b-2 transition-colors ${
            isActive(href)
              ? "border-blue-500 text-white"
              : "border-transparent text-zinc-400 hover:text-zinc-200"
          }`}
        >
          {label}
        </Link>
      ))}
    </nav>
  );
}
