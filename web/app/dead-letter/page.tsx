"use client";
import { useQuery } from "@tanstack/react-query";
import { fetchDeadLetter, type DeadLetterEntry } from "@/lib/api";

export default function DeadLetterPage() {
  const { data, isLoading, error } = useQuery({
    queryKey: ["dead-letter"],
    queryFn: () => fetchDeadLetter(100),
    refetchInterval: 10000,
  });

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold">Dead Letter</h1>
        <span className="text-xs text-zinc-500">auto-refreshes every 10s</span>
      </div>

      {isLoading && <p className="text-zinc-500 text-sm">Loading…</p>}
      {error && <p className="text-red-400 text-sm">Failed to load dead letter</p>}

      {data && (
        <>
          <p className="text-sm text-zinc-500">{data.count} entries</p>
          <div className="rounded-lg border border-zinc-800 overflow-hidden">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-zinc-800 text-zinc-500 text-xs">
                  <th className="text-left px-4 py-2">Job ID</th>
                  <th className="text-left px-4 py-2">Tenant</th>
                  <th className="text-left px-4 py-2">Attempts</th>
                  <th className="text-left px-4 py-2">Moved At</th>
                  <th className="text-left px-4 py-2">Error</th>
                </tr>
              </thead>
              <tbody>
                {(data.entries ?? []).map((e: DeadLetterEntry) => (
                  <tr key={e.job_id} className="border-b border-zinc-900 hover:bg-zinc-900 transition-colors">
                    <td className="px-4 py-2 font-mono text-xs text-zinc-400">{e.job_id.slice(0, 8)}…</td>
                    <td className="px-4 py-2 font-mono text-xs text-zinc-500">{e.tenant_id.slice(0, 8)}…</td>
                    <td className="px-4 py-2 text-amber-400">{e.attempt_count}</td>
                    <td className="px-4 py-2 text-xs text-zinc-500">{new Date(e.moved_at).toLocaleString()}</td>
                    <td className="px-4 py-2 text-xs text-red-400 max-w-xs truncate">{e.final_error ?? "—"}</td>
                  </tr>
                ))}
                {data.entries?.length === 0 && (
                  <tr><td colSpan={5} className="px-4 py-6 text-center text-zinc-600">No dead-letter jobs</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}
