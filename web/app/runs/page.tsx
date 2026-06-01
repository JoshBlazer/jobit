"use client";
import { useQuery } from "@tanstack/react-query";
import { fetchRuns, type JobRun } from "@/lib/api";

const STATE_COLORS: Record<string, string> = {
  succeeded: "text-emerald-400",
  failed: "text-amber-400",
  dead: "text-red-400",
  claimed: "text-purple-400",
  running: "text-purple-400",
};

function fmt(ms: number | null) {
  if (ms === null) return "—";
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

export default function RunsPage() {
  const { data, isLoading, error } = useQuery({
    queryKey: ["runs"],
    queryFn: () => fetchRuns(100),
    refetchInterval: 5000,
  });

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold">Recent Runs</h1>
        <span className="text-xs text-zinc-500">auto-refreshes every 5s</span>
      </div>

      {isLoading && <p className="text-zinc-500 text-sm">Loading…</p>}
      {error && <p className="text-red-400 text-sm">Failed to load runs</p>}

      {data && (
        <div className="rounded-lg border border-zinc-800 overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-zinc-800 text-zinc-500 text-xs">
                <th className="text-left px-4 py-2">Job ID</th>
                <th className="text-left px-4 py-2">Type</th>
                <th className="text-left px-4 py-2">State</th>
                <th className="text-left px-4 py-2">Attempt</th>
                <th className="text-left px-4 py-2">Duration</th>
                <th className="text-left px-4 py-2">Started</th>
              </tr>
            </thead>
            <tbody>
              {(data.runs ?? []).map((r: JobRun) => (
                <tr key={r.run_id} className="border-b border-zinc-900 hover:bg-zinc-900 transition-colors">
                  <td className="px-4 py-2 font-mono text-xs text-zinc-400">{r.job_id.slice(0, 8)}…</td>
                  <td className="px-4 py-2 text-zinc-300">{r.type}</td>
                  <td className={`px-4 py-2 font-medium ${STATE_COLORS[r.state] ?? "text-zinc-300"}`}>{r.state}</td>
                  <td className="px-4 py-2 text-zinc-400">{r.attempt}</td>
                  <td className="px-4 py-2 font-mono text-xs text-zinc-400">{fmt(r.duration_ms)}</td>
                  <td className="px-4 py-2 text-xs text-zinc-500">{new Date(r.started_at).toLocaleTimeString()}</td>
                </tr>
              ))}
              {data.runs?.length === 0 && (
                <tr><td colSpan={6} className="px-4 py-6 text-center text-zinc-600">No runs yet</td></tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
