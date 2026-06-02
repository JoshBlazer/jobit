"use client";
import { useQuery } from "@tanstack/react-query";
import { fetchRuns, type JobRun } from "@/lib/api";
import { useEffect, useState } from "react";

const STATE_COLORS: Record<string, string> = {
  succeeded: "text-emerald-400",
  failed: "text-amber-400",
  dead: "text-red-400",
  claimed: "text-purple-400",
  running: "text-violet-400",
};

function fmtDuration(ms: number | null): { text: string; color: string } {
  if (ms === null) return { text: "—", color: "text-zinc-500" };
  if (ms < 500) return { text: `${ms}ms`, color: "text-emerald-400" };
  if (ms < 5000) return { text: `${(ms / 1000).toFixed(1)}s`, color: "text-amber-400" };
  return { text: `${(ms / 1000).toFixed(1)}s`, color: "text-red-400" };
}

function CopyId({ id }: { id: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      className="font-mono text-xs text-zinc-400 hover:text-zinc-100 transition-colors cursor-pointer"
      title={`Copy: ${id}`}
      onClick={() => { navigator.clipboard.writeText(id); setCopied(true); setTimeout(() => setCopied(false), 1500); }}
    >
      {copied ? <span className="text-emerald-400">copied!</span> : `${id.slice(0, 8)}…`}
    </button>
  );
}

function Skeleton() {
  return (
    <div className="rounded-lg border border-zinc-800 overflow-hidden">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-zinc-800 text-zinc-500 text-xs">
            {["Job ID", "Type", "State", "Try", "Duration", "Started", "Error"].map((h) => (
              <th key={h} className="text-left px-4 py-2">{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {Array.from({ length: 8 }).map((_, i) => (
            <tr key={i} className="border-b border-zinc-900">
              {Array.from({ length: 7 }).map((_, j) => (
                <td key={j} className="px-4 py-2.5">
                  <div className="animate-pulse bg-zinc-800 rounded h-3" style={{ width: j === 0 ? "80px" : j === 5 ? "90px" : "60px" }} />
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function RunsPage() {
  useEffect(() => { document.title = "Runs — Sluice"; }, []);

  const [stateFilter, setStateFilter] = useState("");

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ["runs"],
    queryFn: () => fetchRuns(100),
    refetchInterval: 5000,
  });

  const runs = (data?.runs ?? []).filter((r: JobRun) => !stateFilter || r.state === stateFilter);

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between flex-wrap gap-3">
        <h1 className="text-xl font-semibold">Recent Runs</h1>
        <div className="flex items-center gap-3">
          <select
            value={stateFilter}
            onChange={(e) => setStateFilter(e.target.value)}
            className="text-xs bg-zinc-900 border border-zinc-700 rounded px-2 py-1.5 text-zinc-300 focus:outline-none focus:border-zinc-500"
          >
            <option value="">All states</option>
            <option value="succeeded">Succeeded</option>
            <option value="failed">Failed</option>
            <option value="dead">Dead</option>
            <option value="running">Running</option>
            <option value="claimed">Claimed</option>
          </select>
          <button
            onClick={() => refetch()}
            className="text-xs text-zinc-400 hover:text-zinc-200 border border-zinc-700 rounded px-2 py-1.5 transition-colors"
          >
            ↺ Refresh
          </button>
          <span className="text-xs text-zinc-500">auto-refreshes every 5s</span>
        </div>
      </div>

      {isLoading && <Skeleton />}

      {error && (
        <div className="flex items-center gap-3 text-sm text-red-400 bg-red-950/30 border border-red-900/50 rounded-lg px-4 py-3">
          Failed to load runs
          <button onClick={() => refetch()} className="underline text-red-300 hover:text-red-100">Retry</button>
        </div>
      )}

      {data && (
        <div className="rounded-lg border border-zinc-800 overflow-hidden overflow-x-auto">
          <table className="w-full text-sm min-w-[700px]">
            <thead>
              <tr className="border-b border-zinc-800 bg-zinc-900/50 text-zinc-500 text-xs">
                <th className="text-left px-4 py-2.5">Job ID</th>
                <th className="text-left px-4 py-2.5">Type</th>
                <th className="text-left px-4 py-2.5">State</th>
                <th className="text-left px-4 py-2.5">Try</th>
                <th className="text-left px-4 py-2.5">Duration</th>
                <th className="text-left px-4 py-2.5">Started</th>
                <th className="text-left px-4 py-2.5">Error</th>
              </tr>
            </thead>
            <tbody>
              {runs.map((r: JobRun) => {
                const dur = fmtDuration(r.duration_ms);
                const started = new Date(r.started_at);
                return (
                  <tr key={r.run_id} className="border-b border-zinc-900 hover:bg-zinc-900/60 transition-colors">
                    <td className="px-4 py-2.5"><CopyId id={r.job_id} /></td>
                    <td className="px-4 py-2.5 text-zinc-300">{r.type}</td>
                    <td className={`px-4 py-2.5 font-medium ${STATE_COLORS[r.state] ?? "text-zinc-300"}`}>{r.state}</td>
                    <td className="px-4 py-2.5 text-zinc-400 tabular-nums">{r.attempt + 1}</td>
                    <td className={`px-4 py-2.5 font-mono text-xs ${dur.color}`}>{dur.text}</td>
                    <td className="px-4 py-2.5 text-xs text-zinc-500 tabular-nums whitespace-nowrap">
                      {started.toLocaleDateString([], { month: "short", day: "numeric" })}{" "}
                      {started.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}
                    </td>
                    <td className="px-4 py-2.5 text-xs text-red-400 max-w-[200px] truncate" title={r.error ?? ""}>
                      {r.error ?? ""}
                    </td>
                  </tr>
                );
              })}
              {runs.length === 0 && (
                <tr>
                  <td colSpan={7} className="px-4 py-8 text-center text-zinc-600">
                    {stateFilter ? `No ${stateFilter} runs` : "No runs yet"}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}
      {data && (
        <p className="text-xs text-zinc-600 text-right">
          {runs.length}{stateFilter ? ` ${stateFilter}` : ""} run{runs.length !== 1 ? "s" : ""}{stateFilter && data.count > runs.length ? ` of ${data.count}` : ""}
        </p>
      )}
    </div>
  );
}
