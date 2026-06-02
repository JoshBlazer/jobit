"use client";
import { useQuery } from "@tanstack/react-query";
import { fetchDeadLetter, replayJob, type DeadLetterEntry } from "@/lib/api";
import { useEffect, useState } from "react";

function relativeTime(dateStr: string): string {
  const seconds = Math.floor((Date.now() - new Date(dateStr).getTime()) / 1000);
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.floor(hours / 24)}d ago`;
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

function ExpandableError({ text }: { text: string | undefined }) {
  const [expanded, setExpanded] = useState(false);
  if (!text) return <span className="text-zinc-600">—</span>;
  return (
    <button
      className="text-left text-red-400 hover:text-red-300 text-xs transition-colors max-w-[220px]"
      onClick={() => setExpanded((v) => !v)}
      title={expanded ? "Click to collapse" : "Click to expand"}
    >
      {expanded ? text : `${text.slice(0, 50)}${text.length > 50 ? "…" : ""}`}
    </button>
  );
}

function Skeleton() {
  return (
    <div className="rounded-lg border border-zinc-800 overflow-hidden">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-zinc-800 text-zinc-500 text-xs">
            {["Job ID", "Tenant", "Attempts", "Moved", "Error", ""].map((h, i) => (
              <th key={i} className="text-left px-4 py-2">{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {Array.from({ length: 6 }).map((_, i) => (
            <tr key={i} className="border-b border-zinc-900">
              {Array.from({ length: 6 }).map((_, j) => (
                <td key={j} className="px-4 py-2.5">
                  <div className="animate-pulse bg-zinc-800 rounded h-3" style={{ width: j === 4 ? "120px" : "70px" }} />
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function DeadLetterPage() {
  useEffect(() => { document.title = "Dead Letter — Sluice"; }, []);

  const [replaying, setReplaying] = useState<string | null>(null);
  const [replayDone, setReplayDone] = useState<Set<string>>(new Set());
  const [replayErr, setReplayErr] = useState<string | null>(null);

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ["dead-letter"],
    queryFn: () => fetchDeadLetter(100),
    refetchInterval: 10000,
  });

  async function handleReplay(jobId: string) {
    setReplaying(jobId);
    setReplayErr(null);
    try {
      await replayJob(jobId);
      setReplayDone((s) => { const n = new Set(Array.from(s)); n.add(jobId); return n; });
      refetch();
    } catch {
      setReplayErr(jobId);
    } finally {
      setReplaying(null);
    }
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div>
          <h1 className="text-xl font-semibold">Dead Letter</h1>
          {data && <p className="text-zinc-500 text-sm mt-0.5">{data.count} entr{data.count !== 1 ? "ies" : "y"}</p>}
        </div>
        <button
          onClick={() => refetch()}
          className="text-xs text-zinc-400 hover:text-zinc-200 border border-zinc-700 rounded px-2 py-1.5 transition-colors"
        >
          ↺ Refresh
        </button>
      </div>

      {replayErr && (
        <div className="text-sm text-red-400 bg-red-950/30 border border-red-900/50 rounded-lg px-4 py-2">
          Replay failed for job {replayErr.slice(0, 8)}…
        </div>
      )}

      {isLoading && <Skeleton />}

      {error && (
        <div className="flex items-center gap-3 text-sm text-red-400 bg-red-950/30 border border-red-900/50 rounded-lg px-4 py-3">
          Failed to load dead letter queue
          <button onClick={() => refetch()} className="underline text-red-300 hover:text-red-100">Retry</button>
        </div>
      )}

      {data && (
        <div className="rounded-lg border border-zinc-800 overflow-hidden overflow-x-auto">
          <table className="w-full text-sm min-w-[700px]">
            <thead>
              <tr className="border-b border-zinc-800 bg-zinc-900/50 text-zinc-500 text-xs">
                <th className="text-left px-4 py-2.5">Job ID</th>
                <th className="text-left px-4 py-2.5">Tenant</th>
                <th className="text-left px-4 py-2.5">Attempts</th>
                <th className="text-left px-4 py-2.5">Moved</th>
                <th className="text-left px-4 py-2.5">Error</th>
                <th className="px-4 py-2.5" />
              </tr>
            </thead>
            <tbody>
              {(data.entries ?? []).map((e: DeadLetterEntry) => (
                <tr key={e.job_id} className={`border-b border-zinc-900 hover:bg-zinc-900/60 transition-colors ${replayDone.has(e.job_id) ? "opacity-40" : ""}`}>
                  <td className="px-4 py-2.5"><CopyId id={e.job_id} /></td>
                  <td className="px-4 py-2.5"><CopyId id={e.tenant_id} /></td>
                  <td className="px-4 py-2.5 text-amber-400 tabular-nums">{e.attempt_count}</td>
                  <td className="px-4 py-2.5 text-xs text-zinc-500" title={new Date(e.moved_at).toLocaleString()}>
                    {relativeTime(e.moved_at)}
                  </td>
                  <td className="px-4 py-2.5"><ExpandableError text={e.final_error} /></td>
                  <td className="px-4 py-2.5 text-right">
                    {replayDone.has(e.job_id) ? (
                      <span className="text-xs text-emerald-500">replayed</span>
                    ) : (
                      <button
                        onClick={() => handleReplay(e.job_id)}
                        disabled={replaying === e.job_id}
                        className="text-xs text-blue-400 hover:text-blue-200 border border-blue-900 hover:border-blue-700 rounded px-2 py-1 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                      >
                        {replaying === e.job_id ? "…" : "Replay"}
                      </button>
                    )}
                  </td>
                </tr>
              ))}
              {data.entries?.length === 0 && (
                <tr>
                  <td colSpan={6} className="px-4 py-8 text-center text-zinc-600">No dead-letter jobs</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
