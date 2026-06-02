"use client";
import { useLiveStats } from "@/lib/ws";
import {
  AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend,
} from "recharts";
import { useEffect, useRef, useState } from "react";

type Snapshot = { t: string; high: number; normal: number; low: number };

const PRIORITY_COLORS: Record<string, string> = {
  high: "#f97316",
  normal: "#3b82f6",
  low: "#6b7280",
};

const STATE_COLORS: Record<string, string> = {
  succeeded: "text-emerald-400",
  failed: "text-amber-400",
  dead: "text-red-400",
  pending: "text-blue-400",
  claimed: "text-purple-400",
  running: "text-violet-400",
  scheduled: "text-sky-400",
};

const ACTIVE_STATES = ["pending", "scheduled", "claimed", "running"];
const TERMINAL_STATES = ["succeeded", "failed", "dead"];

export default function QueuePage() {
  useEffect(() => { document.title = "Queue — Sluice"; }, []);

  const { data, connected } = useLiveStats();
  const [history, setHistory] = useState<Snapshot[]>([]);
  const lastTs = useRef("");

  if (data && String(data.timestamp) !== lastTs.current) {
    lastTs.current = String(data.timestamp);
    const totals = { high: 0, normal: 0, low: 0 };
    (data.queues ?? []).forEach((q) => {
      if (q.priority in totals) totals[q.priority as keyof typeof totals] += q.depth;
    });
    setHistory((h) => [...h.slice(-59), { t: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }), ...totals }]);
  }

  const current = history[history.length - 1];
  const prev = history.length >= 10 ? history[history.length - 10] : null;
  const totalDepth = current ? current.high + current.normal + current.low : 0;

  const delta = (key: keyof Snapshot): number | null => {
    if (!current || !prev) return null;
    return (current[key] as number) - (prev[key] as number);
  };

  const jobs_by_state = data?.jobs_by_state ?? {};

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold">Queue Depth</h1>
          <p className="text-zinc-500 text-sm mt-0.5">{totalDepth} job{totalDepth !== 1 ? "s" : ""} waiting</p>
        </div>
        <span className={`text-xs px-2.5 py-1.5 rounded-full border flex items-center gap-1.5 ${connected ? "border-emerald-700 text-emerald-400" : "border-zinc-700 text-zinc-500"}`}>
          <span className={`w-1.5 h-1.5 rounded-full ${connected ? "bg-emerald-400 animate-pulse" : "bg-zinc-500"}`} />
          {connected ? "live" : "reconnecting…"}
        </span>
      </div>

      <div className="grid grid-cols-3 gap-4">
        {(["high", "normal", "low"] as const).map((p) => {
          const count = current?.[p] ?? 0;
          const d = delta(p);
          return (
            <div key={p} className="rounded-lg border border-zinc-800 bg-zinc-900 p-4">
              <p className="text-xs text-zinc-500 mb-1 capitalize">{p} priority</p>
              <div className="flex items-end gap-2">
                <p className="text-3xl font-bold" style={{ color: PRIORITY_COLORS[p] }}>{count}</p>
                {d !== null && d !== 0 && (
                  <span className={`text-xs mb-1 font-mono ${d > 0 ? "text-red-400" : "text-emerald-400"}`}>
                    {d > 0 ? `▲${d}` : `▼${Math.abs(d)}`}
                  </span>
                )}
              </div>
            </div>
          );
        })}
      </div>

      <div className="rounded-lg border border-zinc-800 bg-zinc-900 p-4">
        <p className="text-sm text-zinc-400 mb-3">Queue depth over time (last 60s)</p>
        {history.length === 0 ? (
          <div className="h-[220px] flex items-center justify-center text-zinc-600 text-sm">
            Waiting for first update…
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={220}>
            <AreaChart data={history}>
              <defs>
                {Object.entries(PRIORITY_COLORS).map(([k, color]) => (
                  <linearGradient key={k} id={`grad-${k}`} x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor={color} stopOpacity={0.3} />
                    <stop offset="95%" stopColor={color} stopOpacity={0} />
                  </linearGradient>
                ))}
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#27272a" />
              <XAxis dataKey="t" tick={{ fill: "#71717a", fontSize: 11 }} interval="preserveStartEnd" />
              <YAxis tick={{ fill: "#71717a", fontSize: 11 }} allowDecimals={false} width={32} />
              <Tooltip contentStyle={{ background: "#18181b", border: "1px solid #3f3f46", fontSize: 12 }} />
              <Legend
                wrapperStyle={{ fontSize: 12, color: "#a1a1aa" }}
                formatter={(v) => <span style={{ color: PRIORITY_COLORS[v] }}>{v}</span>}
              />
              {(["high", "normal", "low"] as const).map((p) => (
                <Area key={p} type="monotone" dataKey={p} stroke={PRIORITY_COLORS[p]}
                  fill={`url(#grad-${p})`} strokeWidth={2} dot={false} />
              ))}
            </AreaChart>
          </ResponsiveContainer>
        )}
      </div>

      {data?.jobs_by_state && (
        <div className="rounded-lg border border-zinc-800 bg-zinc-900 p-4">
          <p className="text-sm text-zinc-400 mb-4">Jobs by state</p>
          <div className="grid grid-cols-2 gap-6">
            {[
              { label: "Active", states: ACTIVE_STATES },
              { label: "Terminal", states: TERMINAL_STATES },
            ].map(({ label, states }) => {
              const entries = states.map((s) => ({ state: s, count: jobs_by_state[s] ?? 0 })).filter((e) => e.count > 0 || ACTIVE_STATES.includes(e.state));
              const max = Math.max(...entries.map((e) => e.count), 1);
              return (
                <div key={label}>
                  <p className="text-xs text-zinc-600 uppercase tracking-wider mb-2">{label}</p>
                  <div className="space-y-2">
                    {entries.map(({ state, count }) => (
                      <div key={state} className="flex items-center gap-2">
                        <span className={`w-16 text-xs text-right ${STATE_COLORS[state] ?? "text-zinc-400"}`}>{state}</span>
                        <div className="flex-1 bg-zinc-800 rounded-full h-1.5 overflow-hidden">
                          <div
                            className="h-full rounded-full transition-all duration-500"
                            style={{
                              width: `${(count / max) * 100}%`,
                              backgroundColor: state === "succeeded" ? "#34d399" : state === "dead" ? "#f87171" : state === "failed" ? "#fbbf24" : state === "running" ? "#a78bfa" : state === "claimed" ? "#8b5cf6" : state === "pending" ? "#60a5fa" : "#38bdf8",
                            }}
                          />
                        </div>
                        <span className="text-zinc-300 font-mono text-xs w-10 text-right">{count.toLocaleString()}</span>
                      </div>
                    ))}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
