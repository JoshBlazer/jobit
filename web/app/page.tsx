"use client";
import { useLiveStats } from "@/lib/ws";
import {
  AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid,
} from "recharts";
import { useRef, useState } from "react";

const PRIORITY_COLORS: Record<string, string> = {
  high: "#f97316",
  normal: "#3b82f6",
  low: "#6b7280",
};

export default function QueuePage() {
  const { data, connected } = useLiveStats();
  const [history, setHistory] = useState<{ t: string; high: number; normal: number; low: number }[]>([]);

  // Accumulate up to 60 snapshots for the chart
  const lastTs = useRef("");
  if (data && data.timestamp !== lastTs.current) {
    lastTs.current = String(data.timestamp);
    const totals = { high: 0, normal: 0, low: 0 };
    (data.queues ?? []).forEach((q) => {
      if (q.priority in totals) totals[q.priority as keyof typeof totals] += q.depth;
    });
    setHistory((h) => [...h.slice(-59), { t: new Date().toLocaleTimeString(), ...totals }]);
  }

  const stateColors: Record<string, string> = {
    succeeded: "text-emerald-400",
    failed: "text-amber-400",
    dead: "text-red-400",
    pending: "text-blue-400",
    claimed: "text-purple-400",
    running: "text-purple-400",
    scheduled: "text-sky-400",
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold">Queue Depth</h1>
        <span className={`text-xs px-2 py-1 rounded-full border ${connected ? "border-emerald-700 text-emerald-400" : "border-zinc-700 text-zinc-500"}`}>
          {connected ? "● live" : "○ reconnecting"}
        </span>
      </div>

      {/* Stat cards */}
      <div className="grid grid-cols-3 gap-4">
        {["high", "normal", "low"].map((p) => {
          const total = (data?.queues ?? []).filter((q) => q.priority === p).reduce((s, q) => s + q.depth, 0);
          return (
            <div key={p} className="rounded-lg border border-zinc-800 bg-zinc-900 p-4">
              <p className="text-xs text-zinc-500 mb-1 capitalize">{p} priority</p>
              <p className="text-3xl font-bold" style={{ color: PRIORITY_COLORS[p] }}>{total}</p>
            </div>
          );
        })}
      </div>

      {/* Area chart */}
      <div className="rounded-lg border border-zinc-800 bg-zinc-900 p-4">
        <p className="text-sm text-zinc-400 mb-3">Queue depth over time (last 60s)</p>
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
            <XAxis dataKey="t" tick={{ fill: "#71717a", fontSize: 11 }} />
            <YAxis tick={{ fill: "#71717a", fontSize: 11 }} allowDecimals={false} />
            <Tooltip contentStyle={{ background: "#18181b", border: "1px solid #3f3f46", fontSize: 12 }} />
            {["high", "normal", "low"].map((p) => (
              <Area key={p} type="monotone" dataKey={p} stroke={PRIORITY_COLORS[p]}
                fill={`url(#grad-${p})`} strokeWidth={2} dot={false} />
            ))}
          </AreaChart>
        </ResponsiveContainer>
      </div>

      {/* Jobs by state */}
      {data?.jobs_by_state && (
        <div className="rounded-lg border border-zinc-800 bg-zinc-900 p-4">
          <p className="text-sm text-zinc-400 mb-3">Jobs by state</p>
          <div className="flex flex-wrap gap-4">
            {Object.entries(data.jobs_by_state).map(([state, count]) => (
              <div key={state} className="flex items-center gap-2">
                <span className={`text-sm font-medium ${stateColors[state] ?? "text-zinc-300"}`}>{state}</span>
                <span className="text-zinc-300 font-mono text-sm">{count}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
