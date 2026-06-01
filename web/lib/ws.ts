import { useEffect, useRef, useState } from "react";
import type { StatsResponse } from "./api";

const WS_URL = process.env.NEXT_PUBLIC_WS_URL ?? "ws://localhost:8080/ws";

export function useLiveStats() {
  const [data, setData] = useState<StatsResponse | null>(null);
  const [connected, setConnected] = useState(false);
  const ws = useRef<WebSocket | null>(null);

  useEffect(() => {
    function connect() {
      const sock = new WebSocket(WS_URL);
      ws.current = sock;

      sock.onopen = () => setConnected(true);
      sock.onclose = () => {
        setConnected(false);
        // Reconnect after 3 s
        setTimeout(connect, 3000);
      };
      sock.onerror = () => sock.close();
      sock.onmessage = (e) => {
        try {
          setData(JSON.parse(e.data));
        } catch {}
      };
    }
    connect();
    return () => {
      ws.current?.close();
    };
  }, []);

  return { data, connected };
}
