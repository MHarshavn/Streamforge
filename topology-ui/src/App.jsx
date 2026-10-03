import React, { useEffect, useMemo, useState } from "react";
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
} from "@xyflow/react";

const API = import.meta.env.VITE_STREAMFORGE_API ?? "http://127.0.0.1:8000";

function layout(nodes) {
  const groups = { broker: 0, partition: 1, worker: 2, state: 3, backend: 4 };
  return nodes.map((node, index) => ({
    ...node,
    position: {
      x: groups[node.type] * 250,
      y: (index % 10) * 90,
    },
    style: {
      padding: 10,
      borderRadius: 8,
      border: "1px solid #64748b",
      background:
        node.type === "worker" ? "#0f172a" :
        node.type === "broker" ? "#172554" :
        node.type === "state" ? "#3f2a12" : "#111827",
      color: "white",
      width: 210,
    },
  }));
}

export default function App() {
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [workers, setWorkers] = useState(0);
  const [eventsPerSecond, setEventsPerSecond] = useState(0);
  const [processingLag, setProcessingLag] = useState(0);
  const [error, setError] = useState("");

  async function refresh() {
    try {
      const response = await fetch(API + "/topology");
      if (!response.ok) throw new Error("Topology endpoint unavailable");
      const data = await response.json();
      setNodes(layout(data.nodes));
      setEdges(data.edges);
      setWorkers(data.active_workers);
      setEventsPerSecond(data.events_per_second ?? 0);
      setProcessingLag(data.processing_lag_seconds ?? 0);
      setError("");
    } catch (err) {
      setError(err.message);
    }
  }

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, 5000);
    return () => clearInterval(id);
  }, []);

  const workerNodes = useMemo(
    () => nodes.filter((n) => n.type === "worker"),
    [nodes]
  );

  return (
    <main className="page">
      <header>
        <div>
          <h1>StreamForge Distributed Topology</h1>
          <p>Kafka → partitions → workers → RocksDB → FastAPI</p>
        </div>
        <div className="status">
          <strong>{workers}</strong> active workers
          <span>{eventsPerSecond.toFixed(1)} events/sec</span>
          <span>{processingLag.toFixed(1)} sec lag</span>
        </div>
      </header>
      {error && <div className="error">{error}</div>}
      <section className="graph">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          fitView
        >
          <Background />
          <Controls />
          <MiniMap />
        </ReactFlow>
      </section>
      <footer>
        <span>Workers are refreshed every 5 seconds.</span>
        <span>{workerNodes.length} worker nodes visible</span>
      </footer>
    </main>
  );
}
