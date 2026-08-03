import {
  ReactFlow,
  Background,
  Controls,
} from "@xyflow/react";

import "@xyflow/react/dist/style.css";

const nodes = [
  {
    id: "1",
    position: { x: 0, y: 100 },
    data: { label: "🚚 Telemetry Producer" },
  },
  {
    id: "2",
    position: { x: 220, y: 100 },
    data: { label: "⚡ Kafka" },
  },
  {
    id: "3",
    position: { x: 440, y: 100 },
    data: { label: "🔍 Filter" },
  },
  {
    id: "4",
    position: { x: 660, y: 100 },
    data: { label: "🔄 Map" },
  },
  {
    id: "5",
    position: { x: 880, y: 100 },
    data: { label: "⏱ 5-Min Window" },
  },
  {
    id: "6",
    position: { x: 1100, y: 100 },
    data: { label: "📊 Average" },
  },
];

const edges = [
  { id: "1-2", source: "1", target: "2", animated: true },
  { id: "2-3", source: "2", target: "3", animated: true },
  { id: "3-4", source: "3", target: "4", animated: true },
  { id: "4-5", source: "4", target: "5", animated: true },
  { id: "5-6", source: "5", target: "6", animated: true },
];

function PipelineFlow() {
  return (
    <section className="pipeline">
      <h2>Live Processing Pipeline</h2>

      <div className="flow">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          fitView
          nodesDraggable={false}
        >
          <Background />
          <Controls />
        </ReactFlow>
      </div>
    </section>
  );
}

export default PipelineFlow;