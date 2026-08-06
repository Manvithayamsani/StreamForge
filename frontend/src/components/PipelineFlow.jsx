import {
  ReactFlow,
  Background,
  Controls,
} from "@xyflow/react";

import "@xyflow/react/dist/style.css";

function PipelineFlow({ pipeline }) {
  const workers = pipeline.filter(
    (node) => node.type === "worker"
  );

  const nodes = [
    {
      id: "producer",
      position: { x: 0, y: 180 },
      data: {
        label: "🚚 Telemetry Producer",
      },
    },
    {
      id: "kafka",
      position: { x: 220, y: 180 },
      data: {
        label: "⚡ Kafka\n8 Partitions",
      },
    },
  ];

  const edges = [
    {
      id: "producer-kafka",
      source: "producer",
      target: "kafka",
      animated: true,
    },
  ];

  workers.forEach((worker, index) => {
    const y = 40 + index * 220;

    const workerNodeId = worker.id;
    const windowNodeId = `window-${worker.id}`;
    const rocksDbNodeId = `rocksdb-${worker.id}`;

    nodes.push(
      {
        id: workerNodeId,
        position: { x: 470, y },
        data: {
          label:
            worker.available && worker.online
              ? `🟢 ${worker.worker_id}
${worker.active_partitions} partitions
${Number(worker.processing_rate).toFixed(2)} evt/s`
              : `🔴 ${worker.worker_id} Offline`,
        },
      },
      {
        id: windowNodeId,
        position: { x: 750, y },
        data: {
          label: "⏱ 5-Min Window",
        },
      },
      {
        id: rocksDbNodeId,
        position: { x: 1030, y },
        data: {
          label: `💾 RocksDB
${worker.worker_id} Local State`,
        },
      }
    );

    edges.push(
      {
        id: `kafka-${workerNodeId}`,
        source: "kafka",
        target: workerNodeId,
        animated: true,
      },
      {
        id: `${workerNodeId}-${windowNodeId}`,
        source: workerNodeId,
        target: windowNodeId,
        animated: true,
      },
      {
        id: `${windowNodeId}-${rocksDbNodeId}`,
        source: windowNodeId,
        target: rocksDbNodeId,
        animated: true,
      }
    );
  });

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