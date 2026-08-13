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

    const workerLag = Number(worker.processing_lag || 0);
    const workerRate = Number(worker.processing_rate || 0);
    let workerStatus = "HEALTHY";
    let workerBorder = "#22c55e";

    if (!worker.available || !worker.online) {
      workerStatus = "OFFLINE";
      workerBorder = "#ef4444";
    } else if (workerLag > 60) {
      workerStatus = "BOTTLENECK";
      workerBorder = "#ef4444";
    } else if (workerLag > 10) {
      workerStatus = "WARNING";
      workerBorder = "#f59e0b";
    }

    nodes.push(
      {
        id: workerNodeId,
        position: { x: 470, y },

        style: {
          border: `2px solid ${workerBorder}`,
          minWidth: 170,
        },

        data: {
          label:
            worker.available && worker.online
              ? `${worker.worker_id}\n${worker.active_partitions} partitions\n${workerRate.toFixed(2)} evt/s\n${workerLag.toFixed(1)}s lag\n[${workerStatus}]`
              : `${worker.worker_id}\nOFFLINE`,
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
          label: `💾 RocksDB\n${worker.worker_id} Local State`,
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