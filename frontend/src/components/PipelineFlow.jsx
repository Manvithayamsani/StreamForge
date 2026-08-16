import { useMemo } from "react";

import {
  ReactFlow,
  Background,
  Controls,
} from "@xyflow/react";

import "@xyflow/react/dist/style.css";


function PipelineFlow({ pipeline = [] }) {
  const workers = useMemo(
    () =>
      pipeline.filter(
        (node) =>
          node.type === "worker" &&
          node.online &&
          node.available
      ),
    [pipeline]
  );


  const nodes = useMemo(() => {
    const result = [
      {
        id: "producer",

        position: {
          x: 60,
          y: 220,
        },

        style: {
          minWidth: 170,
          textAlign: "center",
        },

        data: {
          label: "Telemetry Producer",
        },
      },

      {
        id: "kafka",

        position: {
          x: 300,
          y: 220,
        },

        style: {
          minWidth: 180,
          textAlign: "center",
          border: "2px solid #3b82f6",
        },

        data: {
          label:
            "Apache Kafka\n" +
            "streamforge-events\n" +
            "8 Partitions",
        },
      },
    ];


    workers.forEach(
      (worker, index) => {
        const y =
          20 + index * 190;

        const workerLag =
          Number(
            worker.processing_lag || 0
          );

        const workerRate =
          Number(
            worker.processing_rate || 0
          );

        let status =
          "HEALTHY";

        let border =
          "#22c55e";


        if (workerLag > 60) {
          status =
            "BOTTLENECK";

          border =
            "#ef4444";
        } else if (
          workerLag > 10
        ) {
          status =
            "WARNING";

          border =
            "#f59e0b";
        }


        result.push(
          {
            id: worker.id,

            position: {
              x: 590,
              y,
            },

            style: {
              border:
                `2px solid ${border}`,

              minWidth: 170,
              textAlign: "center",
            },

            data: {
              label:
                `${worker.worker_id}\n` +
                `${Number(
                  worker.active_partitions || 0
                )} partitions\n` +
                `${workerRate.toFixed(
                  2
                )} evt/s\n` +
                `${workerLag.toFixed(
                  2
                )}s lag\n` +
                `[${status}]`,
            },
          },

          {
            id:
              `window-${worker.id}`,

            position: {
              x: 870,
              y,
            },

            style: {
              minWidth: 150,
              textAlign: "center",
            },

            data: {
              label:
                "5-Min Window",
            },
          },

          {
            id:
              `rocksdb-${worker.id}`,

            position: {
              x: 1110,
              y,
            },

            style: {
              minWidth: 180,
              textAlign: "center",
            },

            data: {
              label:
                `RocksDB\n` +
                `${worker.worker_id} Local State`,
            },
          }
        );
      }
    );


    return result;
  }, [workers]);


  const edges = useMemo(() => {
    const result = [
      {
        id:
          "producer-kafka",

        source:
          "producer",

        target:
          "kafka",

        animated:
          true,
      },
    ];


    workers.forEach(
      (worker) => {
        result.push(
          {
            id:
              `kafka-${worker.id}`,

            source:
              "kafka",

            target:
              worker.id,

            animated:
              true,
          },

          {
            id:
              `${worker.id}-window`,

            source:
              worker.id,

            target:
              `window-${worker.id}`,

            animated:
              true,
          },

          {
            id:
              `window-rocksdb-${worker.id}`,

            source:
              `window-${worker.id}`,

            target:
              `rocksdb-${worker.id}`,

            animated:
              true,
          }
        );
      }
    );


    return result;
  }, [workers]);


  const topologyKey =
    workers
      .map(
        (worker) =>
          worker.id
      )
      .sort()
      .join("-");


  return (
    <section className="pipeline">

      <h2>
        Live Processing Pipeline
      </h2>

      <div className="flow">

        <ReactFlow
          key={topologyKey}
          nodes={nodes}
          edges={edges}
          fitView
          fitViewOptions={{
            padding: 0.15,
            minZoom: 0.25,
            maxZoom: 0.85,
          }}
          nodesDraggable={false}
          nodesConnectable={false}
          elementsSelectable={false}
        >

          <Background />

          <Controls />

        </ReactFlow>

      </div>

    </section>
  );
}


export default PipelineFlow;