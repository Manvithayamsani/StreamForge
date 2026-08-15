import { useEffect, useState } from "react";

import Header from "../components/Header";
import MetricCard from "../components/MetricCard";
import PipelineFlow from "../components/PipelineFlow";
import AnalyticsPanel from "../components/AnalyticsPanel";

import {
  fetchTopology,
  fetchAggregations,
} from "../services/metricsService";


function Dashboard() {
  const [topology, setTopology] = useState({
    pipeline: [],
    summary: {
      workers_online: 0,
      events_processed: 0,
      events_filtered: 0,
      processing_rate: 0,
      active_partitions: 0,
      max_processing_lag: 0,
    },
  });

  const [history, setHistory] = useState([]);
  const [aggregations, setAggregations] = useState([]);


  useEffect(() => {
    const loadTopology = async () => {
      try {
        const data = await fetchTopology();

        setTopology(data);

        const summary = data.summary || {};

        const sample = {
          time: Date.now(),
          rate: Number(
            summary.processing_rate || 0
          ),
          lag: Number(
            summary.max_processing_lag || 0
          ),
          processed: Number(
            summary.events_processed || 0
          ),
          filtered: Number(
            summary.events_filtered || 0
          ),
        };

        setHistory((previous) => {
          const updated = [
            ...previous,
            sample,
          ];

          return updated.slice(-30);
        });
      } catch (error) {
        console.error(
          "Topology error:",
          error
        );
      }
    };


    const loadAggregations = async () => {
      try {
        const data =
          await fetchAggregations();

        setAggregations(
          data.slice(0, 10)
        );
      } catch (error) {
        console.error(
          "Aggregation error:",
          error
        );
      }
    };


    loadTopology();
    loadAggregations();

    const topologyInterval =
      setInterval(
        loadTopology,
        2000
      );

    const aggregationInterval =
      setInterval(
        loadAggregations,
        2000
      );


    return () => {
      clearInterval(
        topologyInterval
      );

      clearInterval(
        aggregationInterval
      );
    };
  }, []);


  const summary =
    topology.summary;

  const workers =
    topology.pipeline.filter(
      (node) =>
        node.type === "worker"
    );


  const getSystemStatus = () => {
    if (
      summary.workers_online === 0
    ) {
      return {
        label: "CRITICAL",
        message:
          "No processing workers are online",
        className: "critical",
      };
    }

    if (
      summary.max_processing_lag >
      60
    ) {
      return {
        label: "BOTTLENECK",
        message:
          `Kafka backlog / processing lag is high (` +
          `${summary.max_processing_lag.toFixed(
            1
          )}s)`,
        className: "danger",
      };
    }

    if (
      summary.max_processing_lag >
      10
    ) {
      return {
        label: "WARNING",
        message:
          `Processing lag is elevated (` +
          `${summary.max_processing_lag.toFixed(
            1
          )}s)`,
        className: "warning",
      };
    }

    return {
      label: "HEALTHY",
      message:
        "Stream processing pipeline is operating normally",
      className: "healthy",
    };
  };


  const systemStatus =
    getSystemStatus();


  return (
    <div className="dashboard">
      <Header />

      <section className="metrics">
        <MetricCard
          title="Workers Online"
          value={
            summary.workers_online
          }
        />

        <MetricCard
          title="Events Processed"
          value={
            summary.events_processed.toLocaleString()
          }
        />

        <MetricCard
          title="Processing Rate"
          value={`${summary.processing_rate.toFixed(
            2
          )} evt/s`}
        />

        <MetricCard
          title="Active Partitions"
          value={
            summary.active_partitions
          }
        />

        <MetricCard
          title="Processing Lag"
          value={`${summary.max_processing_lag.toFixed(
            3
          )} s`}
        />

        <MetricCard
          title="Events Filtered"
          value={
            summary.events_filtered.toLocaleString()
          }
        />
      </section>


      <section
        className={`bottleneck-status ${systemStatus.className}`}
      >
        <div>
          <span className="bottleneck-label">
            {systemStatus.label}
          </span>

          <strong>
            {
              systemStatus.message
            }
          </strong>
        </div>

        <span>
          Cluster Rate:{" "}
          {summary.processing_rate.toFixed(
            2
          )}{" "}
          evt/s
        </span>
      </section>


      <PipelineFlow
        pipeline={
          topology.pipeline
        }
      />


      <section className="aggregation-section">
        <div className="section-heading">
          <div>
            <span className="section-kicker">
              STREAM OUTPUT
            </span>

            <h2>
              Live Window Aggregations
            </h2>
          </div>

          <span className="analytics-live">
            ● Live • 2s refresh
          </span>
        </div>


        <div className="aggregation-card">
          <table className="aggregation-table">
            <thead>
              <tr>
                <th>Truck</th>
                <th>Worker</th>
                <th>
                  5-Min Window
                </th>
                <th>
                  Readings
                </th>
                <th>
                  Avg Temperature
                </th>
              </tr>
            </thead>

            <tbody>
              {
                aggregations.length ===
                0
                  ? (
                      <tr>
                        <td
                          colSpan="5"
                        >
                          No aggregation data available
                        </td>
                      </tr>
                    )
                  : (
                      aggregations.map(
                        (item) => {
                          const start =
                            new Date(
                              item.window_start
                            );

                          const end =
                            new Date(
                              item.window_end
                            );

                          const windowLabel =
                            `${start.toLocaleTimeString(
                              [],
                              {
                                hour: "2-digit",
                                minute:
                                  "2-digit",
                              }
                            )} - ` +
                            `${end.toLocaleTimeString(
                              [],
                              {
                                hour: "2-digit",
                                minute:
                                  "2-digit",
                              }
                            )}`;

                          return (
                            <tr
                              key={`${item.worker_id}-${item.truck_id}-${item.window_start}`}
                            >
                              <td>
                                <strong>
                                  {
                                    item.truck_id
                                  }
                                </strong>
                              </td>

                              <td>
                                {
                                  item.worker_id
                                }
                              </td>

                              <td>
                                {
                                  windowLabel
                                }
                              </td>

                              <td>
                                {
                                  item.reading_count
                                }
                              </td>

                              <td>
                                {Number(
                                  item.average_temperature
                                ).toFixed(
                                  2
                                )}
                                °C
                              </td>
                            </tr>
                          );
                        }
                      )
                    )
              }
            </tbody>
          </table>
        </div>
      </section>


      <AnalyticsPanel
        history={history}
        workers={workers}
        summary={summary}
      />
    </div>
  );
}


export default Dashboard;