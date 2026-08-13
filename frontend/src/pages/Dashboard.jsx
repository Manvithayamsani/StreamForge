import { useEffect, useState } from "react";

import Header from "../components/Header";
import MetricCard from "../components/MetricCard";
import PipelineFlow from "../components/PipelineFlow";
import { fetchTopology } from "../services/metricsService";

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

  useEffect(() => {
    const loadTopology = async () => {
      try {
        const data = await fetchTopology();
        setTopology(data);
      } catch (error) {
        console.error("Topology error:", error);
      }
    };

    loadTopology();

    const interval = setInterval(loadTopology, 2000);

    return () => clearInterval(interval);
  }, []);

  const summary = topology.summary;

  const getSystemStatus = () => {
    if (summary.workers_online === 0) {
      return {
        label: "CRITICAL",
        message: "No processing workers are online",
        className: "critical",
      };
    }

    if (summary.max_processing_lag > 60) {
      return {
        label: "BOTTLENECK",
        message: `Kafka backlog / processing lag is high (${summary.max_processing_lag.toFixed(
          1
        )}s)`,
        className: "danger",
      };
    }

    if (summary.max_processing_lag > 10) {
      return {
        label: "WARNING",
        message: `Processing lag is elevated (${summary.max_processing_lag.toFixed(
          1
        )}s)`,
        className: "warning",
      };
    }

    return {
      label: "HEALTHY",
      message: "Stream processing pipeline is operating normally",
      className: "healthy",
    };
  };

  const systemStatus = getSystemStatus();

  return (
    <div className="dashboard">
      <Header />

      <section className="metrics">
        <MetricCard
          title="Workers Online"
          value={summary.workers_online}
        />

        <MetricCard
          title="Events Processed"
          value={summary.events_processed}
        />

        <MetricCard
          title="Processing Rate"
          value={`${summary.processing_rate.toFixed(2)} evt/s`}
        />

        <MetricCard
          title="Active Partitions"
          value={summary.active_partitions}
        />

        <MetricCard
          title="Processing Lag"
          value={`${summary.max_processing_lag.toFixed(3)} s`}
        />

        <MetricCard
          title="Events Filtered"
          value={summary.events_filtered}
        />
      </section>

      <section className={`bottleneck-status ${systemStatus.className}`}>
        <div>
          <span className="bottleneck-label">
            {systemStatus.label}
          </span>

          <strong>
            {systemStatus.message}
          </strong>
        </div>

        <span>
          Cluster Rate: {summary.processing_rate.toFixed(2)} evt/s
        </span>
      </section>

      <PipelineFlow pipeline={topology.pipeline} />
    </div>
  );
}

export default Dashboard;