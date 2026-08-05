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

      <PipelineFlow pipeline={topology.pipeline} />
    </div>
  );
}

export default Dashboard;