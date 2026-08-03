import { useEffect, useState } from "react";

import Header from "../components/Header";
import MetricCard from "../components/MetricCard";
import PipelineFlow from "../components/PipelineFlow";
import { fetchMetrics } from "../services/metricsService";

function Dashboard() {
  const [metrics, setMetrics] = useState({
    eventsProcessed: 0,
    eventsFiltered: 0,
    windowsClosed: 0,
    activeWindows: 0,
  });

  useEffect(() => {
    const loadMetrics = async () => {
      try {
        const data = await fetchMetrics();
        setMetrics(data);
      } catch (error) {
        console.error("Metrics error:", error);
      }
    };

    loadMetrics();

    const interval = setInterval(loadMetrics, 2000);

    return () => clearInterval(interval);
  }, []);

  return (
    <div className="dashboard">
      <Header />

      <section className="metrics">
        <MetricCard
          title="Events Processed"
          value={metrics.eventsProcessed}
        />

        <MetricCard
          title="Events Filtered"
          value={metrics.eventsFiltered}
        />

        <MetricCard
          title="Windows Closed"
          value={metrics.windowsClosed}
        />

        <MetricCard
          title="Active Windows"
          value={metrics.activeWindows}
        />
      </section>

      <PipelineFlow />
    </div>
  );
}

export default Dashboard;