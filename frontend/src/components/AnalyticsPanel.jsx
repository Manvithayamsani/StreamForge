function LineChart({ data, dataKey, suffix = "", title }) {
  const width = 700;
  const height = 220;
  const padding = 28;

  const values = data.map((item) => Number(item[dataKey] || 0));
  const maxValue = Math.max(...values, 1);
  const minValue = Math.min(...values, 0);

  const range = Math.max(maxValue - minValue, 1);

  const points = values
    .map((value, index) => {
      const x =
        data.length <= 1
          ? padding
          : padding +
            (index / (data.length - 1)) *
              (width - padding * 2);

      const y =
        height -
        padding -
        ((value - minValue) / range) *
          (height - padding * 2);

      return `${x},${y}`;
    })
    .join(" ");

  const latest = values.at(-1) || 0;

  return (
    <div className="analytics-chart-card">
      <div className="chart-header">
        <div>
          <span className="chart-label">LIVE</span>
          <h3>{title}</h3>
        </div>

        <strong>
          {latest.toFixed(2)}
          {suffix}
        </strong>
      </div>

      <div className="chart-container">
        {data.length > 1 ? (
          <svg
            viewBox={`0 0 ${width} ${height}`}
            preserveAspectRatio="none"
            className="line-chart"
          >
            <line
              x1={padding}
              y1={height - padding}
              x2={width - padding}
              y2={height - padding}
              className="chart-axis"
            />

            <line
              x1={padding}
              y1={padding}
              x2={padding}
              y2={height - padding}
              className="chart-axis"
            />

            <polyline
              points={points}
              fill="none"
              className="chart-line"
            />
          </svg>
        ) : (
          <div className="chart-empty">
            Collecting live samples...
          </div>
        )}
      </div>

      <div className="chart-footer">
        <span>Last ~60 seconds</span>
        <span>Peak: {maxValue.toFixed(2)}{suffix}</span>
      </div>
    </div>
  );
}


function getWorkerStatus(worker) {
  const lag = Number(worker.processing_lag || 0);

  if (!worker.available || !worker.online) {
    return {
      label: "OFFLINE",
      className: "offline",
    };
  }

  if (lag > 60) {
    return {
      label: "BOTTLENECK",
      className: "danger",
    };
  }

  if (lag > 10) {
    return {
      label: "WARNING",
      className: "warning",
    };
  }

  return {
    label: "HEALTHY",
    className: "healthy",
  };
}


function AnalyticsPanel({
  history,
  workers,
  summary,
}) {
  const processed = Number(summary.events_processed || 0);
  const filtered = Number(summary.events_filtered || 0);

  const validEvents = Math.max(processed - filtered, 0);

  const successRate =
    processed > 0
      ? (validEvents / processed) * 100
      : 100;

  const peakLiveRate =
    history.length > 0
      ? Math.max(
          ...history.map(
            (item) => Number(item.rate || 0)
          )
        )
      : 0;

  return (
    <section className="analytics-section">
      <div className="section-heading">
        <div>
          <span className="section-kicker">
            OBSERVABILITY
          </span>
          <h2>Real-Time Analytics</h2>
        </div>

        <span className="analytics-live">
          ● Live • 2s refresh
        </span>
      </div>

      <div className="analytics-summary">
        <div className="mini-stat">
          <span>Valid Events</span>
          <strong>{validEvents.toLocaleString()}</strong>
        </div>

        <div className="mini-stat">
          <span>Success Rate</span>
          <strong>{successRate.toFixed(1)}%</strong>
        </div>

        <div className="mini-stat">
          <span>Live Peak Rate</span>
          <strong>
            {peakLiveRate.toFixed(2)} evt/s
          </strong>
        </div>

        <div className="mini-stat">
          <span>Cluster Capacity</span>
          <strong>
            {summary.active_partitions || 0} partitions
          </strong>
        </div>
      </div>

      <div className="analytics-grid">
        <LineChart
          data={history}
          dataKey="rate"
          suffix=" evt/s"
          title="Processing Rate"
        />

        <LineChart
          data={history}
          dataKey="lag"
          suffix=" s"
          title="Processing Lag"
        />
      </div>

      <div className="analytics-lower-grid">
        <div className="analytics-card">
          <div className="card-heading">
            <div>
              <span className="section-kicker">
                DISTRIBUTED PROCESSING
              </span>
              <h3>Worker Performance</h3>
            </div>

            <span>{workers.length} workers</span>
          </div>

          <div className="worker-table-wrapper">
            <table className="worker-table">
              <thead>
                <tr>
                  <th>Worker</th>
                  <th>Partitions</th>
                  <th>Rate</th>
                  <th>Lag</th>
                  <th>Status</th>
                </tr>
              </thead>

              <tbody>
                {workers.length === 0 ? (
                  <tr>
                    <td colSpan="5">
                      No workers registered
                    </td>
                  </tr>
                ) : (
                  workers.map((worker) => {
                    const status =
                      getWorkerStatus(worker);

                    return (
                      <tr key={worker.id}>
                        <td>
                          <strong>
                            {worker.worker_id}
                          </strong>
                        </td>

                        <td>
                          {worker.active_partitions}
                        </td>

                        <td>
                          {Number(
                            worker.processing_rate || 0
                          ).toFixed(2)}{" "}
                          evt/s
                        </td>

                        <td>
                          {Number(
                            worker.processing_lag || 0
                          ).toFixed(2)}{" "}
                          s
                        </td>

                        <td>
                          <span
                            className={`worker-status ${status.className}`}
                          >
                            {status.label}
                          </span>
                        </td>
                      </tr>
                    );
                  })
                )}
              </tbody>
            </table>
          </div>
        </div>

        <div className="analytics-card benchmark-card">
          <span className="section-kicker">
            VERIFIED PERFORMANCE
          </span>

          <h3>Benchmark Result</h3>

          <div className="benchmark-number">
            142K+
            <span> events/sec</span>
          </div>

          <p>
            Distributed throughput benchmark using
            two workers across eight Kafka
            partitions.
          </p>

          <div className="benchmark-details">
            <div>
              <span>Worker A</span>
              <strong>73.8K evt/s</strong>
            </div>

            <div>
              <span>Worker B</span>
              <strong>68.3K evt/s</strong>
            </div>

            <div>
              <span>Test Load</span>
              <strong>500K events</strong>
            </div>

            <div>
              <span>Partitions</span>
              <strong>8</strong>
            </div>
          </div>

          <div className="benchmark-badge">
            ✓ 100K evt/s target exceeded
          </div>
        </div>
      </div>
    </section>
  );
}

export default AnalyticsPanel;