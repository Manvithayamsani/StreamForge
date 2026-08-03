export async function fetchMetrics() {
  const response = await fetch("/metrics");

  if (!response.ok) {
    throw new Error("Failed to fetch metrics");
  }

  const text = await response.text();

  const metrics = {};

  text.split("\n").forEach((line) => {
    if (!line.startsWith("#") && line.trim() !== "") {
      const parts = line.trim().split(/\s+/);

      if (parts.length >= 2) {
        metrics[parts[0]] = Number(parts[1]);
      }
    }
  });

  return {
    eventsProcessed:
      metrics["streamforge_events_processed_total"] ?? 0,

    eventsFiltered:
      metrics["streamforge_events_filtered_total"] ?? 0,

    windowsClosed:
      metrics["streamforge_windows_closed_total"] ?? 0,

    activeWindows:
      metrics["streamforge_active_windows"] ?? 0,
  };
}