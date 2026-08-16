export async function fetchTopology() {
  const response = await fetch("http://localhost:8000/topology");

  if (!response.ok) {
    throw new Error("Failed to fetch topology");
  }

  return response.json();
}
export async function fetchAggregations() {
  const response = await fetch(
    "http://localhost:8000/aggregations"
  );

  if (!response.ok) {
    throw new Error("Failed to fetch aggregations");
  }

  return response.json();
}
export async function fetchStreamStatus() {
  const response = await fetch(
    "http://localhost:8000/stream/status"
  );

  if (!response.ok) {
    throw new Error(
      "Failed to fetch stream status"
    );
  }

  return response.json();
}


export async function startEventStream() {
  const response = await fetch(
    "http://localhost:8000/stream/start",
    {
      method: "POST",
    }
  );

  if (!response.ok) {
    throw new Error(
      "Failed to start event stream"
    );
  }

  return response.json();
}


export async function stopEventStream() {
  const response = await fetch(
    "http://localhost:8000/stream/stop",
    {
      method: "POST",
    }
  );

  if (!response.ok) {
    throw new Error(
      "Failed to stop event stream"
    );
  }

  return response.json();
}
export async function fetchManagedWorkers() {
  const response = await fetch(
    "http://localhost:8000/cluster/workers"
  );

  if (!response.ok) {
    throw new Error("Failed to fetch managed workers");
  }

  return response.json();
}

export async function addWorker() {
  const response = await fetch(
    "http://localhost:8000/cluster/workers/start",
    {
      method: "POST",
    }
  );

  if (!response.ok) {
    throw new Error("Failed to start worker");
  }

  return response.json();
}

export async function stopWorker(workerId) {
  const response = await fetch(
    `http://localhost:8000/cluster/workers/${workerId}/stop`,
    {
      method: "POST",
    }
  );

  if (!response.ok) {
    throw new Error("Failed to stop worker");
  }

  return response.json();
}