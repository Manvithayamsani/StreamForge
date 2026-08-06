export async function fetchTopology() {
  const response = await fetch("http://localhost:8000/topology");

  if (!response.ok) {
    throw new Error("Failed to fetch topology");
  }

  return response.json();
}