# Grafana / Prometheus / Tempo MCP

Small [Model Context Protocol](https://modelcontextprotocol.io/) server so Cursor (or any MCP client) can:

- List and fetch Grafana dashboards (HTTP API)
- Run PromQL (direct against Prometheus, or via Grafana datasource proxy)
- Fetch Tempo traces and run TraceQL search (e.g. slow spans)

## Prerequisites

- Node.js **20+** and **npm** on your machine (required to install `@modelcontextprotocol/sdk`)
- From repo root:

```powershell
cd mcp/grafana-mcp
npm install
```

On Windows PowerShell, use `;` instead of `&&` if your shell version does not support `&&`.

### Windows: `'node' is not recognized` in Cursor MCP logs

Cursor often starts MCP **without your interactive shell PATH**, so `"command": "node"` fails even if `node` works in PowerShell.

**Fix A (recommended):** use the **full path** to `node.exe` in MCP config. In PowerShell:

```powershell
(Get-Command node -ErrorAction SilentlyContinue).Source
```

Copy the output (for example `C:\Program Files\nodejs\node.exe`) into `mcp.json`:

```json
{
  "mcpServers": {
    "grafana-observability": {
      "command": "C:\\Program Files\\nodejs\\node.exe",
      "args": ["C:/Users/tilsh/Documents/Repos/backend-at-scale/mcp/grafana-mcp/src/server.mjs"]
    }
  }
}
```

**Fix B:** use the bundled launcher `run-mcp.cmd` (tries `Program Files\nodejs` first):

```json
{
  "mcpServers": {
    "grafana-observability": {
      "command": "C:\\Users\\tilsh\\Documents\\Repos\\backend-at-scale\\mcp\\grafana-mcp\\run-mcp.cmd",
      "args": []
    }
  }
}
```

**Fix C:** set `NODE_EXE` in the MCP server `env` block to your `node.exe` path, and point `command` at `run-mcp.cmd` as in Fix B.

If Node is not installed at all, install **Node.js 20 LTS** from [https://nodejs.org/](https://nodejs.org/), then fully **quit and restart Cursor** so it picks up the new install.

## Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `GRAFANA_URL` | `http://127.0.0.1:3000` | Grafana base URL |
| `GRAFANA_USER` / `GRAFANA_PASSWORD` | `admin` / `admin` | Basic auth (or set `GRAFANA_API_TOKEN`) |
| `GRAFANA_API_TOKEN` | (empty) | Optional service account token (overrides basic auth) |
| `PROMETHEUS_URL` | `http://127.0.0.1:9090` | Prometheus for direct queries |
| `TEMPO_URL` | `http://127.0.0.1:3200` | Tempo HTTP API |
| `PROMETHEUS_DATASOURCE_UID` | `prometheus` | UID used by Grafana proxy tools |
| `NODE_EXE` | (empty) | Windows only: full path to `node.exe` when using `run-mcp.cmd` |

## Tools

- `prometheus_instant_query` / `prometheus_range_query`
- `grafana_prometheus_proxy_instant` / `grafana_prometheus_proxy_range`
- `grafana_list_dashboards` / `grafana_get_dashboard`
- `tempo_get_trace` / `tempo_search_traceql`
- `latency_debug_bundle` — runs a fixed set of queries + a TraceQL search for traces **> 400ms** (`trace:duration` intrinsic)

## Cursor MCP configuration

Add to your Cursor MCP settings (path depends on OS), for example:

```json
{
  "mcpServers": {
    "grafana-observability": {
      "command": "node",
      "args": ["C:/Users/tilsh/Documents/Repos/backend-at-scale/mcp/grafana-mcp/src/server.mjs"],
      "env": {
        "GRAFANA_URL": "http://127.0.0.1:3000",
        "GRAFANA_USER": "admin",
        "GRAFANA_PASSWORD": "admin",
        "PROMETHEUS_URL": "http://127.0.0.1:9090",
        "TEMPO_URL": "http://127.0.0.1:3200"
      }
    }
  }
}
```

Adjust paths and credentials for your machine. After saving, restart Cursor or reload MCP servers.

## Debugging p95 > 400ms

1. Call **`latency_debug_bundle`** with `range_minutes` (e.g. `15`).
2. Inspect `prometheus` section: HTTP histogram vs `ecommerce_db_query_duration_seconds` for `select_products`.
3. From `tempo_traceql`, pick a `traceID` and call **`tempo_get_trace`** to see span breakdown (HTTP → Redis → Postgres → Kafka).

If `resource.service.name` does not match (your `SERVICE_NAME` env), adjust the TraceQL filter or call `tempo_search_traceql` manually.

Common causes when histograms show high latency:

- **Load / saturation**: RPS near pool or CPU limits; check DB pool gauges and container CPU.
- **DB**: slow `select_products` (index, cold cache, contention); compare DB histogram to HTTP.
- **Cold paths**: cache miss + DB + Redis `SET` + Kafka publish on every miss.
- **Windows Docker networking**: host → container latency; compare in-container k6 (`BASE_URL=http://app:8080`) vs host.
