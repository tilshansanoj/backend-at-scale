# Postgres MCP

MCP server for **querying** your ecommerce Postgres (and optional **insert** for local dev).

## Prerequisites

- Node.js **20+**
- `cd mcp/postgres-mcp && npm install`

## Environment

| Variable | Default | Purpose |
|----------|---------|---------|
| `DATABASE_URL` or `POSTGRES_URL` | `postgres://postgres:postgres@127.0.0.1:5432/ecommerce` | Connection string (use **primary** port `5432` for writes if you enable insert) |
| `POSTGRES_MCP_MAX_ROWS` | `500` | Cap for `select_readonly` |
| `POSTGRES_MCP_ALLOW_INSERT` | `false` | Set `true` to expose `insert_product` tool |

## Tools

- `list_tables` — tables in a schema
- `describe_table` — column metadata
- `select_readonly` — only `SELECT` / `WITH` / `EXPLAIN` / `SHOW` / `TABLE` (no semicolons)
- `insert_product` — optional, writes to **primary** (same as API) when `POSTGRES_MCP_ALLOW_INSERT=true`

## Cursor MCP example

Point `command` at your full `node.exe` path on Windows if needed (see `mcp/grafana-mcp/README.md`).

```json
{
  "mcpServers": {
    "postgres-ecommerce": {
      "command": "node",
      "args": ["C:/Users/tilsh/Documents/Repos/backend-at-scale/mcp/postgres-mcp/src/server.mjs"],
      "env": {
        "POSTGRES_URL": "postgres://postgres:postgres@127.0.0.1:5432/ecommerce?sslmode=disable"
      }
    }
  }
}
```

Read replica (optional MCP read path): `postgres://postgres:postgres@127.0.0.1:5433/ecommerce?sslmode=disable` when compose is up.

## Safety

Default configuration is **read-only** except the optional insert tool. Do not enable `insert_product` against production.
