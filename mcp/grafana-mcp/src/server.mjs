#!/usr/bin/env node
/**
 * MCP server: Grafana (dashboards), Prometheus (PromQL), Tempo (traces / TraceQL search).
 * Stdio transport for Cursor and other MCP clients.
 */
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";

function env(name, fallback = "") {
  const v = process.env[name];
  return v != null && v !== "" ? v : fallback;
}

const GRAFANA_URL = env("GRAFANA_URL", "http://127.0.0.1:3000").replace(/\/$/, "");
const GRAFANA_USER = env("GRAFANA_USER", "admin");
const GRAFANA_PASSWORD = env("GRAFANA_PASSWORD", "admin");
const PROMETHEUS_URL = env("PROMETHEUS_URL", "http://127.0.0.1:9090").replace(/\/$/, "");
const TEMPO_URL = env("TEMPO_URL", "http://127.0.0.1:3200").replace(/\/$/, "");
const PROMETHEUS_DS_UID = env("PROMETHEUS_DATASOURCE_UID", "prometheus");

function grafanaAuthHeader() {
  const token = env("GRAFANA_API_TOKEN", "");
  if (token) return { Authorization: `Bearer ${token}` };
  const basic = Buffer.from(`${GRAFANA_USER}:${GRAFANA_PASSWORD}`).toString("base64");
  return { Authorization: `Basic ${basic}` };
}

async function fetchJson(url, init = {}) {
  const res = await fetch(url, {
    ...init,
    headers: {
      Accept: "application/json",
      ...(init.headers || {}),
    },
  });
  const text = await res.text();
  let body;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = { raw: text };
  }
  if (!res.ok) {
    const err = new Error(`HTTP ${res.status} ${res.statusText}: ${url}`);
    err.body = body;
    throw err;
  }
  return body;
}

async function prometheusInstant(query) {
  const u = new URL(`${PROMETHEUS_URL}/api/v1/query`);
  u.searchParams.set("query", query);
  return fetchJson(u.toString());
}

async function prometheusRange(query, start, end, step) {
  const u = new URL(`${PROMETHEUS_URL}/api/v1/query_range`);
  u.searchParams.set("query", query);
  u.searchParams.set("start", String(start));
  u.searchParams.set("end", String(end));
  u.searchParams.set("step", String(step));
  return fetchJson(u.toString());
}

async function grafanaListDashboards(search) {
  const u = new URL(`${GRAFANA_URL}/api/search`);
  u.searchParams.set("type", "dash-db");
  if (search) u.searchParams.set("query", search);
  return fetchJson(u.toString(), { headers: { ...grafanaAuthHeader() } });
}

async function grafanaGetDashboard(uid) {
  const u = `${GRAFANA_URL}/api/dashboards/uid/${encodeURIComponent(uid)}`;
  return fetchJson(u, { headers: { ...grafanaAuthHeader() } });
}

/** Proxy PromQL through Grafana (same as Explore with datasource uid). */
async function grafanaPrometheusProxyInstant(query) {
  const u = new URL(`${GRAFANA_URL}/api/datasources/proxy/uid/${PROMETHEUS_DS_UID}/api/v1/query`);
  u.searchParams.set("query", query);
  return fetchJson(u.toString(), { headers: { ...grafanaAuthHeader() } });
}

async function grafanaPrometheusProxyRange(query, start, end, step) {
  const u = new URL(
    `${GRAFANA_URL}/api/datasources/proxy/uid/${PROMETHEUS_DS_UID}/api/v1/query_range`
  );
  u.searchParams.set("query", query);
  u.searchParams.set("start", String(start));
  u.searchParams.set("end", String(end));
  u.searchParams.set("step", String(step));
  return fetchJson(u.toString(), { headers: { ...grafanaAuthHeader() } });
}

async function tempoGetTrace(traceId) {
  const u = `${TEMPO_URL}/api/traces/${encodeURIComponent(traceId)}`;
  return fetchJson(u);
}

/** TraceQL tag search (Tempo 2.x). Example: { duration > 400ms } */
async function tempoSearchTraceQL(q, limit = 20) {
  const u = new URL(`${TEMPO_URL}/api/search`);
  u.searchParams.set("q", q);
  u.searchParams.set("limit", String(limit));
  return fetchJson(u.toString());
}

const server = new Server(
  { name: "grafana-observability-mcp", version: "1.0.0" },
  { capabilities: { tools: {} } }
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    {
      name: "prometheus_instant_query",
      description: "Run PromQL instant query against Prometheus (direct).",
      inputSchema: {
        type: "object",
        properties: {
          query: { type: "string", description: "PromQL expression" },
        },
        required: ["query"],
      },
    },
    {
      name: "prometheus_range_query",
      description: "Run PromQL range query against Prometheus (direct).",
      inputSchema: {
        type: "object",
        properties: {
          query: { type: "string" },
          start: { type: "string", description: "Unix timestamp or RFC3339" },
          end: { type: "string", description: "Unix timestamp or RFC3339" },
          step: { type: "string", description: "e.g. 15s", default: "15s" },
        },
        required: ["query", "start", "end"],
      },
    },
    {
      name: "grafana_prometheus_proxy_instant",
      description: "Run PromQL instant query via Grafana datasource proxy (uses GRAFANA_* auth).",
      inputSchema: {
        type: "object",
        properties: { query: { type: "string" } },
        required: ["query"],
      },
    },
    {
      name: "grafana_prometheus_proxy_range",
      description: "Run PromQL range query via Grafana datasource proxy.",
      inputSchema: {
        type: "object",
        properties: {
          query: { type: "string" },
          start: { type: "string" },
          end: { type: "string" },
          step: { type: "string", default: "15s" },
        },
        required: ["query", "start", "end"],
      },
    },
    {
      name: "grafana_list_dashboards",
      description: "List Grafana dashboards (api/search).",
      inputSchema: {
        type: "object",
        properties: {
          search: { type: "string", description: "Optional title search string" },
        },
      },
    },
    {
      name: "grafana_get_dashboard",
      description: "Fetch full dashboard JSON by uid.",
      inputSchema: {
        type: "object",
        properties: { uid: { type: "string" } },
        required: ["uid"],
      },
    },
    {
      name: "tempo_get_trace",
      description: "Fetch a single trace by trace ID from Tempo.",
      inputSchema: {
        type: "object",
        properties: { traceId: { type: "string" } },
        required: ["traceId"],
      },
    },
    {
      name: "tempo_search_traceql",
      description:
        "Search traces with TraceQL (Tempo /api/search?q=...). Example: { trace:duration > 400ms && resource.service.name = \"ecommerce-api\" }",
      inputSchema: {
        type: "object",
        properties: {
          q: { type: "string" },
          limit: { type: "number", default: 20 },
        },
        required: ["q"],
      },
    },
    {
      name: "latency_debug_bundle",
      description:
        "Prebuilt PromQL + TraceQL for /products slowness: p50/p95/p99, error rate, DB query duration, and slow trace search.",
      inputSchema: {
        type: "object",
        properties: {
          range_minutes: { type: "number", description: "Lookback window in minutes", default: 15 },
        },
      },
    },
  ],
}));

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const name = request.params.name;
  const args = request.params.arguments ?? {};

  try {
    switch (name) {
      case "prometheus_instant_query": {
        const data = await prometheusInstant(args.query);
        return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
      }
      case "prometheus_range_query": {
        const data = await prometheusRange(args.query, args.start, args.end, args.step || "15s");
        return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
      }
      case "grafana_prometheus_proxy_instant": {
        const data = await grafanaPrometheusProxyInstant(args.query);
        return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
      }
      case "grafana_prometheus_proxy_range": {
        const data = await grafanaPrometheusProxyRange(
          args.query,
          args.start,
          args.end,
          args.step || "15s"
        );
        return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
      }
      case "grafana_list_dashboards": {
        const data = await grafanaListDashboards(args.search);
        return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
      }
      case "grafana_get_dashboard": {
        const data = await grafanaGetDashboard(args.uid);
        return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
      }
      case "tempo_get_trace": {
        const data = await tempoGetTrace(args.traceId);
        return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
      }
      case "tempo_search_traceql": {
        const data = await tempoSearchTraceQL(args.q, args.limit ?? 20);
        return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
      }
      case "latency_debug_bundle": {
        const mins = Number(args.range_minutes) || 15;
        const end = Math.floor(Date.now() / 1000);
        const start = end - mins * 60;
        const step = "15s";
        const queries = {
          p95_latency_products: `histogram_quantile(0.95, sum(rate(ecommerce_http_request_duration_seconds_bucket{route="/products"}[5m])) by (le))`,
          p50_latency_products: `histogram_quantile(0.50, sum(rate(ecommerce_http_request_duration_seconds_bucket{route="/products"}[5m])) by (le))`,
          p99_latency_products: `histogram_quantile(0.99, sum(rate(ecommerce_http_request_duration_seconds_bucket{route="/products"}[5m])) by (le))`,
          rps_products: `sum(rate(ecommerce_http_requests_total{route="/products"}[1m]))`,
          error_ratio_products: `sum(rate(ecommerce_http_requests_total{route="/products",status=~"5..|4.."}[5m])) / clamp_min(sum(rate(ecommerce_http_requests_total{route="/products"}[5m])), 1)`,
          db_query_p95: `histogram_quantile(0.95, sum(rate(ecommerce_db_query_duration_seconds_bucket{query="select_products"}[5m])) by (le))`,
        };
        const results = {};
        for (const [k, q] of Object.entries(queries)) {
          try {
            results[k] = await prometheusRange(q, start, end, step);
          } catch (e) {
            results[k] = { error: String(e), body: e.body };
          }
        }
        let traceSearch = null;
        try {
          traceSearch = await tempoSearchTraceql(
            `{ trace:duration > 400ms && resource.service.name = "ecommerce-api" }`,
            15
          );
        } catch (e) {
          traceSearch = { error: String(e), body: e.body };
        }
        const bundle = { window: { start, end, step, minutes: mins }, prometheus: results, tempo_traceql: traceSearch };
        return { content: [{ type: "text", text: JSON.stringify(bundle, null, 2) }] };
      }
      default:
        return { content: [{ type: "text", text: `Unknown tool: ${name}` }], isError: true };
    }
  } catch (e) {
    const msg = e.body ? `${e.message}\n${JSON.stringify(e.body, null, 2)}` : e.message;
    return { content: [{ type: "text", text: msg }], isError: true };
  }
});

const transport = new StdioServerTransport();
await server.connect(transport);
