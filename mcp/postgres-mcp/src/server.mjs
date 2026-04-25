#!/usr/bin/env node
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import pg from "pg";

const { Pool } = pg;

function env(name, fallback = "") {
  const v = process.env[name];
  return v != null && v !== "" ? v : fallback;
}

const connectionString = env(
  "DATABASE_URL",
  env("POSTGRES_URL", "postgres://postgres:postgres@127.0.0.1:5432/ecommerce")
);
const allowInsert = env("POSTGRES_MCP_ALLOW_INSERT", "false") === "true";
const defaultMaxRows = Math.min(Math.max(parseInt(env("POSTGRES_MCP_MAX_ROWS", "500"), 1), 5000), 5000);

const pool = new Pool({ connectionString, max: 4 });

function isReadOnlySQL(sql) {
  const s = sql.trim().replace(/^\ufeff/, "");
  if (!s) return false;
  const upper = s.toUpperCase();
  return (
    upper.startsWith("SELECT") ||
    upper.startsWith("WITH") ||
    upper.startsWith("EXPLAIN") ||
    upper.startsWith("SHOW") ||
    upper.startsWith("TABLE ")
  );
}

const server = new Server(
  { name: "postgres-mcp", version: "1.0.0" },
  { capabilities: { tools: {} } }
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    {
      name: "list_tables",
      description: "List tables in a schema (default: public).",
      inputSchema: {
        type: "object",
        properties: {
          schema: { type: "string", description: "Schema name", default: "public" },
        },
      },
    },
    {
      name: "describe_table",
      description: "Show columns for a table (information_schema).",
      inputSchema: {
        type: "object",
        properties: {
          table: { type: "string" },
          schema: { type: "string", default: "public" },
        },
        required: ["table"],
      },
    },
    {
      name: "select_readonly",
      description:
        "Run a single read-only SQL statement (must start with SELECT, WITH, EXPLAIN, SHOW, or TABLE). Semicolons are rejected.",
      inputSchema: {
        type: "object",
        properties: {
          sql: { type: "string" },
          max_rows: { type: "number", description: "Max rows returned", default: defaultMaxRows },
        },
        required: ["sql"],
      },
    },
    ...(allowInsert
      ? [
          {
            name: "insert_product",
            description: "Insert one row into products (same as API). Requires POSTGRES_MCP_ALLOW_INSERT=true.",
            inputSchema: {
              type: "object",
              properties: {
                name: { type: "string" },
                price: { type: "number" },
              },
              required: ["name", "price"],
            },
          },
        ]
      : []),
  ],
}));

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const name = request.params.name;
  const args = request.params.arguments ?? {};

  try {
    switch (name) {
      case "list_tables": {
        const schema = args.schema || "public";
        const r = await pool.query(
          `SELECT table_name
           FROM information_schema.tables
           WHERE table_schema = $1 AND table_type = 'BASE TABLE'
           ORDER BY table_name`,
          [schema]
        );
        return { content: [{ type: "text", text: JSON.stringify(r.rows, null, 2) }] };
      }
      case "describe_table": {
        const schema = args.schema || "public";
        const table = args.table;
        const r = await pool.query(
          `SELECT column_name, data_type, is_nullable, column_default
           FROM information_schema.columns
           WHERE table_schema = $1 AND table_name = $2
           ORDER BY ordinal_position`,
          [schema, table]
        );
        return { content: [{ type: "text", text: JSON.stringify(r.rows, null, 2) }] };
      }
      case "select_readonly": {
        const sql = String(args.sql || "");
        if (sql.includes(";")) {
          return { content: [{ type: "text", text: "Semicolons are not allowed." }], isError: true };
        }
        if (!isReadOnlySQL(sql)) {
          return {
            content: [
              {
                type: "text",
                text: "Only read-only queries are allowed (SELECT / WITH / EXPLAIN / SHOW / TABLE …).",
              },
            ],
            isError: true,
          };
        }
        const maxRows = Math.min(
          Math.max(parseInt(args.max_rows ?? defaultMaxRows, 10) || defaultMaxRows, 1),
          5000
        );
        const upper = sql.trim().toUpperCase();
        let r;
        if (upper.startsWith("SHOW") || upper.startsWith("EXPLAIN")) {
          r = await pool.query(sql);
        } else {
          r = await pool.query(`SELECT * FROM (${sql}) AS _mcp_sub LIMIT ${maxRows}`);
        }
        return {
          content: [
            {
              type: "text",
              text: JSON.stringify({ rowCount: r.rowCount, rows: r.rows }, null, 2),
            },
          ],
        };
      }
      case "insert_product": {
        if (!allowInsert) {
          return { content: [{ type: "text", text: "insert_product is disabled." }], isError: true };
        }
        const pname = String(args.name || "").trim();
        const price = Number(args.price);
        if (!pname || pname.length > 200 || !Number.isFinite(price) || price <= 0) {
          return { content: [{ type: "text", text: "Invalid name or price." }], isError: true };
        }
        const r = await pool.query(
          `INSERT INTO products (name, price) VALUES ($1, $2) RETURNING id, name, price`,
          [pname, price]
        );
        return { content: [{ type: "text", text: JSON.stringify(r.rows[0], null, 2) }] };
      }
      default:
        return { content: [{ type: "text", text: `Unknown tool: ${name}` }], isError: true };
    }
  } catch (e) {
    return { content: [{ type: "text", text: String(e.message || e) }], isError: true };
  }
});

const transport = new StdioServerTransport();
await server.connect(transport);
