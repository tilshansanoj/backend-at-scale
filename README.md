# backend-at-scale

Production-ready Go ecommerce backend with full local observability stack:
- API: Fiber + PostgreSQL (primary + read replica) + Redis + Kafka
- Metrics: Prometheus
- Traces: OpenTelemetry + Tempo
- Dashboards: Grafana (auto-provisioned)
- Load test: k6
- Kafka product events: async bounded queue (handlers do not block on `WriteMessages`)
- MCP (optional): Grafana + Prometheus + Tempo — [mcp/grafana-mcp/README.md](mcp/grafana-mcp/README.md); Postgres — [mcp/postgres-mcp/README.md](mcp/postgres-mcp/README.md)

## Quick start

1. Copy env file:
   - `cp .env.example .env` (or create `.env` manually on Windows)
2. (Optional) Fresh DB bootstrap with generated dataset:
   - `docker compose down -v` to remove existing Postgres **and replica** volumes
   - next startup initializes ~1,000,000 `products` rows on the **primary**, then the **replica** clones via streaming base backup (first replica start can take several minutes)
3. Start full stack:
   - `docker compose up --build`
4. Open:
   - API: [http://localhost:8080](http://localhost:8080)
   - Metrics: [http://localhost:8080/metrics](http://localhost:8080/metrics)
   - Prometheus: [http://localhost:9090](http://localhost:9090)
   - Grafana: [http://localhost:3000](http://localhost:3000)
   - Tempo API: [http://localhost:3200](http://localhost:3200)
   - Postgres primary: `localhost:5432`, read replica: `localhost:5433`

## API

- `GET /products` — list (cached; reads go to **replica** when `POSTGRES_REPLICA_URL` is set in compose)
- `POST /products` — insert `{"name":"...","price":99.99}` (writes go to **primary**; list cache invalidated; briefly **eventual consistency** on replica for `GET` until replication catches up)

## Run load test

- One-off from host:
  - `docker compose --profile loadtest run --rm k6`
- Or with custom target:
  - `docker compose --profile loadtest run --rm -e BASE_URL=http://app:8080 k6`
- Mixed **GET + POST** (≈12% inserts):
  - `docker compose --profile loadtest run --rm k6 run /scripts/products-mixed.js`