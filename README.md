# backend-at-scale

Production-ready Go ecommerce backend with full local observability stack:
- API: Fiber + PostgreSQL (primary + read replicas) + Redis + Kafka
- Metrics: Prometheus
- Dashboards: Grafana (auto-provisioned)
- Load test: k6
- Kafka product events: async bounded queue (handlers do not block on `WriteMessages`)
- Kafka product writes: `POST /products` enqueues to Redis-backed queue (DB configurable), workers publish to Kafka, consumer persists to Postgres
- MCP (optional): Grafana + Prometheus — [mcp/grafana-mcp/README.md](mcp/grafana-mcp/README.md); Postgres — [mcp/postgres-mcp/README.md](mcp/postgres-mcp/README.md)

## Quick start

1. Copy env file:
   - `cp .env.example .env` (or create `.env` manually on Windows)
2. (Optional) Fresh DB bootstrap with generated dataset:
   - `docker compose down -v` to remove existing Postgres **and replica** volumes
   - next startup initializes ~1,000,000 `products` rows on the **primary**, then each **read replica** clones via `pg_basebackup` (first start of each replica can take several minutes)
3. Start full stack:
   - `docker compose up --build`
   - If a replica logs `requested WAL segment ... has already been removed`, the primary recycled WAL before catch-up: keep `POSTGRES_WAL_KEEP_SIZE` (default `2GB` in compose), then remove that replica’s Docker volume and recreate the container so it runs `pg_basebackup` again.
   - If you see **`replication terminated by primary server`**, **`server closed the connection unexpectedly`**, or **`invalid record length ... got 0`** on a replica, the **primary** usually restarted, crashed (check Docker / OOM), or was stopped with too little grace time. Compose sets **`shm_size`** and **`stop_grace_period`** on Postgres to reduce that; check **`docker logs ecommerce-postgres`** at the same timestamp. Replicas often **reconnect** a few seconds later (`started streaming WAL from primary` again).
4. Open:
   - API: [http://localhost:8080](http://localhost:8080)
   - Metrics: [http://localhost:8080/metrics](http://localhost:8080/metrics)
   - Prometheus: [http://localhost:9090](http://localhost:9090)
   - Grafana: [http://localhost:3000](http://localhost:3000)
   - Postgres primary: `localhost:5432`, read replicas: `localhost:5433`, `localhost:5434`

## API

- `GET /products` — list (cached; reads round-robin across **replica** DSNs when `POSTGRES_REPLICA_URL` lists one or more URLs)
- `POST /products` — enqueue write command `{"name":"...","price":99.99}` and returns `202 Accepted` with `request_id`; request path uses bounded Redis queue (backpressure returns `503` when full), workers publish to Kafka, consumer writes to **primary** and invalidates list cache

## Run load test

- One-off from host:
  - `docker compose --profile loadtest run --rm k6`
- Or with custom target:
  - `docker compose --profile loadtest run --rm -e BASE_URL=http://app:8080 k6`
- Mixed **GET + POST** (≈12% inserts):
  - `docker compose --profile loadtest run --rm k6 run /scripts/products-mixed.js`
- **Spike** (steady → sharp RPS ramp → hold → recover; same traffic mix):
  - `docker compose --profile loadtest run --rm k6 run /scripts/products-spike.js`
- **Stress** (stepped RPS plateaus → sustained peak → cool down; same traffic mix):
  - `docker compose --profile loadtest run --rm k6 run /scripts/products-stress.js`