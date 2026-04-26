# backend-at-scale

Production-ready Go ecommerce backend with full local observability stack:
- API: Fiber + PostgreSQL (primary + read replicas) + Redis + Kafka
- Metrics: Prometheus
- Dashboards: Grafana (auto-provisioned)
- Load test: k6
- Kafka product events: async bounded queue (handlers do not block on `WriteMessages`)
- Kafka product writes: `POST /products` enqueues to Redis-backed queue (DB configurable), workers publish to Kafka, consumer persists to Postgres
- Kafka order writes: `POST /orders` uses a separate Redis queue and command topic; lifecycle topic advances order status asynchronously
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
   - Grafana: [http://localhost:3000](http://localhost:3000) — the **Ecommerce Observability** dashboard’s RPS / latency / error panels aggregate `GET|POST /products` and `/orders` routes (including `GET /orders/:request_id`). If you only load-test `/health` or scrape `/metrics`, those panels stay flat even though Prometheus is collecting data.
   - Postgres primary: `localhost:5432`, read replicas: `localhost:5433`, `localhost:5434`

## API

- `GET /products` — list (cached; reads round-robin across **replica** DSNs when `POSTGRES_REPLICA_URL` lists one or more URLs)
- `POST /products` — enqueue write command `{"name":"...","price":99.99}` and returns `202 Accepted` with `request_id`; request path uses bounded Redis queue (backpressure returns `503` when full), workers publish to Kafka, consumer writes to **primary** and invalidates list cache
- `POST /orders` — enqueue place-order command `{"product_id":1,"quantity":2}` and returns `202 Accepted` with `request_id`; uses a **separate** Redis queue (`REDIS_ORDER_QUEUE_KEY`) and Kafka topic (`KAFKA_ORDER_COMMANDS_TOPIC`) so order traffic does not contend with product creates. A consumer inserts the row (`waiting`), then the **lifecycle** topic (`KAFKA_ORDER_LIFECYCLE_TOPIC`) drives guarded transitions: `waiting` → `order_received` → `sent_for_shipping` → `completed`.
- `GET /orders/:request_id` — read order by `request_id` from the **read** pool (replicas when configured); `404` if unknown

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
- **Orders** — async `POST /orders` only:
  - `docker compose --profile loadtest run --rm k6 run /scripts/orders-place.js`
- **Orders mixed** — `POST /orders` plus `GET /orders/:request_id` (GET may return `404` until the place consumer has persisted the row; the k6 script marks `404` as an **expected** response so it does not inflate `http_req_failed`):
  - `docker compose --profile loadtest run --rm k6 run /scripts/orders-mixed.js`
- k6 scripts set a stable `name` tag and a `systemTags` whitelist **without** `url` (older k6 builds require `systemTags` as a string array, not `{ exclude: [...] }`) so unique `request_id` paths do not explode time-series cardinality.

## Performance at extreme RPS (e.g. one million per second)

Sustained **1M HTTP requests per second** is a **fleet-scale** outcome: many stateless API instances behind load balancing, Redis and Kafka clusters sized for your enqueue rate, and a write path that stays non-blocking (this repo returns `202` from `POST` without waiting on Postgres). A **single** container or single Postgres primary will hit a ceiling long before the Go runtime does. Use **horizontal scaling** (more app replicas, more Kafka partitions and consumers, read replicas for `GET /orders`), define **SLOs** (error rate and p99 latency under a chosen RPS), and measure capacity per replica before extrapolating. **k6** on one host is for regression and moderate ramps; validating megascale RPS needs a **distributed** load generator fleet and matching infrastructure.