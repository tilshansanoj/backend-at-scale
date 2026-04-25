# backend-at-scale

Production-ready Go ecommerce backend with full local observability stack:
- API: Fiber + PostgreSQL + Redis + Kafka
- Metrics: Prometheus
- Traces: OpenTelemetry + Tempo
- Dashboards: Grafana (auto-provisioned)
- Load test: k6

## Quick start

1. Copy env file:
   - `cp .env.example .env` (or create `.env` manually on Windows)
2. (Optional) Fresh DB bootstrap with generated dataset:
   - `docker compose down -v` to remove existing Postgres volume
   - next startup initializes ~1,000,000 `products` rows
3. Start full stack:
   - `docker compose up --build`
4. Open:
   - API: [http://localhost:8080](http://localhost:8080)
   - Metrics: [http://localhost:8080/metrics](http://localhost:8080/metrics)
   - Prometheus: [http://localhost:9090](http://localhost:9090)
   - Grafana: [http://localhost:3000](http://localhost:3000)
   - Tempo API: [http://localhost:3200](http://localhost:3200)

## Run load test

- One-off from host:
  - `docker compose --profile loadtest run --rm k6`
- Or with custom target:
  - `docker compose --profile loadtest run --rm -e BASE_URL=http://app:8080 k6`