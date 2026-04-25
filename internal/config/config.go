package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	ServiceName string
	AppPort     string
	Environment string

	PostgresURL        string
	PostgresReplicaURL string
	// Max open connections per pool (pgx). Keeps total clients under Postgres max_connections.
	PostgresPoolMaxConns     int
	PostgresReadPoolMaxConns int
	RedisAddr   string
	RedisPass   string
	RedisDB     int

	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroupID string
	KafkaAsyncQueueSize int
	KafkaAsyncWorkers   int

	OTELExporterEndpoint string
	OTELInsecure         bool
}

func Load() Config {
	return Config{
		ServiceName: getEnv("SERVICE_NAME", "ecommerce-api"),
		AppPort:     getEnv("APP_PORT", "8080"),
		Environment: getEnv("APP_ENV", "local"),
		PostgresURL: getEnv("POSTGRES_URL", "postgres://postgres:postgres@postgres:5432/ecommerce?sslmode=disable"),
		// Empty => use primary for reads as well (local `go run` without replica).
		PostgresReplicaURL: strings.TrimSpace(os.Getenv("POSTGRES_REPLICA_URL")),
		PostgresPoolMaxConns: clampInt(
			getEnvInt("POSTGRES_POOL_MAX_CONNS", 25),
			2,
			500,
		),
		PostgresReadPoolMaxConns: clampInt(
			getEnvInt("POSTGRES_READ_POOL_MAX_CONNS", 60),
			2,
			500,
		),
		RedisAddr:   getEnv("REDIS_ADDR", "redis:6379"),
		RedisPass:   getEnv("REDIS_PASSWORD", ""),
		RedisDB:     0,
		KafkaBrokers: splitAndTrim(
			getEnv("KAFKA_BROKERS", "kafka:9092"),
		),
		KafkaTopic:   getEnv("KAFKA_TOPIC", "product-events"),
		KafkaGroupID: getEnv("KAFKA_GROUP_ID", "ecommerce-observability"),
		KafkaAsyncQueueSize: clampInt(
			getEnvInt("KAFKA_ASYNC_QUEUE_SIZE", 16384),
			256,
			500_000,
		),
		KafkaAsyncWorkers: clampInt(getEnvInt("KAFKA_ASYNC_WORKERS", 2), 1, 32),
		OTELExporterEndpoint: getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "tempo:4317"),
		OTELInsecure:         getEnv("OTEL_EXPORTER_OTLP_INSECURE", "true") == "true",
	}
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func clampInt(v, min, max int) int {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func splitAndTrim(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}
