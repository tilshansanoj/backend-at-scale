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
	GetProductsTimeoutMS int
	ProductsCacheTTLSeconds int

	PostgresURL string
	// Non-empty DSNs from POSTGRES_REPLICA_URL (comma-separated). Empty => reads use primary only.
	PostgresReplicaURLs []string
	// Max open connections per pool (pgx). Keeps total clients under Postgres max_connections.
	PostgresPoolMaxConns     int
	PostgresReadPoolMaxConns int
	RedisAddr   string
	RedisPass   string
	RedisDB     int
	RedisPoolSize      int
	RedisMinIdleConns  int

	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroupID string
	KafkaCommandsTopic   string
	KafkaCommandsGroupID string
	KafkaAsyncQueueSize int
	KafkaAsyncWorkers   int

	OTELExporterEndpoint string
	OTELInsecure         bool
	// 0..1; lower reduces tracing overhead under load (spans still created but mostly dropped).
	OTELTraceSampleRatio float64

	FiberPrefork bool
}

func Load() Config {
	return Config{
		ServiceName: getEnv("SERVICE_NAME", "ecommerce-api"),
		AppPort:     getEnv("APP_PORT", "8080"),
		Environment: getEnv("APP_ENV", "local"),
		GetProductsTimeoutMS: clampInt(
			getEnvInt("GET_PRODUCTS_TIMEOUT_MS", 12000),
			1000,
			120000,
		),
		ProductsCacheTTLSeconds: clampInt(
			getEnvInt("PRODUCTS_CACHE_TTL_SECONDS", 120),
			5,
			3600,
		),
		PostgresURL:         getEnv("POSTGRES_URL", "postgres://postgres:postgres@postgres:5432/ecommerce?sslmode=disable"),
		PostgresReplicaURLs: splitAndTrim(os.Getenv("POSTGRES_REPLICA_URL")),
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
		RedisPoolSize: clampInt(
			getEnvInt("REDIS_POOL_SIZE", 128),
			10,
			500,
		),
		RedisMinIdleConns: clampInt(
			getEnvInt("REDIS_MIN_IDLE_CONNS", 32),
			0,
			200,
		),
		KafkaBrokers: splitAndTrim(
			getEnv("KAFKA_BROKERS", "kafka:9092"),
		),
		KafkaTopic:   getEnv("KAFKA_TOPIC", "product-events"),
		KafkaGroupID: getEnv("KAFKA_GROUP_ID", "ecommerce-observability"),
		KafkaCommandsTopic: getEnv("KAFKA_COMMANDS_TOPIC", "product-create-commands"),
		KafkaCommandsGroupID: getEnv("KAFKA_COMMANDS_GROUP_ID", "ecommerce-product-writes"),
		KafkaAsyncQueueSize: clampInt(
			getEnvInt("KAFKA_ASYNC_QUEUE_SIZE", 16384),
			256,
			500_000,
		),
		KafkaAsyncWorkers: clampInt(getEnvInt("KAFKA_ASYNC_WORKERS", 2), 1, 32),
		OTELExporterEndpoint: getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "tempo:4317"),
		OTELInsecure:         getEnv("OTEL_EXPORTER_OTLP_INSECURE", "true") == "true",
		OTELTraceSampleRatio: clampFloat(
			getEnvFloat("OTEL_TRACE_SAMPLE_RATIO", 1.0),
			0,
			1,
		),
		FiberPrefork: getEnv("FIBER_PREFORK", "") == "true",
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

func getEnvFloat(key string, fallback float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return fallback
	}
	return n
}

func clampFloat(v, min, max float64) float64 {
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
