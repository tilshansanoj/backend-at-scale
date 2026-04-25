package config

import (
	"os"
	"strings"
)

type Config struct {
	ServiceName string
	AppPort     string

	PostgresURL string
	RedisAddr   string
	RedisPass   string
	RedisDB     int

	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroupID string
}

func Load() Config {
	return Config{
		ServiceName: getEnv("SERVICE_NAME", "ecommerce-api"),
		AppPort:     getEnv("APP_PORT", "8080"),
		PostgresURL: getEnv("POSTGRES_URL", "postgres://postgres:postgres@postgres:5432/ecommerce?sslmode=disable"),
		RedisAddr:   getEnv("REDIS_ADDR", "redis:6379"),
		RedisPass:   getEnv("REDIS_PASSWORD", ""),
		RedisDB:     0,
		KafkaBrokers: splitAndTrim(
			getEnv("KAFKA_BROKERS", "kafka:9092"),
		),
		KafkaTopic:   getEnv("KAFKA_TOPIC", "product-events"),
		KafkaGroupID: getEnv("KAFKA_GROUP_ID", "ecommerce-observability"),
	}
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
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
