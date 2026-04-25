package app

import (
	"backend-at-scale/internal/config"
	"backend-at-scale/internal/handlers"
	"backend-at-scale/internal/middleware"
	"backend-at-scale/internal/observability"
	"backend-at-scale/internal/store"
	"github.com/gofiber/adaptor/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func NewServer(
	cfg config.Config,
	postgres *store.PostgresStore,
	redisClient *redis.Client,
	producer *kafka.Writer,
	metrics *observability.Metrics,
) *fiber.App {
	app := fiber.New()
	app.Use(middleware.PrometheusHTTP(cfg, metrics))

	productHandler := handlers.NewProductHandler(cfg, postgres, redisClient, producer, metrics)

	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": "ok"})
	})
	app.Get("/products", productHandler.GetProducts)
	app.Get("/metrics", adaptor.HTTPHandler(promhttp.Handler()))

	return app
}
