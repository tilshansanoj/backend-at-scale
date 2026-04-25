package app

import (
	"backend-at-scale/internal/config"
	"backend-at-scale/internal/handlers"
	"backend-at-scale/internal/kafka"
	"backend-at-scale/internal/middleware"
	"backend-at-scale/internal/observability"
	"log"
	"os"
	otelfiber "github.com/gofiber/contrib/otelfiber"
	"github.com/prometheus/client_golang/prometheus"
	"backend-at-scale/internal/store"
	"github.com/gofiber/adaptor/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

func NewServer(
	cfg config.Config,
	postgres *store.PostgresStore,
	redisClient *redis.Client,
	kafkaPub *kafka.AsyncPublisher,
	metrics *observability.Metrics,
) *fiber.App {
	app := fiber.New()
	app.Use(otelfiber.Middleware())
	app.Use(middleware.PrometheusHTTP(cfg, metrics))

	productHandler := handlers.NewProductHandler(cfg, postgres, redisClient, kafkaPub, metrics)

	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": "ok"})
	})
	app.Get("/products", productHandler.GetProducts)
	app.Post("/products", productHandler.CreateProduct)
	app.Get("/metrics", adaptor.HTTPHandler(promhttp.HandlerFor(
		prometheus.DefaultGatherer,
		promhttp.HandlerOpts{
			ErrorHandling: promhttp.ContinueOnError,
			ErrorLog:      log.New(os.Stderr, "promhttp: ", log.LstdFlags),
		},
	)))

	return app
}
