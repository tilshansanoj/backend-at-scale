package handlers

import (
	"context"
	"encoding/json"
	"time"

	"backend-at-scale/internal/config"
	"backend-at-scale/internal/kafka"
	"backend-at-scale/internal/observability"
	"backend-at-scale/internal/store"
	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type ProductHandler struct {
	cfg      config.Config
	postgres *store.PostgresStore
	redis    *redis.Client
	producer *kafka.Writer
	metrics  *observability.Metrics
}

func NewProductHandler(
	cfg config.Config,
	postgres *store.PostgresStore,
	redisClient *redis.Client,
	producer *kafka.Writer,
	metrics *observability.Metrics,
) *ProductHandler {
	return &ProductHandler{
		cfg:      cfg,
		postgres: postgres,
		redis:    redisClient,
		producer: producer,
		metrics:  metrics,
	}
}

func (h *ProductHandler) GetProducts(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.UserContext(), 3*time.Second)
	defer cancel()

	const cacheKey = "products:list:v1"

	cacheValue, err := h.redis.Get(ctx, cacheKey).Result()
	if err == nil {
		h.metrics.RedisCacheTotal.WithLabelValues(h.cfg.ServiceName, "get", "hit").Inc()
		kafka.Publish(ctx, h.producer, h.cfg, h.metrics, kafka.Event{
			Type:      "products.list.cache_hit",
			Route:     "/products",
			Timestamp: time.Now().UTC(),
		})

		var products []store.Product
		if unmarshalErr := json.Unmarshal([]byte(cacheValue), &products); unmarshalErr == nil {
			return c.Status(fiber.StatusOK).JSON(products)
		}
	} else {
		h.metrics.RedisCacheTotal.WithLabelValues(h.cfg.ServiceName, "get", "miss").Inc()
	}

	products, err := h.postgres.GetProducts(ctx)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to fetch products"})
	}

	payload, err := json.Marshal(products)
	if err == nil {
		_ = h.redis.Set(ctx, cacheKey, payload, 30*time.Second).Err()
	}

	kafka.Publish(ctx, h.producer, h.cfg, h.metrics, kafka.Event{
		Type:      "products.list.db_read",
		Route:     "/products",
		Timestamp: time.Now().UTC(),
	})

	return c.Status(fiber.StatusOK).JSON(products)
}
