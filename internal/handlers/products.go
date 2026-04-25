package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"backend-at-scale/internal/config"
	appkafka "backend-at-scale/internal/kafka"
	"backend-at-scale/internal/observability"
	"backend-at-scale/internal/store"
	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

type ProductHandler struct {
	cfg      config.Config
	postgres *store.PostgresStore
	redis    *redis.Client
	producer *kafkago.Writer
	metrics  *observability.Metrics
}

func NewProductHandler(
	cfg config.Config,
	postgres *store.PostgresStore,
	redisClient *redis.Client,
	producer *kafkago.Writer,
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
	ctx, span := otel.Tracer("ecommerce.handlers").Start(ctx, "products.get")
	defer span.End()

	const cacheKey = "products:list:v1"

	_, cacheSpan := otel.Tracer("ecommerce.handlers").Start(ctx, "redis.get.products")
	cacheValue, err := h.redis.Get(ctx, cacheKey).Result()
	cacheSpan.End()
	if err == nil {
		h.metrics.RedisCacheTotal.WithLabelValues(h.cfg.ServiceName, "get", "hit").Inc()
		appkafka.Publish(ctx, h.producer, h.cfg, h.metrics, appkafka.Event{
			Type:      "products.list.cache_hit",
			Route:     "/products",
			Timestamp: time.Now().UTC(),
		})

		var products []store.Product
		if unmarshalErr := json.Unmarshal([]byte(cacheValue), &products); unmarshalErr == nil {
			span.SetAttributes(attribute.String("cache.result", "hit"))
			return c.Status(fiber.StatusOK).JSON(products)
		}
	} else {
		if !errors.Is(err, redis.Nil) {
			span.RecordError(err)
			span.SetStatus(codes.Error, "redis get failed")
		}
		h.metrics.RedisCacheTotal.WithLabelValues(h.cfg.ServiceName, "get", "miss").Inc()
		span.SetAttributes(attribute.String("cache.result", "miss"))
	}

	products, err := h.postgres.GetProducts(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "postgres query failed")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to fetch products"})
	}

	payload, err := json.Marshal(products)
	if err == nil {
		setCtx, setSpan := otel.Tracer("ecommerce.handlers").Start(ctx, "redis.set.products")
		if setErr := h.redis.Set(setCtx, cacheKey, payload, 30*time.Second).Err(); setErr != nil {
			setSpan.RecordError(setErr)
		}
		setSpan.End()
	}

	appkafka.Publish(ctx, h.producer, h.cfg, h.metrics, appkafka.Event{
		Type:      "products.list.db_read",
		Route:     "/products",
		Timestamp: time.Now().UTC(),
	})

	return c.Status(fiber.StatusOK).JSON(products)
}
