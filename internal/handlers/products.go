package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"backend-at-scale/internal/config"
	appkafka "backend-at-scale/internal/kafka"
	"backend-at-scale/internal/observability"
	"backend-at-scale/internal/store"
	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

const productsListCacheKey = "products:list:v1"

// GET /products does Redis + replica read + optional Redis SET; a tight deadline cancels pgx mid-query,
// which shows on replicas as "could not send data to client: Broken pipe" / "connection to client lost".
const getProductsHandlerTimeout = 12 * time.Second

type createProductRequest struct {
	Name  string  `json:"name"`
	Price float64 `json:"price"`
}

type ProductHandler struct {
	cfg      config.Config
	postgres *store.PostgresStore
	redis    *redis.Client
	kafkaPub *appkafka.AsyncPublisher
	metrics  *observability.Metrics
}

func NewProductHandler(
	cfg config.Config,
	postgres *store.PostgresStore,
	redisClient *redis.Client,
	kafkaPub *appkafka.AsyncPublisher,
	metrics *observability.Metrics,
) *ProductHandler {
	return &ProductHandler{
		cfg:      cfg,
		postgres: postgres,
		redis:    redisClient,
		kafkaPub: kafkaPub,
		metrics:  metrics,
	}
}

func (h *ProductHandler) GetProducts(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.UserContext(), getProductsHandlerTimeout)
	defer cancel()
	ctx, span := otel.Tracer("ecommerce.handlers").Start(ctx, "products.get")
	defer span.End()

	_, cacheSpan := otel.Tracer("ecommerce.handlers").Start(ctx, "redis.get.products")
	cacheValue, err := h.redis.Get(ctx, productsListCacheKey).Result()
	cacheSpan.End()
	if err == nil {
		h.metrics.RedisCacheTotal.WithLabelValues(h.cfg.ServiceName, "get", "hit").Inc()
		h.kafkaPub.TryEnqueue(appkafka.Event{
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
		if setErr := h.redis.Set(setCtx, productsListCacheKey, payload, 30*time.Second).Err(); setErr != nil {
			setSpan.RecordError(setErr)
		}
		setSpan.End()
	}

	h.kafkaPub.TryEnqueue(appkafka.Event{
		Type:      "products.list.db_read",
		Route:     "/products",
		Timestamp: time.Now().UTC(),
	})

	return c.Status(fiber.StatusOK).JSON(products)
}

func (h *ProductHandler) CreateProduct(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.UserContext(), 5*time.Second)
	defer cancel()
	ctx, span := otel.Tracer("ecommerce.handlers").Start(ctx, "products.create")
	defer span.End()

	var req createProductRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid JSON body"})
	}

	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" || len(req.Name) > 200 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "name is required and must be at most 200 characters"})
	}
	if req.Price <= 0 || req.Price > 1_000_000 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "price must be greater than 0 and at most 1000000"})
	}

	p, err := h.postgres.InsertProduct(ctx, req.Name, req.Price)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "insert failed")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to create product"})
	}

	_ = h.redis.Del(ctx, productsListCacheKey).Err()

	h.kafkaPub.TryEnqueue(appkafka.Event{
		Type:      "products.created",
		Route:     "/products",
		Timestamp: time.Now().UTC(),
	})

	return c.Status(fiber.StatusCreated).JSON(p)
}
