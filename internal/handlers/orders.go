package handlers

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"backend-at-scale/internal/config"
	appkafka "backend-at-scale/internal/kafka"
	"backend-at-scale/internal/observability"
	"backend-at-scale/internal/store"
	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

type createOrderRequest struct {
	ProductID int64 `json:"product_id"`
	Quantity  int   `json:"quantity"`
}

type OrderHandler struct {
	cfg           config.Config
	postgres      *store.PostgresStore
	orderQueue    *appkafka.JSONQueueProducer
	metrics       *observability.Metrics
	kafkaEvents   *appkafka.AsyncPublisher
}

func NewOrderHandler(
	cfg config.Config,
	postgres *store.PostgresStore,
	orderQueue *appkafka.JSONQueueProducer,
	kafkaEvents *appkafka.AsyncPublisher,
	metrics *observability.Metrics,
) *OrderHandler {
	return &OrderHandler{
		cfg:         cfg,
		postgres:    postgres,
		orderQueue:  orderQueue,
		metrics:     metrics,
		kafkaEvents: kafkaEvents,
	}
}

func (h *OrderHandler) CreateOrder(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.UserContext(), 5*time.Second)
	defer cancel()
	ctx, span := otel.Tracer("ecommerce.handlers").Start(ctx, "orders.create")
	defer span.End()

	var req createOrderRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid JSON body"})
	}
	if req.ProductID <= 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "product_id must be positive"})
	}
	if req.Quantity <= 0 || req.Quantity > 100_000 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "quantity must be between 1 and 100000"})
	}

	requestID := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	cmd := appkafka.PlaceOrderCommand{
		RequestID: requestID,
		ProductID: req.ProductID,
		Quantity:  req.Quantity,
		Timestamp: time.Now().UTC(),
	}
	body, err := json.Marshal(cmd)
	if err != nil {
		span.SetStatus(codes.Error, "marshal failed")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to encode order"})
	}

	ok := h.orderQueue.TryEnqueue([]byte(requestID), body)
	if !ok {
		span.SetStatus(codes.Error, "command queue full")
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "failed to enqueue order"})
	}

	h.kafkaEvents.TryEnqueue(appkafka.Event{
		Type:      "orders.create.enqueued",
		Route:     "/orders",
		Timestamp: time.Now().UTC(),
	})

	return c.Status(fiber.StatusAccepted).JSON(fiber.Map{
		"request_id": requestID,
		"status":     "accepted",
	})
}

func (h *OrderHandler) GetOrder(c *fiber.Ctx) error {
	requestID := strings.TrimSpace(c.Params("request_id"))
	if requestID == "" || len(requestID) > 200 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request_id"})
	}

	timeout := time.Duration(h.cfg.GetProductsTimeoutMS) * time.Millisecond
	ctx, cancel := context.WithTimeout(c.UserContext(), timeout)
	defer cancel()
	ctx, span := otel.Tracer("ecommerce.handlers").Start(ctx, "orders.get")
	defer span.End()
	span.SetAttributes(attribute.String("order.request_id", requestID))

	o, err := h.postgres.GetOrderByRequestID(ctx, requestID)
	if err != nil {
		if err == pgx.ErrNoRows {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "order not found"})
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, "query failed")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to fetch order"})
	}

	return c.Status(fiber.StatusOK).JSON(o)
}
