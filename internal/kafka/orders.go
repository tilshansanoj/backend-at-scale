package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"backend-at-scale/internal/config"
	"backend-at-scale/internal/observability"
	"backend-at-scale/internal/store"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// PlaceOrderCommand is the Kafka payload for async order placement (same shape as Redis queue value before bridging).
type PlaceOrderCommand struct {
	RequestID string    `json:"request_id"`
	ProductID int64     `json:"product_id"`
	Quantity  int       `json:"quantity"`
	Timestamp time.Time `json:"timestamp"`
}

// OrderLifecycleStep advances order status in one hop (compare-and-swap on "from").
type OrderLifecycleStep struct {
	OrderID int64  `json:"order_id"`
	From    string `json:"from"`
	To      string `json:"to"`
}

// nextStatusAfter returns the next lifecycle target when the row has just reached status `at`.
func nextStatusAfter(at string) string {
	switch at {
	case store.OrderStatusOrderReceived:
		return store.OrderStatusSentForShipping
	case store.OrderStatusSentForShipping:
		return store.OrderStatusCompleted
	default:
		return ""
	}
}

func publishOrderLifecycleStep(ctx context.Context, w *kafka.Writer, cfg config.Config, metrics *observability.Metrics, step OrderLifecycleStep) bool {
	topic := w.Topic
	body, err := json.Marshal(step)
	if err != nil {
		metrics.KafkaPublish.WithLabelValues(cfg.ServiceName, topic, "error").Inc()
		return false
	}
	key := []byte(strconv.FormatInt(step.OrderID, 10))
	msg := kafka.Message{
		Key:   key,
		Value: body,
		Time:  time.Now().UTC(),
	}
	if err := w.WriteMessages(ctx, msg); err != nil {
		metrics.KafkaPublish.WithLabelValues(cfg.ServiceName, topic, "error").Inc()
		return false
	}
	metrics.KafkaPublish.WithLabelValues(cfg.ServiceName, topic, "success").Inc()
	return true
}

func isFKViolation(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "23503"
}

// StartOrderPlaceConsumer persists place commands and kicks off the lifecycle topic.
func StartOrderPlaceConsumer(
	ctx context.Context,
	reader *kafka.Reader,
	postgres *store.PostgresStore,
	lifecycleWriter *kafka.Writer,
	metrics *observability.Metrics,
	cfg config.Config,
) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			metrics.KafkaConsume.WithLabelValues(cfg.ServiceName, cfg.KafkaOrderCommandsTopic, "error").Inc()
			time.Sleep(250 * time.Millisecond)
			continue
		}

		consumeCtx, span := otel.Tracer("ecommerce.kafka").Start(ctx, "kafka.consume.order_place")
		span.SetAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination", cfg.KafkaOrderCommandsTopic),
			attribute.Int64("messaging.kafka.partition", int64(msg.Partition)),
			attribute.Int64("messaging.kafka.offset", msg.Offset),
		)

		var cmd PlaceOrderCommand
		if err := json.Unmarshal(msg.Value, &cmd); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "decode command failed")
			metrics.KafkaConsume.WithLabelValues(cfg.ServiceName, cfg.KafkaOrderCommandsTopic, "decode_error").Inc()
			_ = reader.CommitMessages(consumeCtx, msg)
			span.End()
			continue
		}
		cmd.RequestID = strings.TrimSpace(cmd.RequestID)
		if cmd.RequestID == "" || len(cmd.RequestID) > 200 || cmd.ProductID <= 0 || cmd.Quantity <= 0 || cmd.Quantity > 100_000 {
			metrics.KafkaConsume.WithLabelValues(cfg.ServiceName, cfg.KafkaOrderCommandsTopic, "invalid").Inc()
			_ = reader.CommitMessages(consumeCtx, msg)
			span.End()
			continue
		}

		writeCtx, writeCancel := context.WithTimeout(consumeCtx, 5*time.Second)
		o, inserted, err := postgres.InsertOrderWaitingIfAbsent(writeCtx, cmd.RequestID, cmd.ProductID, cmd.Quantity)
		writeCancel()
		if err != nil {
			if isFKViolation(err) {
				metrics.KafkaConsume.WithLabelValues(cfg.ServiceName, cfg.KafkaOrderCommandsTopic, "invalid").Inc()
				_ = reader.CommitMessages(consumeCtx, msg)
				span.End()
				continue
			}
			span.RecordError(err)
			span.SetStatus(codes.Error, "insert failed")
			metrics.KafkaConsume.WithLabelValues(cfg.ServiceName, cfg.KafkaOrderCommandsTopic, "insert_error").Inc()
			span.End()
			time.Sleep(250 * time.Millisecond)
			continue
		}

		needKick := inserted || o.Status == store.OrderStatusWaiting
		if needKick {
			pubCtx, pubCancel := context.WithTimeout(consumeCtx, 10*time.Second)
			ok := publishOrderLifecycleStep(pubCtx, lifecycleWriter, cfg, metrics, OrderLifecycleStep{
				OrderID: o.ID,
				From:    store.OrderStatusWaiting,
				To:      store.OrderStatusOrderReceived,
			})
			pubCancel()
			if !ok {
				span.SetStatus(codes.Error, "lifecycle publish failed")
				metrics.KafkaConsume.WithLabelValues(cfg.ServiceName, cfg.KafkaOrderCommandsTopic, "lifecycle_publish_error").Inc()
				span.End()
				time.Sleep(250 * time.Millisecond)
				continue
			}
		}

		if commitErr := reader.CommitMessages(consumeCtx, msg); commitErr != nil {
			span.RecordError(commitErr)
			span.SetStatus(codes.Error, "kafka commit failed")
			metrics.KafkaConsume.WithLabelValues(cfg.ServiceName, cfg.KafkaOrderCommandsTopic, "commit_error").Inc()
			span.End()
			continue
		}
		metrics.KafkaConsume.WithLabelValues(cfg.ServiceName, cfg.KafkaOrderCommandsTopic, "success").Inc()

		lag := msg.HighWaterMark - msg.Offset - 1
		if lag < 0 {
			lag = 0
		}
		metrics.KafkaLagGauge.WithLabelValues(
			cfg.ServiceName,
			cfg.KafkaOrderCommandsTopic,
			strconv.Itoa(int(msg.Partition)),
			cfg.KafkaOrderCommandsGroupID,
		).Set(float64(lag))
		span.SetAttributes(attribute.Int64("messaging.kafka.consumer.lag", lag))
		span.End()
	}
}

// StartOrderLifecycleConsumer applies guarded status updates and chains the next step on the lifecycle topic.
func StartOrderLifecycleConsumer(
	ctx context.Context,
	reader *kafka.Reader,
	postgres *store.PostgresStore,
	lifecycleWriter *kafka.Writer,
	metrics *observability.Metrics,
	cfg config.Config,
) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			metrics.KafkaConsume.WithLabelValues(cfg.ServiceName, cfg.KafkaOrderLifecycleTopic, "error").Inc()
			time.Sleep(250 * time.Millisecond)
			continue
		}

		consumeCtx, span := otel.Tracer("ecommerce.kafka").Start(ctx, "kafka.consume.order_lifecycle")
		span.SetAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination", cfg.KafkaOrderLifecycleTopic),
			attribute.Int64("messaging.kafka.partition", int64(msg.Partition)),
			attribute.Int64("messaging.kafka.offset", msg.Offset),
		)

		var step OrderLifecycleStep
		if err := json.Unmarshal(msg.Value, &step); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "decode lifecycle failed")
			metrics.KafkaConsume.WithLabelValues(cfg.ServiceName, cfg.KafkaOrderLifecycleTopic, "decode_error").Inc()
			_ = reader.CommitMessages(consumeCtx, msg)
			span.End()
			continue
		}
		if step.OrderID <= 0 || step.From == "" || step.To == "" {
			metrics.KafkaConsume.WithLabelValues(cfg.ServiceName, cfg.KafkaOrderLifecycleTopic, "invalid").Inc()
			_ = reader.CommitMessages(consumeCtx, msg)
			span.End()
			continue
		}

		writeCtx, writeCancel := context.WithTimeout(consumeCtx, 5*time.Second)
		ok, err := postgres.AdvanceOrderStatus(writeCtx, step.OrderID, step.From, step.To)
		writeCancel()
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "advance failed")
			metrics.KafkaConsume.WithLabelValues(cfg.ServiceName, cfg.KafkaOrderLifecycleTopic, "update_error").Inc()
			span.End()
			time.Sleep(250 * time.Millisecond)
			continue
		}
		if !ok {
			metrics.KafkaConsume.WithLabelValues(cfg.ServiceName, cfg.KafkaOrderLifecycleTopic, "noop").Inc()
		}

		readCtx, readCancel := context.WithTimeout(consumeCtx, 3*time.Second)
		cur, err := postgres.GetOrderStatusByID(readCtx, step.OrderID)
		readCancel()
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				metrics.KafkaConsume.WithLabelValues(cfg.ServiceName, cfg.KafkaOrderLifecycleTopic, "invalid").Inc()
				_ = reader.CommitMessages(consumeCtx, msg)
				span.End()
				continue
			}
			span.RecordError(err)
			span.SetStatus(codes.Error, "read status failed")
			metrics.KafkaConsume.WithLabelValues(cfg.ServiceName, cfg.KafkaOrderLifecycleTopic, "read_error").Inc()
			span.End()
			time.Sleep(250 * time.Millisecond)
			continue
		}

		// After a successful CAS update, or on redelivery when the row already reflects step.To, chain the next hop.
		if cur == step.To {
			if next := nextStatusAfter(step.To); next != "" {
				pubCtx, pubCancel := context.WithTimeout(consumeCtx, 10*time.Second)
				pubOK := publishOrderLifecycleStep(pubCtx, lifecycleWriter, cfg, metrics, OrderLifecycleStep{
					OrderID: step.OrderID,
					From:    step.To,
					To:      next,
				})
				pubCancel()
				if !pubOK {
					span.SetStatus(codes.Error, "chain publish failed")
					metrics.KafkaConsume.WithLabelValues(cfg.ServiceName, cfg.KafkaOrderLifecycleTopic, "chain_publish_error").Inc()
					span.End()
					time.Sleep(250 * time.Millisecond)
					continue
				}
			}
		}

		if commitErr := reader.CommitMessages(consumeCtx, msg); commitErr != nil {
			span.RecordError(commitErr)
			span.SetStatus(codes.Error, "kafka commit failed")
			metrics.KafkaConsume.WithLabelValues(cfg.ServiceName, cfg.KafkaOrderLifecycleTopic, "commit_error").Inc()
			span.End()
			continue
		}
		metrics.KafkaConsume.WithLabelValues(cfg.ServiceName, cfg.KafkaOrderLifecycleTopic, "success").Inc()

		lag := msg.HighWaterMark - msg.Offset - 1
		if lag < 0 {
			lag = 0
		}
		metrics.KafkaLagGauge.WithLabelValues(
			cfg.ServiceName,
			cfg.KafkaOrderLifecycleTopic,
			strconv.Itoa(int(msg.Partition)),
			cfg.KafkaOrderLifecycleGroupID,
		).Set(float64(lag))
		span.End()
	}
}
