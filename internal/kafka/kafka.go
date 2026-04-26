package kafka

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"backend-at-scale/internal/config"
	"backend-at-scale/internal/observability"
	"backend-at-scale/internal/store"
	"github.com/segmentio/kafka-go"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

type Event struct {
	Type      string    `json:"type"`
	Route     string    `json:"route"`
	Timestamp time.Time `json:"timestamp"`
}

type CreateProductCommand struct {
	RequestID string    `json:"request_id"`
	Name      string    `json:"name"`
	Price     float64   `json:"price"`
	Timestamp time.Time `json:"timestamp"`
}

const productsListCacheKey = "products:list:v1"

func NewProducer(cfg config.Config, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(cfg.KafkaBrokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
		BatchTimeout: time.Duration(cfg.KafkaProducerBatchTimeoutMS) * time.Millisecond,
		BatchBytes:   int64(cfg.KafkaProducerBatchBytes),
		RequiredAcks: kafka.RequireOne,
		MaxAttempts:  cfg.KafkaProducerMaxAttempts,
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
	}
}

func NewConsumer(cfg config.Config, topic, groupID string) (*kafka.Reader, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.KafkaBrokers,
		Topic:   topic,
		GroupID: groupID,
		MaxWait: 500 * time.Millisecond,
	})
	return reader, nil
}

// publishSync writes one message to Kafka and records metrics/traces. Used by AsyncPublisher workers.
func publishSync(ctx context.Context, writer *kafka.Writer, cfg config.Config, metrics *observability.Metrics, event Event) {
	topic := writer.Topic
	if topic == "" {
		topic = cfg.KafkaTopic
	}
	ctx, span := otel.Tracer("ecommerce.kafka").Start(ctx, "kafka.publish")
	span.SetAttributes(
		attribute.String("messaging.system", "kafka"),
		attribute.String("messaging.destination", topic),
		attribute.String("event.type", event.Type),
	)
	defer span.End()

	body, err := json.Marshal(event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "marshal event failed")
		metrics.KafkaPublish.WithLabelValues(cfg.ServiceName, topic, "error").Inc()
		return
	}

	msg := kafka.Message{
		Key:   []byte(event.Type),
		Value: body,
		Time:  time.Now(),
	}

	if err := writer.WriteMessages(ctx, msg); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "kafka publish failed")
		metrics.KafkaPublish.WithLabelValues(cfg.ServiceName, topic, "error").Inc()
		return
	}
	metrics.KafkaPublish.WithLabelValues(cfg.ServiceName, topic, "success").Inc()
}

func StartProductCreateConsumer(
	ctx context.Context,
	reader *kafka.Reader,
	postgres *store.PostgresStore,
	redisClient *redis.Client,
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
			metrics.KafkaConsume.WithLabelValues(cfg.ServiceName, cfg.KafkaCommandsTopic, "error").Inc()
			time.Sleep(250 * time.Millisecond)
			continue
		}

		consumeCtx, span := otel.Tracer("ecommerce.kafka").Start(ctx, "kafka.consume.product_create")
		span.SetAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination", cfg.KafkaCommandsTopic),
			attribute.Int64("messaging.kafka.partition", int64(msg.Partition)),
			attribute.Int64("messaging.kafka.offset", msg.Offset),
		)

		var cmd CreateProductCommand
		if err := json.Unmarshal(msg.Value, &cmd); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "decode command failed")
			metrics.KafkaConsume.WithLabelValues(cfg.ServiceName, cfg.KafkaCommandsTopic, "decode_error").Inc()
			_ = reader.CommitMessages(consumeCtx, msg)
			span.End()
			continue
		}
		cmd.Name = strings.TrimSpace(cmd.Name)
		if cmd.Name == "" || len(cmd.Name) > 200 || cmd.Price <= 0 || cmd.Price > 1_000_000 {
			metrics.KafkaConsume.WithLabelValues(cfg.ServiceName, cfg.KafkaCommandsTopic, "invalid").Inc()
			_ = reader.CommitMessages(consumeCtx, msg)
			span.End()
			continue
		}

		writeCtx, writeCancel := context.WithTimeout(consumeCtx, 5*time.Second)
		_, err = postgres.InsertProduct(writeCtx, cmd.Name, cmd.Price)
		writeCancel()
		if err != nil {
			// Do not commit on write failures so Kafka retries this command.
			span.RecordError(err)
			span.SetStatus(codes.Error, "insert failed")
			metrics.KafkaConsume.WithLabelValues(cfg.ServiceName, cfg.KafkaCommandsTopic, "insert_error").Inc()
			span.End()
			time.Sleep(250 * time.Millisecond)
			continue
		}
		_ = redisClient.Del(consumeCtx, productsListCacheKey).Err()

		if commitErr := reader.CommitMessages(consumeCtx, msg); commitErr != nil {
			span.RecordError(commitErr)
			span.SetStatus(codes.Error, "kafka commit failed")
			metrics.KafkaConsume.WithLabelValues(cfg.ServiceName, cfg.KafkaCommandsTopic, "commit_error").Inc()
			span.End()
			continue
		}
		metrics.KafkaConsume.WithLabelValues(cfg.ServiceName, cfg.KafkaCommandsTopic, "success").Inc()

		lag := msg.HighWaterMark - msg.Offset - 1
		if lag < 0 {
			lag = 0
		}
		metrics.KafkaLagGauge.WithLabelValues(
			cfg.ServiceName,
			cfg.KafkaCommandsTopic,
			strconv.Itoa(int(msg.Partition)),
			cfg.KafkaCommandsGroupID,
		).Set(float64(lag))
		span.SetAttributes(attribute.Int64("messaging.kafka.consumer.lag", lag))
		span.End()
	}
}
