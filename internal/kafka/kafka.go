package kafka

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"backend-at-scale/internal/config"
	"backend-at-scale/internal/observability"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

type Event struct {
	Type      string    `json:"type"`
	Route     string    `json:"route"`
	Timestamp time.Time `json:"timestamp"`
}

func NewProducer(cfg config.Config) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(cfg.KafkaBrokers...),
		Topic:    cfg.KafkaTopic,
		Balancer: &kafka.LeastBytes{},
	}
}

func NewConsumer(cfg config.Config) (*kafka.Reader, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.KafkaBrokers,
		Topic:   cfg.KafkaTopic,
		GroupID: cfg.KafkaGroupID,
		MaxWait: 500 * time.Millisecond,
	})
	return reader, nil
}

func Publish(ctx context.Context, writer *kafka.Writer, cfg config.Config, metrics *observability.Metrics, event Event) {
	ctx, span := otel.Tracer("ecommerce.kafka").Start(ctx, "kafka.publish")
	span.SetAttributes(
		attribute.String("messaging.system", "kafka"),
		attribute.String("messaging.destination", cfg.KafkaTopic),
		attribute.String("event.type", event.Type),
	)
	defer span.End()

	body, err := json.Marshal(event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "marshal event failed")
		metrics.KafkaPublish.WithLabelValues(cfg.ServiceName, cfg.KafkaTopic, "error").Inc()
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
		metrics.KafkaPublish.WithLabelValues(cfg.ServiceName, cfg.KafkaTopic, "error").Inc()
		return
	}
	metrics.KafkaPublish.WithLabelValues(cfg.ServiceName, cfg.KafkaTopic, "success").Inc()
}

func StartBackgroundConsumer(ctx context.Context, reader *kafka.Reader, metrics *observability.Metrics, cfg config.Config) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			metrics.KafkaConsume.WithLabelValues(cfg.ServiceName, cfg.KafkaTopic, "error").Inc()
			time.Sleep(250 * time.Millisecond)
			continue
		}

		consumeCtx, span := otel.Tracer("ecommerce.kafka").Start(ctx, "kafka.consume")
		span.SetAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination", cfg.KafkaTopic),
			attribute.Int64("messaging.kafka.partition", int64(msg.Partition)),
			attribute.Int64("messaging.kafka.offset", msg.Offset),
		)
		metrics.KafkaConsume.WithLabelValues(cfg.ServiceName, cfg.KafkaTopic, "success").Inc()

		if commitErr := reader.CommitMessages(consumeCtx, msg); commitErr != nil {
			span.RecordError(commitErr)
			span.SetStatus(codes.Error, "kafka commit failed")
		}

		lag := msg.HighWaterMark - msg.Offset - 1
		if lag < 0 {
			lag = 0
		}
		metrics.KafkaLagGauge.WithLabelValues(
			cfg.ServiceName,
			cfg.KafkaTopic,
			strconv.Itoa(int(msg.Partition)),
			cfg.KafkaGroupID,
		).Set(float64(lag))
		span.SetAttributes(attribute.Int64("messaging.kafka.consumer.lag", lag))
		span.End()
	}
}
