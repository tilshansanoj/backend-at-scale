package kafka

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"backend-at-scale/internal/config"
	"backend-at-scale/internal/observability"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/redis/go-redis/v9"
)

// JSONQueueProducer moves JSON payloads from a Redis list to a Kafka topic (bounded queue + workers).
type JSONQueueProducer struct {
	redis    *redis.Client
	writer   *kafkago.Writer
	queueKey string
	cfg      config.Config
	metrics  *observability.Metrics
	wg       sync.WaitGroup
}

func NewJSONQueueProducer(
	ctx context.Context,
	redisClient *redis.Client,
	writer *kafkago.Writer,
	queueKey string,
	cfg config.Config,
	metrics *observability.Metrics,
) *JSONQueueProducer {
	p := &JSONQueueProducer{
		redis:    redisClient,
		writer:   writer,
		queueKey: queueKey,
		cfg:      cfg,
		metrics:  metrics,
	}
	for range cfg.KafkaAsyncWorkers {
		p.wg.Add(1)
		go p.worker(ctx)
	}
	return p
}

func (p *JSONQueueProducer) worker(ctx context.Context) {
	defer p.wg.Done()
	topic := p.writer.Topic
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		parts, err := p.redis.BLPop(ctx, time.Second, p.queueKey).Result()
		if err != nil {
			if err == redis.Nil || ctx.Err() != nil {
				continue
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if len(parts) != 2 {
			continue
		}
		queuePayload := []byte(parts[1])
		writeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		ok := publishKafkaMessage(writeCtx, p.writer, p.cfg, p.metrics, topic, queuePayload)
		cancel()
		if !ok {
			_ = p.redis.LPush(context.Background(), p.queueKey, parts[1]).Err()
			time.Sleep(250 * time.Millisecond)
		}
	}
}

// TryEnqueue pushes raw JSON to the Redis queue when under max length. Returns false when full or on Redis error.
func (p *JSONQueueProducer) TryEnqueue(messageKey []byte, jsonValue []byte) bool {
	ctx := context.Background()
	length, err := p.redis.LLen(ctx, p.queueKey).Result()
	if err != nil {
		p.metrics.KafkaAsyncEnqueue.WithLabelValues(p.cfg.ServiceName, "dropped").Inc()
		return false
	}
	if length >= int64(p.cfg.RedisCommandQueueMaxLen) {
		p.metrics.KafkaAsyncEnqueue.WithLabelValues(p.cfg.ServiceName, "dropped").Inc()
		return false
	}
	msg := kafkago.Message{
		Key:   messageKey,
		Value: jsonValue,
		Time:  time.Now().UTC(),
	}
	body, err := json.Marshal(queuedKafkaMessage{Key: msg.Key, Value: msg.Value})
	if err != nil {
		p.metrics.KafkaAsyncEnqueue.WithLabelValues(p.cfg.ServiceName, "dropped").Inc()
		return false
	}
	if err := p.redis.RPush(ctx, p.queueKey, body).Err(); err != nil {
		p.metrics.KafkaAsyncEnqueue.WithLabelValues(p.cfg.ServiceName, "dropped").Inc()
		return false
	}
	p.metrics.KafkaAsyncEnqueue.WithLabelValues(p.cfg.ServiceName, "accepted").Inc()
	return true
}

func (p *JSONQueueProducer) Wait() {
	p.wg.Wait()
}

// jsonMarshalMessage encodes a kafka message as JSON for the Redis queue worker.
// The worker unmarshals and calls WriteMessages so key/topic are preserved.
type queuedKafkaMessage struct {
	Key   []byte `json:"key,omitempty"`
	Value []byte `json:"value"`
}

func jsonUnmarshalMessage(data []byte) (kafkago.Message, error) {
	var q queuedKafkaMessage
	if err := json.Unmarshal(data, &q); err != nil {
		return kafkago.Message{}, err
	}
	return kafkago.Message{Key: q.Key, Value: q.Value, Time: time.Now().UTC()}, nil
}

func publishKafkaMessage(ctx context.Context, writer *kafkago.Writer, cfg config.Config, metrics *observability.Metrics, topic string, queuePayload []byte) bool {
	msg, err := jsonUnmarshalMessage(queuePayload)
	if err != nil {
		metrics.KafkaPublish.WithLabelValues(cfg.ServiceName, topic, "error").Inc()
		return false
	}
	if err := writer.WriteMessages(ctx, msg); err != nil {
		metrics.KafkaPublish.WithLabelValues(cfg.ServiceName, topic, "error").Inc()
		return false
	}
	metrics.KafkaPublish.WithLabelValues(cfg.ServiceName, topic, "success").Inc()
	return true
}
