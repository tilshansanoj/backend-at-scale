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

// AsyncCommandProducer enqueues product write commands in Redis; workers pop from Redis and publish to Kafka.
type AsyncCommandProducer struct {
	redis   *redis.Client
	writer  *kafkago.Writer
	cfg     config.Config
	metrics *observability.Metrics
	wg      sync.WaitGroup
}

func NewAsyncCommandProducer(
	ctx context.Context,
	redisClient *redis.Client,
	writer *kafkago.Writer,
	cfg config.Config,
	metrics *observability.Metrics,
) *AsyncCommandProducer {
	p := &AsyncCommandProducer{
		redis:   redisClient,
		writer:  writer,
		cfg:     cfg,
		metrics: metrics,
	}
	for range cfg.KafkaAsyncWorkers {
		p.wg.Add(1)
		go p.worker(ctx)
	}
	return p
}

func (p *AsyncCommandProducer) worker(ctx context.Context) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		parts, err := p.redis.BLPop(ctx, time.Second, p.cfg.RedisCommandQueueKey).Result()
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

		var cmd CreateProductCommand
		if err := json.Unmarshal([]byte(parts[1]), &cmd); err != nil {
			p.metrics.KafkaPublish.WithLabelValues(p.cfg.ServiceName, p.cfg.KafkaCommandsTopic, "error").Inc()
			continue
		}

		writeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		ok := publishCreateCommandSync(writeCtx, p.writer, p.cfg, p.metrics, cmd)
		cancel()
		if !ok {
			// Requeue on publish failure to avoid losing accepted commands.
			_ = p.redis.LPush(context.Background(), p.cfg.RedisCommandQueueKey, parts[1]).Err()
			time.Sleep(250 * time.Millisecond)
		}
	}
}

// TryEnqueue is non-blocking: if Redis queue is full, return false for caller backpressure handling.
func (p *AsyncCommandProducer) TryEnqueue(cmd CreateProductCommand) bool {
	ctx := context.Background()
	length, err := p.redis.LLen(ctx, p.cfg.RedisCommandQueueKey).Result()
	if err != nil {
		p.metrics.KafkaAsyncEnqueue.WithLabelValues(p.cfg.ServiceName, "dropped").Inc()
		return false
	}
	if length >= int64(p.cfg.RedisCommandQueueMaxLen) {
		p.metrics.KafkaAsyncEnqueue.WithLabelValues(p.cfg.ServiceName, "dropped").Inc()
		return false
	}
	body, err := json.Marshal(cmd)
	if err != nil {
		p.metrics.KafkaAsyncEnqueue.WithLabelValues(p.cfg.ServiceName, "dropped").Inc()
		return false
	}
	if err := p.redis.RPush(ctx, p.cfg.RedisCommandQueueKey, body).Err(); err != nil {
		p.metrics.KafkaAsyncEnqueue.WithLabelValues(p.cfg.ServiceName, "dropped").Inc()
		return false
	}
	p.metrics.KafkaAsyncEnqueue.WithLabelValues(p.cfg.ServiceName, "accepted").Inc()
	return true
}

func (p *AsyncCommandProducer) Wait() {
	p.wg.Wait()
}

func publishCreateCommandSync(ctx context.Context, writer *kafkago.Writer, cfg config.Config, metrics *observability.Metrics, cmd CreateProductCommand) bool {
	body, err := json.Marshal(cmd)
	if err != nil {
		metrics.KafkaPublish.WithLabelValues(cfg.ServiceName, cfg.KafkaCommandsTopic, "error").Inc()
		return false
	}
	msg := kafkago.Message{
		Key:   []byte(cmd.RequestID),
		Value: body,
		Time:  time.Now().UTC(),
	}
	if err := writer.WriteMessages(ctx, msg); err != nil {
		metrics.KafkaPublish.WithLabelValues(cfg.ServiceName, cfg.KafkaCommandsTopic, "error").Inc()
		return false
	}
	metrics.KafkaPublish.WithLabelValues(cfg.ServiceName, cfg.KafkaCommandsTopic, "success").Inc()
	return true
}

