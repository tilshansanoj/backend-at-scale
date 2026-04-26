package kafka

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"backend-at-scale/internal/config"
	"backend-at-scale/internal/observability"
	kafkago "github.com/segmentio/kafka-go"
)

// AsyncCommandProducer enqueues product write commands on a bounded channel; workers publish to Kafka.
type AsyncCommandProducer struct {
	writer  *kafkago.Writer
	cfg     config.Config
	metrics *observability.Metrics
	ch      chan CreateProductCommand
	wg      sync.WaitGroup
}

func NewAsyncCommandProducer(ctx context.Context, writer *kafkago.Writer, cfg config.Config, metrics *observability.Metrics) *AsyncCommandProducer {
	p := &AsyncCommandProducer{
		writer:  writer,
		cfg:     cfg,
		metrics: metrics,
		ch:      make(chan CreateProductCommand, cfg.KafkaAsyncQueueSize),
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
		case cmd := <-p.ch:
			writeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			publishCreateCommandSync(writeCtx, p.writer, p.cfg, p.metrics, cmd)
			cancel()
		}
	}
}

// TryEnqueue is non-blocking: if the command queue is full, return false for caller backpressure handling.
func (p *AsyncCommandProducer) TryEnqueue(cmd CreateProductCommand) bool {
	select {
	case p.ch <- cmd:
		p.metrics.KafkaAsyncEnqueue.WithLabelValues(p.cfg.ServiceName, "accepted").Inc()
		return true
	default:
		p.metrics.KafkaAsyncEnqueue.WithLabelValues(p.cfg.ServiceName, "dropped").Inc()
		return false
	}
}

func (p *AsyncCommandProducer) Wait() {
	p.wg.Wait()
}

func publishCreateCommandSync(ctx context.Context, writer *kafkago.Writer, cfg config.Config, metrics *observability.Metrics, cmd CreateProductCommand) {
	body, err := json.Marshal(cmd)
	if err != nil {
		metrics.KafkaPublish.WithLabelValues(cfg.ServiceName, cfg.KafkaCommandsTopic, "error").Inc()
		return
	}
	msg := kafkago.Message{
		Key:   []byte(cmd.RequestID),
		Value: body,
		Time:  time.Now().UTC(),
	}
	if err := writer.WriteMessages(ctx, msg); err != nil {
		metrics.KafkaPublish.WithLabelValues(cfg.ServiceName, cfg.KafkaCommandsTopic, "error").Inc()
		return
	}
	metrics.KafkaPublish.WithLabelValues(cfg.ServiceName, cfg.KafkaCommandsTopic, "success").Inc()
}

