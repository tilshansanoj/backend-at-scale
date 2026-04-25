package kafka

import (
	"context"
	"sync"
	"time"

	"backend-at-scale/internal/config"
	"backend-at-scale/internal/observability"
	kafkago "github.com/segmentio/kafka-go"
)

// AsyncPublisher enqueues product events on a bounded channel; workers call Kafka WriteMessages
// so HTTP handlers never block on the broker.
type AsyncPublisher struct {
	writer  *kafkago.Writer
	cfg     config.Config
	metrics *observability.Metrics
	ch      chan Event
	wg      sync.WaitGroup
}

func NewAsyncPublisher(ctx context.Context, writer *kafkago.Writer, cfg config.Config, metrics *observability.Metrics) *AsyncPublisher {
	p := &AsyncPublisher{
		writer:  writer,
		cfg:     cfg,
		metrics: metrics,
		ch:      make(chan Event, cfg.KafkaAsyncQueueSize),
	}
	for range cfg.KafkaAsyncWorkers {
		p.wg.Add(1)
		go p.worker(ctx)
	}
	return p
}

func (p *AsyncPublisher) worker(ctx context.Context) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-p.ch:
			writeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			publishSync(writeCtx, p.writer, p.cfg, p.metrics, ev)
			cancel()
		}
	}
}

// TryEnqueue is non-blocking: if the buffer is full, the event is dropped and a metric is incremented.
func (p *AsyncPublisher) TryEnqueue(event Event) {
	select {
	case p.ch <- event:
		p.metrics.KafkaAsyncEnqueue.WithLabelValues(p.cfg.ServiceName, "accepted").Inc()
	default:
		p.metrics.KafkaAsyncEnqueue.WithLabelValues(p.cfg.ServiceName, "dropped").Inc()
	}
}

// Wait blocks until all workers have exited (typically after root context cancellation).
func (p *AsyncPublisher) Wait() {
	p.wg.Wait()
}
