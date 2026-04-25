package store

import (
	"context"
	"time"

	"backend-at-scale/internal/config"
	"backend-at-scale/internal/observability"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

type Product struct {
	ID    int64   `json:"id"`
	Name  string  `json:"name"`
	Price float64 `json:"price"`
}

type PostgresStore struct {
	Pool    *pgxpool.Pool
	metrics *observability.Metrics
	config  config.Config
}

func NewPostgres(ctx context.Context, cfg config.Config, metrics *observability.Metrics) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, cfg.PostgresURL)
	if err != nil {
		return nil, err
	}

	store := &PostgresStore{
		Pool:    pool,
		metrics: metrics,
		config:  cfg,
	}

	go store.recordPoolStats(ctx)
	return store, nil
}

func (s *PostgresStore) recordPoolStats(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats := s.Pool.Stat()
			s.metrics.DBPoolConns.WithLabelValues(s.config.ServiceName, "acquired").Set(float64(stats.AcquiredConns()))
			s.metrics.DBPoolConns.WithLabelValues(s.config.ServiceName, "idle").Set(float64(stats.IdleConns()))
			s.metrics.DBPoolConns.WithLabelValues(s.config.ServiceName, "total").Set(float64(stats.TotalConns()))
		}
	}
}

func (s *PostgresStore) GetProducts(ctx context.Context) ([]Product, error) {
	const queryName = "select_products"
	start := time.Now()
	ctx, span := otel.Tracer("ecommerce.store").Start(ctx, "postgres.get_products")
	span.SetAttributes(attribute.String("db.operation", "select"))
	defer span.End()

	rows, err := s.Pool.Query(ctx, "SELECT id, name, price FROM products ORDER BY id LIMIT 100")
	s.metrics.DBQueryDur.WithLabelValues(s.config.ServiceName, queryName).Observe(time.Since(start).Seconds())
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "query failed")
		return nil, err
	}
	defer rows.Close()

	products := make([]Product, 0, 100)
	for rows.Next() {
		var p Product
		if err := rows.Scan(&p.ID, &p.Name, &p.Price); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "row scan failed")
			return nil, err
		}
		products = append(products, p)
	}
	if err := rows.Err(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "rows iteration failed")
		return nil, err
	}
	span.SetAttributes(attribute.Int("db.rows", len(products)))
	return products, nil
}

func (s *PostgresStore) Close() {
	s.Pool.Close()
}
