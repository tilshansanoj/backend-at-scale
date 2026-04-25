package store

import (
	"context"
	"fmt"
	"strings"
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
	write   *pgxpool.Pool
	read    *pgxpool.Pool
	metrics *observability.Metrics
	config  config.Config
}

func NewPostgres(ctx context.Context, cfg config.Config, metrics *observability.Metrics) (*PostgresStore, error) {
	writePool, err := pgxpool.New(ctx, cfg.PostgresURL)
	if err != nil {
		return nil, fmt.Errorf("primary postgres: %w", err)
	}

	readDSN := strings.TrimSpace(cfg.PostgresReplicaURL)
	var readPool *pgxpool.Pool
	if readDSN == "" || readDSN == cfg.PostgresURL {
		readPool = writePool
	} else {
		readPool, err = pgxpool.New(ctx, readDSN)
		if err != nil {
			writePool.Close()
			return nil, fmt.Errorf("replica postgres: %w", err)
		}
	}

	store := &PostgresStore{
		write:   writePool,
		read:    readPool,
		metrics: metrics,
		config:  cfg,
	}

	go store.recordPoolStats(ctx)
	return store, nil
}

func (s *PostgresStore) samePool() bool {
	return s.read == s.write
}

func (s *PostgresStore) recordPoolStats(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.recordOnePool("primary", s.write)
			if !s.samePool() {
				s.recordOnePool("replica", s.read)
			}
		}
	}
}

func (s *PostgresStore) recordOnePool(poolName string, pool *pgxpool.Pool) {
	stats := pool.Stat()
	s.metrics.DBPoolConns.WithLabelValues(s.config.ServiceName, poolName, "acquired").Set(float64(stats.AcquiredConns()))
	s.metrics.DBPoolConns.WithLabelValues(s.config.ServiceName, poolName, "idle").Set(float64(stats.IdleConns()))
	s.metrics.DBPoolConns.WithLabelValues(s.config.ServiceName, poolName, "total").Set(float64(stats.TotalConns()))
}

func (s *PostgresStore) GetProducts(ctx context.Context) ([]Product, error) {
	const queryName = "select_products"
	start := time.Now()
	ctx, span := otel.Tracer("ecommerce.store").Start(ctx, "postgres.get_products")
	span.SetAttributes(
		attribute.String("db.operation", "select"),
		attribute.String("db.pool", readPoolLabel(s)),
	)
	defer span.End()

	rows, err := s.read.Query(ctx, "SELECT id, name, price FROM products ORDER BY id LIMIT 100")
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

func readPoolLabel(s *PostgresStore) string {
	if s.samePool() {
		return "primary"
	}
	return "replica"
}

func (s *PostgresStore) InsertProduct(ctx context.Context, name string, price float64) (Product, error) {
	const queryName = "insert_product"
	start := time.Now()
	ctx, span := otel.Tracer("ecommerce.store").Start(ctx, "postgres.insert_product")
	span.SetAttributes(
		attribute.String("db.operation", "insert"),
		attribute.String("db.pool", "primary"),
	)
	defer span.End()

	var p Product
	err := s.write.QueryRow(
		ctx,
		`INSERT INTO products (name, price) VALUES ($1, $2) RETURNING id, name, price`,
		name,
		price,
	).Scan(&p.ID, &p.Name, &p.Price)
	s.metrics.DBQueryDur.WithLabelValues(s.config.ServiceName, queryName).Observe(time.Since(start).Seconds())
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "insert failed")
		return Product{}, err
	}
	return p, nil
}

func (s *PostgresStore) Close() {
	s.write.Close()
	if !s.samePool() {
		s.read.Close()
	}
}
