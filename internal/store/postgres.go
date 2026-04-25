package store

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"backend-at-scale/internal/config"
	"backend-at-scale/internal/observability"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

const (
	sqlSelectProductsList = `SELECT id, name, price FROM products ORDER BY id ASC LIMIT 100`
)

type Product struct {
	ID    int64   `json:"id"`
	Name  string  `json:"name"`
	Price float64 `json:"price"`
}

type PostgresStore struct {
	write   *pgxpool.Pool
	reads   []*pgxpool.Pool // empty => use write for reads
	readRR  uint64
	metrics *observability.Metrics
	config  config.Config
}

func NewPostgres(ctx context.Context, cfg config.Config, metrics *observability.Metrics) (*PostgresStore, error) {
	replicaURLs := normalizeReplicaURLs(cfg.PostgresURL, cfg.PostgresReplicaURLs)
	writeMax := cfg.PostgresPoolMaxConns
	readMax := cfg.PostgresReadPoolMaxConns
	readMaxPerReplica := readMaxPerDSN(readMax, len(replicaURLs))

	var writePool *pgxpool.Pool
	var readPools []*pgxpool.Pool
	var err error

	if len(replicaURLs) == 0 {
		combined := maxInt(writeMax, readMax)
		writePool, err = newPgxPool(ctx, cfg.PostgresURL, combined)
		if err != nil {
			return nil, fmt.Errorf("primary postgres: %w", err)
		}
		readPools = nil
	} else {
		writePool, err = newPgxPool(ctx, cfg.PostgresURL, writeMax)
		if err != nil {
			return nil, fmt.Errorf("primary postgres: %w", err)
		}
		for _, dsn := range replicaURLs {
			p, err := newPgxPool(ctx, dsn, readMaxPerReplica)
			if err != nil {
				writePool.Close()
				for _, q := range readPools {
					q.Close()
				}
				return nil, fmt.Errorf("read replica postgres: %w", err)
			}
			readPools = append(readPools, p)
		}
	}

	store := &PostgresStore{
		write:   writePool,
		reads:   readPools,
		metrics: metrics,
		config:  cfg,
	}

	go store.recordPoolStats(ctx)

	store.logServerLimits(ctx)
	return store, nil
}

// normalizeReplicaURLs drops empty entries and collapses to primary-only when every URL is the primary DSN.
func normalizeReplicaURLs(primary string, replicas []string) []string {
	out := make([]string, 0, len(replicas))
	for _, u := range replicas {
		u = strings.TrimSpace(u)
		if u == "" {
			continue
		}
		out = append(out, u)
	}
	if len(out) == 0 {
		return nil
	}
	for _, u := range out {
		if u != primary {
			return out
		}
	}
	return nil
}

func readMaxPerDSN(readMax int, replicaCount int) int {
	if replicaCount <= 1 {
		return readMax
	}
	per := readMax / replicaCount
	return maxInt(2, per)
}

func (s *PostgresStore) logServerLimits(ctx context.Context) {
	q := `SELECT setting FROM pg_settings WHERE name = 'max_connections'`
	logPool := func(label string, pool *pgxpool.Pool, poolMax int32) {
		c, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		var setting string
		if err := pool.QueryRow(c, q).Scan(&setting); err != nil {
			log.Printf("[postgres] %s: pool_max=%d server max_connections query failed: %v", label, poolMax, err)
			return
		}
		log.Printf("[postgres] %s: app pool MaxConns=%d server max_connections=%s", label, poolMax, setting)
	}

	if s.samePool() {
		maxC := maxInt(s.config.PostgresPoolMaxConns, s.config.PostgresReadPoolMaxConns)
		logPool("primary+read (single DSN)", s.write, int32(maxC))
		return
	}
	logPool("primary (write)", s.write, s.write.Config().MaxConns)
	for i, p := range s.reads {
		logPool(fmt.Sprintf("replica-%d (read)", i), p, p.Config().MaxConns)
	}
}

func (s *PostgresStore) samePool() bool {
	return len(s.reads) == 0
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
			for i, p := range s.reads {
				s.recordOnePool(fmt.Sprintf("replica-%d", i), p)
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

func (s *PostgresStore) pickRead() (*pgxpool.Pool, string) {
	if len(s.reads) == 0 {
		return s.write, "primary"
	}
	if len(s.reads) == 1 {
		return s.reads[0], "replica-0"
	}
	n := atomic.AddUint64(&s.readRR, 1)
	i := int((n - 1) % uint64(len(s.reads)))
	return s.reads[i], fmt.Sprintf("replica-%d", i)
}

func (s *PostgresStore) GetProducts(ctx context.Context) ([]Product, error) {
	const queryName = "select_products"
	start := time.Now()
	ctx, span := otel.Tracer("ecommerce.store").Start(ctx, "postgres.get_products")
	pool, poolLabel := s.pickRead()
	span.SetAttributes(
		attribute.String("db.operation", "select"),
		attribute.String("db.pool", poolLabel),
	)
	defer span.End()

	rows, err := pool.Query(ctx, sqlSelectProductsList)
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
	for _, p := range s.reads {
		p.Close()
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func newPgxPool(ctx context.Context, dsn string, maxConns int) (*pgxpool.Pool, error) {
	if maxConns < 2 {
		maxConns = 2
	}
	pc, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	pc.MaxConns = int32(maxConns)
	if maxConns >= 4 {
		pc.MinConns = 2
	} else {
		pc.MinConns = 1
	}
	pc.MaxConnLifetime = time.Hour
	pc.MaxConnIdleTime = 15 * time.Minute
	return pgxpool.NewWithConfig(ctx, pc)
}
