package store

import (
	"time"

	"backend-at-scale/internal/config"
	"github.com/redis/go-redis/v9"
)

func NewRedis(cfg config.Config) *redis.Client {
	return NewRedisWithDB(cfg, cfg.RedisDB)
}

func NewRedisWithDB(cfg config.Config, db int) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPass,
		DB:       db,
		PoolSize:        cfg.RedisPoolSize,
		MinIdleConns:    cfg.RedisMinIdleConns,
		ConnMaxIdleTime: 5 * time.Minute,
	})
}
