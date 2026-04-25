package store

import (
	"backend-at-scale/internal/config"
	"github.com/redis/go-redis/v9"
)

func NewRedis(cfg config.Config) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPass,
		DB:       cfg.RedisDB,
	})
}
