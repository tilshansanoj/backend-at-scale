package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"backend-at-scale/internal/app"
	"backend-at-scale/internal/config"
	"backend-at-scale/internal/kafka"
	"backend-at-scale/internal/observability"
	"backend-at-scale/internal/store"
)

func main() {
	cfg := config.Load()
	metrics := observability.NewMetrics(cfg.ServiceName)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	shutdownTrace, err := observability.InitTracing(ctx, cfg)
	if err != nil {
		log.Fatalf("otel tracing init failed: %v", err)
	}
	defer func() {
		_ = shutdownTrace(context.Background())
	}()

	db, err := store.NewPostgres(ctx, cfg, metrics)
	if err != nil {
		log.Fatalf("postgres init failed: %v", err)
	}
	defer db.Close()

	redisClient := store.NewRedis(cfg)
	defer redisClient.Close()

	producer := kafka.NewProducer(cfg)
	defer producer.Close()

	consumer, err := kafka.NewConsumer(cfg)
	if err != nil {
		log.Fatalf("kafka consumer init failed: %v", err)
	}
	defer consumer.Close()

	go kafka.StartBackgroundConsumer(ctx, consumer, metrics, cfg)

	server := app.NewServer(cfg, db, redisClient, producer, metrics)
	if err := server.Listen(":" + cfg.AppPort); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
