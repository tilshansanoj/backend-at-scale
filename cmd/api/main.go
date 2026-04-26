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

	db, err := store.NewPostgres(ctx, cfg, metrics)
	if err != nil {
		log.Fatalf("postgres init failed: %v", err)
	}
	defer db.Close()

	redisClient := store.NewRedis(cfg)
	defer redisClient.Close()

	eventsProducer := kafka.NewProducer(cfg, cfg.KafkaTopic)
	defer eventsProducer.Close()
	commandsProducer := kafka.NewProducer(cfg, cfg.KafkaCommandsTopic)
	defer commandsProducer.Close()

	pubCtx, pubStop := context.WithCancel(context.Background())
	kafkaPub := kafka.NewAsyncPublisher(pubCtx, eventsProducer, cfg, metrics)
	kafkaCmd := kafka.NewAsyncCommandProducer(pubCtx, commandsProducer, cfg, metrics)
	defer func() {
		pubStop()
		kafkaPub.Wait()
		kafkaCmd.Wait()
	}()

	consumer, err := kafka.NewConsumer(cfg, cfg.KafkaCommandsTopic, cfg.KafkaCommandsGroupID)
	if err != nil {
		log.Fatalf("kafka consumer init failed: %v", err)
	}
	defer consumer.Close()

	go kafka.StartProductCreateConsumer(ctx, consumer, db, redisClient, metrics, cfg)

	server := app.NewServer(cfg, db, redisClient, kafkaPub, kafkaCmd, metrics)
	if err := server.Listen(":" + cfg.AppPort); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
