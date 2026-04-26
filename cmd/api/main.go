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
	redisQueueClient := store.NewRedisWithDB(cfg, cfg.RedisQueueDB)
	defer redisQueueClient.Close()

	eventsProducer := kafka.NewProducer(cfg, cfg.KafkaTopic)
	defer eventsProducer.Close()
	commandsProducer := kafka.NewProducer(cfg, cfg.KafkaCommandsTopic)
	defer commandsProducer.Close()
	orderCommandsProducer := kafka.NewProducer(cfg, cfg.KafkaOrderCommandsTopic)
	defer orderCommandsProducer.Close()
	orderLifecycleProducer := kafka.NewProducer(cfg, cfg.KafkaOrderLifecycleTopic)
	defer orderLifecycleProducer.Close()

	pubCtx, pubStop := context.WithCancel(context.Background())
	kafkaPub := kafka.NewAsyncPublisher(pubCtx, eventsProducer, cfg, metrics)
	kafkaCmd := kafka.NewAsyncCommandProducer(pubCtx, redisQueueClient, commandsProducer, cfg, metrics)
	orderQueue := kafka.NewJSONQueueProducer(pubCtx, redisQueueClient, orderCommandsProducer, cfg.RedisOrderQueueKey, cfg, metrics)
	defer func() {
		pubStop()
		kafkaPub.Wait()
		kafkaCmd.Wait()
		orderQueue.Wait()
	}()

	consumer, err := kafka.NewConsumer(cfg, cfg.KafkaCommandsTopic, cfg.KafkaCommandsGroupID)
	if err != nil {
		log.Fatalf("kafka consumer init failed: %v", err)
	}
	defer consumer.Close()

	go kafka.StartProductCreateConsumer(ctx, consumer, db, redisClient, metrics, cfg)

	orderConsumer, err := kafka.NewConsumer(cfg, cfg.KafkaOrderCommandsTopic, cfg.KafkaOrderCommandsGroupID)
	if err != nil {
		log.Fatalf("kafka order consumer init failed: %v", err)
	}
	defer orderConsumer.Close()

	orderLifecycleConsumer, err := kafka.NewConsumer(cfg, cfg.KafkaOrderLifecycleTopic, cfg.KafkaOrderLifecycleGroupID)
	if err != nil {
		log.Fatalf("kafka order lifecycle consumer init failed: %v", err)
	}
	defer orderLifecycleConsumer.Close()

	go kafka.StartOrderPlaceConsumer(ctx, orderConsumer, db, orderLifecycleProducer, metrics, cfg)
	go kafka.StartOrderLifecycleConsumer(ctx, orderLifecycleConsumer, db, orderLifecycleProducer, metrics, cfg)

	server := app.NewServer(cfg, db, redisClient, kafkaPub, kafkaCmd, orderQueue, metrics)
	if err := server.Listen(":" + cfg.AppPort); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
