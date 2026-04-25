package observability

import (
	"context"
	"fmt"
	"time"

	"backend-at-scale/internal/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func InitTracing(ctx context.Context, cfg config.Config) (func(context.Context) error, error) {
	client := otlptracegrpc.NewClient(
		otlptracegrpc.WithEndpoint(cfg.OTELExporterEndpoint),
		otlptracegrpc.WithTimeout(5*time.Second),
	)
	if cfg.OTELInsecure {
		client = otlptracegrpc.NewClient(
			otlptracegrpc.WithEndpoint(cfg.OTELExporterEndpoint),
			otlptracegrpc.WithInsecure(),
			otlptracegrpc.WithTimeout(5*time.Second),
		)
	}

	exporter, err := otlptracegrpc.New(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("create otlp trace exporter: %w", err)
	}

	res, err := resource.New(
		ctx,
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
			semconv.DeploymentEnvironmentName(cfg.Environment),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create otel resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp.Shutdown, nil
}
