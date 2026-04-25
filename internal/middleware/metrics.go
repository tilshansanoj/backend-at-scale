package middleware

import (
	"strconv"
	"time"

	"backend-at-scale/internal/config"
	"backend-at-scale/internal/observability"
	"github.com/gofiber/fiber/v2"
)

func PrometheusHTTP(cfg config.Config, metrics *observability.Metrics) fiber.Handler {
	return func(c *fiber.Ctx) error {
		start := time.Now()
		err := c.Next()
		duration := time.Since(start).Seconds()

		route := c.Path()
		if c.Route() != nil && c.Route().Path != "" {
			route = c.Route().Path
		}
		if route == "" {
			route = c.Path()
		}
		status := strconv.Itoa(c.Response().StatusCode())
		method := c.Method()

		metrics.HTTPRequestTotal.WithLabelValues(cfg.ServiceName, route, method, status).Inc()
		metrics.HTTPRequestDur.WithLabelValues(cfg.ServiceName, route, method, status).Observe(duration)

		return err
	}
}
