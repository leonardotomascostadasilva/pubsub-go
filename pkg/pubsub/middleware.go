package pubsub

import (
	"context"
	"time"

	"github.com/leonardotomascostadasilva/pubsub-go/pkg/events"
	"github.com/sirupsen/logrus"
)

func LoggingMiddleware(logger *logrus.Logger) Middleware {
	return func(next EventHandler) EventHandler {
		return func(ctx context.Context, event *events.Event) error {
			start := time.Now()

			logger.WithFields(logrus.Fields{
				"event_id":       event.ID,
				"event_name":     event.Name,
				"correlation_id": event.CorrelationID,
				"timestamp":      event.Timestamp,
			}).Info("Processing event")

			err := next(ctx, event)

			duration := time.Since(start)
			logEntry := logger.WithFields(logrus.Fields{
				"event_id":       event.ID,
				"event_name":     event.Name,
				"correlation_id": event.CorrelationID,
				"duration_ms":    duration.Milliseconds(),
			})

			if err != nil {
				logEntry.WithError(err).Error("Event processing failed")
			} else {
				logEntry.Info("Event processed successfully")
			}

			return err
		}
	}
}

func RetryMiddleware(maxRetries int, delay time.Duration) Middleware {
	return func(next EventHandler) EventHandler {
		return func(ctx context.Context, event *events.Event) error {
			var err error

			for attempt := 0; attempt <= maxRetries; attempt++ {
				if attempt > 0 {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(delay * time.Duration(attempt)):
						// Exponential backoff
					}
				}

				err = next(ctx, event)
				if err == nil {
					return nil
				}

				if attempt < maxRetries {
					logrus.WithFields(logrus.Fields{
						"event_id": event.ID,
						"attempt":  attempt + 1,
						"error":    err.Error(),
					}).Warn("Event processing failed, retrying...")
				}
			}

			return err
		}
	}
}

func MetricsMiddleware() Middleware {
	return func(next EventHandler) EventHandler {
		return func(ctx context.Context, event *events.Event) error {
			// TODO: Implementar coleta de métricas
			start := time.Now()
			err := next(ctx, event)
			duration := time.Since(start)

			// Exemplo de como seria a coleta de métricas
			_ = duration

			return err
		}
	}
}
