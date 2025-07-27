package redispkg

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/go-redis/redis/v8"
	"github.com/leonardotomascostadasilva/pubsub-go/pkg/events"
	"github.com/leonardotomascostadasilva/pubsub-go/pkg/pubsub"
	"github.com/sirupsen/logrus"
)

type Consumer struct {
	client *redis.Client
	logger *logrus.Logger
	config *pubsub.Config
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func NewConsumer(client *redis.Client, logger *logrus.Logger, config *pubsub.Config) *Consumer {
	return &Consumer{
		client: client,
		logger: logger,
		config: config,
	}
}

func (c *Consumer) Subscribe(ctx context.Context, topic string, handler pubsub.EventHandler) error {
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	pubsub := c.client.Subscribe(ctx, topic)
	defer pubsub.Close()

	ch := pubsub.Channel()

	c.logger.WithFields(logrus.Fields{
		"topic":       topic,
		"max_workers": c.config.MaxWorkers,
	}).Info("Started consuming events (no buffer - max speed)")

	for i := 0; i < c.config.MaxWorkers; i++ {
		c.wg.Add(1)
		go c.worker(ctx, ch, handler, i)
	}

	c.wg.Wait()
	return ctx.Err()
}

func (c *Consumer) worker(ctx context.Context, ch <-chan *redis.Message, handler pubsub.EventHandler, workerID int) {
	defer c.wg.Done()

	c.logger.WithField("worker_id", workerID).Info("Worker started")

	for {
		select {
		case <-ctx.Done():
			c.logger.WithField("worker_id", workerID).Info("Worker stopped")
			return
		case msg, ok := <-ch:
			if !ok {
				c.logger.WithField("worker_id", workerID).Info("Channel closed, worker stopping")
				return
			}

			var event events.Event
			if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
				c.logger.WithError(err).Error("Failed to unmarshal event")
				continue
			}

			if err := handler(ctx, &event); err != nil {
				c.logger.WithFields(logrus.Fields{
					"event_id":       event.ID,
					"correlation_id": event.CorrelationID,
					"worker_id":      workerID,
				}).WithError(err).Error("Event processing failed")
			}
		}
	}
}

func (c *Consumer) Close() error {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	return c.client.Close()
}
