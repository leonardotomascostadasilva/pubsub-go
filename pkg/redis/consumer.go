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
	client      *redis.Client
	logger      *logrus.Logger
	config      *pubsub.Config
	middlewares []pubsub.Middleware
	wg          sync.WaitGroup
	cancel      context.CancelFunc
}

func NewConsumer(client *redis.Client, logger *logrus.Logger, config *pubsub.Config) *Consumer {
	return &Consumer{
		client:      client,
		logger:      logger,
		config:      config,
		middlewares: make([]pubsub.Middleware, 0),
	}
}

func (c *Consumer) AddMiddleware(middleware pubsub.Middleware) {
	c.middlewares = append(c.middlewares, middleware)
}

func (c *Consumer) Subscribe(ctx context.Context, topic string, handler pubsub.EventHandler) error {
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	finalHandler := handler
	for i := len(c.middlewares) - 1; i >= 0; i-- {
		finalHandler = c.middlewares[i](finalHandler)
	}

	pubsub := c.client.Subscribe(ctx, topic)
	defer pubsub.Close()

	ch := pubsub.Channel()

	eventChan := make(chan *events.Event, c.config.BufferSize)

	for i := 0; i < c.config.MaxWorkers; i++ {
		c.wg.Add(1)
		go c.worker(ctx, eventChan, finalHandler)
	}

	for {
		select {
		case <-ctx.Done():
			close(eventChan)
			c.wg.Wait()
			return ctx.Err()
		case msg := <-ch:
			var event events.Event
			if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
				c.logger.WithError(err).Error("Failed to unmarshal event")
				continue
			}

			select {
			case eventChan <- &event:
			case <-ctx.Done():
				close(eventChan)
				c.wg.Wait()
				return ctx.Err()
			}
		}
	}
}

func (c *Consumer) worker(ctx context.Context, eventChan <-chan *events.Event, handler pubsub.EventHandler) {
	defer c.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-eventChan:
			if !ok {
				return
			}

			if err := handler(ctx, event); err != nil {
				c.logger.WithFields(logrus.Fields{
					"event_id":       event.ID,
					"correlation_id": event.CorrelationID,
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
