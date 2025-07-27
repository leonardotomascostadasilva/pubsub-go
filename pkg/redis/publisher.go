package redispkg

import (
	"context"
	"encoding/json"

	"github.com/go-redis/redis/v8"
	"github.com/leonardotomascostadasilva/pubsub-go/pkg/events"
	"github.com/leonardotomascostadasilva/pubsub-go/pkg/pubsub"
	"github.com/sirupsen/logrus"
)

type Publisher struct {
	client *redis.Client
	logger *logrus.Logger
	config *pubsub.Config
}

func NewPublisher(client *redis.Client, logger *logrus.Logger, config *pubsub.Config) *Publisher {
	return &Publisher{
		client: client,
		logger: logger,
		config: config,
	}
}

func (p *Publisher) Publish(ctx context.Context, topic string, event *events.Event) error {
	eventBytes, err := json.Marshal(event)
	if err != nil {
		p.logger.WithError(err).Error("Failed to marshal event")
		return err
	}

	err = p.client.Publish(ctx, topic, eventBytes).Err()
	if err != nil {
		p.logger.WithFields(logrus.Fields{
			"topic":          topic,
			"event_id":       event.ID,
			"correlation_id": event.CorrelationID,
		}).WithError(err).Error("Failed to publish event")
		return err
	}

	p.logger.WithFields(logrus.Fields{
		"topic":          topic,
		"event_id":       event.ID,
		"event_name":     event.Name,
		"correlation_id": event.CorrelationID,
	}).Info("Event published successfully")

	return nil
}

func (p *Publisher) Close() error {
	return p.client.Close()
}
