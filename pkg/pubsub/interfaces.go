package pubsub

import (
	"context"

	"github.com/leonardotomascostadasilva/pubsub-go/pkg/events"
)

type EventPublisher interface {
	Publish(ctx context.Context, topic string, event *events.Event) error
	Close() error
}

type EventConsumer interface {
	Subscribe(ctx context.Context, topic string, handler EventHandler) error
	Close() error
}

type EventHandler func(ctx context.Context, event *events.Event) error
