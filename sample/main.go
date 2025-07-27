package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"

	"github.com/leonardotomascostadasilva/pubsub-go/pkg/events"
	"github.com/leonardotomascostadasilva/pubsub-go/pkg/pubsub"
	redispkg "github.com/leonardotomascostadasilva/pubsub-go/pkg/redis"
)

type UserCreatedPayload struct {
	UserID   string `json:"user_id"`
	Email    string `json:"email"`
	Username string `json:"username"`
}

var (
	processedCount int64
	startTime      time.Time
)

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatal("Failed to connect to Redis:", err)
	}

	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	go runConsumer(context.Background(), rdb, logger)
	time.Sleep(2 * time.Second)
	runPublisher(ctx, rdb, logger)

}

func runPublisher(ctx context.Context, rdb *redis.Client, logger *logrus.Logger) {
	fmt.Println("=== PUBLISHER MODE ===")

	publisher := redispkg.NewPublisher(rdb, logger)
	defer publisher.Close()

	fmt.Println("Publishing single event...")
	payload := UserCreatedPayload{
		UserID:   "user-123",
		Email:    "user@example.com",
		Username: "johndoe",
	}

	event, err := events.NewEvent("user.created", payload, "")
	if err != nil {
		log.Fatal(err)
	}

	if err := publisher.Publish(ctx, "user-events", event); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Single event published!")

	fmt.Println("Publishing 1000 events for performance test...")
	start := time.Now()

	for i := 0; i < 1000; i++ {
		payload := UserCreatedPayload{
			UserID:   fmt.Sprintf("user-%d", i),
			Email:    fmt.Sprintf("user%d@example.com", i),
			Username: fmt.Sprintf("user%d", i),
		}

		event, _ := events.NewEvent("user.created", payload, "batch-perf-test")
		if err := publisher.Publish(ctx, "user-events", event); err != nil {
			log.Printf("Failed to publish event %d: %v", i, err)
		}

		if i%100 == 0 {
			fmt.Printf("Published %d events...\n", i)
		}
	}

	duration := time.Since(start)
	fmt.Printf("Published 1000 events in %v (%.2f events/sec)\n",
		duration, 1000.0/duration.Seconds())
}

func runConsumer(ctx context.Context, rdb *redis.Client, logger *logrus.Logger) {
	fmt.Println("=== CONSUMER MODE ===")

	config := &pubsub.Config{
		MaxWorkers: 20,
	}

	consumer := redispkg.NewConsumer(rdb, logger, config)
	defer consumer.Close()

	handler := func(ctx context.Context, event *events.Event) error {
		count := atomic.AddInt64(&processedCount, 1)

		if count%100 == 0 {
			elapsed := time.Since(startTime)
			rate := float64(count) / elapsed.Seconds()
			fmt.Printf("Processed %d events (%.2f events/sec)\n", count, rate)
		}

		switch event.Name {
		case "user.created":
			var payload UserCreatedPayload
			if err := event.UnmarshalPayload(&payload); err != nil {
				return err
			}

			fmt.Printf("Event consume: %v\n", payload)

		default:
			logger.WithField("event_name", event.Name).Debug("Unknown event type")
		}

		return nil
	}

	ctx, cancel := context.WithCancel(ctx)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		logger.Info("Shutting down consumer...")
		cancel()
	}()

	startTime = time.Now()
	fmt.Printf("Starting consumer with %d workers (optimized for speed)...\n", config.MaxWorkers)

	if err := consumer.Subscribe(ctx, "user-events", handler); err != nil {
		if err != context.Canceled {
			log.Fatal(err)
		}
	}

	elapsed := time.Since(startTime)
	finalCount := atomic.LoadInt64(&processedCount)
	finalRate := float64(finalCount) / elapsed.Seconds()

	fmt.Printf("\nConsumer stopped. Final stats:\n")
	fmt.Printf("- Processed: %d events\n", finalCount)
	fmt.Printf("- Duration: %v\n", elapsed)
	fmt.Printf("- Rate: %.2f events/sec\n", finalRate)
}
