package events

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Event struct {
	ID            string                 `json:"id"`
	Name          string                 `json:"event_name"`
	Payload       json.RawMessage        `json:"payload"`
	CorrelationID string                 `json:"correlation_id"`
	Timestamp     time.Time              `json:"timestamp"`
	Metadata      map[string]interface{} `json:"metadata,omitempty"`
}

func NewEvent(name string, payload interface{}, correlationID string) (*Event, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	if correlationID == "" {
		correlationID = uuid.New().String()
	}

	return &Event{
		ID:            uuid.New().String(),
		Name:          name,
		Payload:       payloadBytes,
		CorrelationID: correlationID,
		Timestamp:     time.Now().UTC(),
		Metadata:      make(map[string]interface{}),
	}, nil
}

func (e *Event) UnmarshalPayload(v interface{}) error {
	return json.Unmarshal(e.Payload, v)
}
