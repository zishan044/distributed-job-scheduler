package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
)

// DeadLetter contains either a failed dispatch or the source of a malformed message.
type DeadLetter struct {
	Message *store.DispatchMessage `json:"message,omitempty"`
	Error   string                 `json:"error"`
	Source  *DeadLetterSource      `json:"source,omitempty"`
}

type DeadLetterSource struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Key       []byte `json:"key"`
	RawValue  []byte `json:"raw_value"` // Base64 preserves malformed JSON and invalid UTF-8.
}

func PublishDeadLetters(ctx context.Context, writer *kafka.Writer, counter prometheus.Counter, letters ...DeadLetter) error {
	if len(letters) == 0 {
		return nil
	}
	messages := make([]kafka.Message, 0, len(letters))
	for _, letter := range letters {
		var key []byte
		if letter.Message != nil {
			key = []byte(letter.Message.RunID)
		} else if letter.Source != nil {
			key = letter.Source.Key
		} else {
			return fmt.Errorf("dead letter requires a dispatch or source")
		}
		value, err := json.Marshal(letter)
		if err != nil {
			return err
		}
		messages = append(messages, kafka.Message{Key: key, Value: value})
	}
	err := writer.WriteMessages(ctx, messages...)
	counter.Add(float64(acknowledgedMessages(len(messages), err)))
	return err
}

func acknowledgedMessages(count int, err error) int {
	if err == nil {
		return count
	}
	var failures kafka.WriteErrors
	if !errors.As(err, &failures) {
		return 0 // Cancellation and connection errors leave acknowledgement unknown.
	}
	acknowledged := 0
	for _, failure := range failures {
		if failure == nil {
			acknowledged++
		}
	}
	return acknowledged
}
