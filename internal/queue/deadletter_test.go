package queue

import (
	"context"
	"fmt"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestAcknowledgedMessages(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want int
	}{
		{"success", nil, 3},
		{"unknown", context.Canceled, 0},
		{"partial", kafka.WriteErrors{nil, kafka.MessageSizeTooLarge, nil}, 2},
		{"failed", kafka.WriteErrors{kafka.MessageSizeTooLarge, kafka.NotLeaderForPartition, kafka.NotLeaderForPartition}, 0},
		{"wrapped", fmt.Errorf("write: %w", kafka.WriteErrors{nil, kafka.NotLeaderForPartition, nil}), 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := acknowledgedMessages(3, tc.err); got != tc.want {
				t.Fatalf("acknowledged = %d, want %d", got, tc.want)
			}
		})
	}
}
