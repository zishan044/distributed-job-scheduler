package queue

import (
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestConsumerLagOffsets(t *testing.T) {
	for _, test := range []struct {
		name                   string
		committed, first, last int64
		want                   int64
	}{
		{"uncommitted", -1, 0, 12, 12},
		{"partly committed", 4, 0, 12, 8},
		{"drained", 12, 0, 12, 0},
		{"empty", -1, 0, 0, 0},
		{"retained uncommitted", -1, 7, 12, 5},
		{"expired commit", 4, 7, 12, 5},
		{"truncated log", 20, 7, 12, 5},
	} {
		t.Run(test.name, func(t *testing.T) {
			lag, err := consumerLag([]int{0}, []kafka.OffsetFetchPartition{{Partition: 0, CommittedOffset: test.committed}},
				[]kafka.PartitionOffsets{{Partition: 0, FirstOffset: test.first, LastOffset: test.last}})
			if err != nil || lag != test.want {
				t.Fatalf("lag = %d, %v, want %d", lag, err, test.want)
			}
		})
	}
	committed := []kafka.OffsetFetchPartition{{Partition: 1, CommittedOffset: 3}, {Partition: 0, CommittedOffset: -1}}
	bounds := []kafka.PartitionOffsets{{Partition: 0, FirstOffset: 2, LastOffset: 5}, {Partition: 1, LastOffset: 6}}
	if lag, err := consumerLag([]int{0, 1}, committed, bounds); err != nil || lag != 6 {
		t.Fatalf("multi-partition lag = %d, %v, want 6", lag, err)
	}
	for _, test := range []struct {
		name      string
		committed []kafka.OffsetFetchPartition
		bounds    []kafka.PartitionOffsets
	}{
		{"missing commit", committed[:1], bounds},
		{"missing bounds", committed, bounds[:1]},
		{"commit error", []kafka.OffsetFetchPartition{{Partition: 0, Error: kafka.GroupAuthorizationFailed}}, bounds},
		{"bounds error", committed, []kafka.PartitionOffsets{{Partition: 0, Error: kafka.UnknownTopicOrPartition}}},
		{"invalid bounds", committed, []kafka.PartitionOffsets{{Partition: 0, FirstOffset: 5, LastOffset: 2}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := consumerLag([]int{0, 1}, test.committed, test.bounds); err == nil {
				t.Fatal("incomplete or failed collection must return an error")
			}
		})
	}
}
