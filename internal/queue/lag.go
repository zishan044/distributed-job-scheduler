package queue

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// ConsumerLag counts retained offsets awaiting group commits. Executors start
// at the first available offset when a partition has no usable committed offset.
func ConsumerLag(ctx context.Context, client *kafka.Client, topic, group string) (int64, error) {
	metadata, err := client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		return 0, err
	}
	if len(metadata.Topics) != 1 || metadata.Topics[0].Name != topic {
		return 0, fmt.Errorf("missing metadata for topic %s", topic)
	}
	info := metadata.Topics[0]
	if info.Error != nil {
		return 0, fmt.Errorf("topic %s: %w", topic, info.Error)
	}
	if len(info.Partitions) == 0 {
		return 0, fmt.Errorf("topic %s has no partitions", topic)
	}
	partitions := make([]int, 0, len(info.Partitions))
	requests := make([]kafka.OffsetRequest, 0, 2*len(info.Partitions))
	for _, partition := range info.Partitions {
		if partition.Error != nil {
			return 0, fmt.Errorf("partition %d: %w", partition.ID, partition.Error)
		}
		partitions = append(partitions, partition.ID)
		requests = append(requests, kafka.FirstOffsetOf(partition.ID), kafka.LastOffsetOf(partition.ID))
	}
	committed, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{GroupID: group, Topics: map[string][]int{topic: partitions}})
	if err != nil {
		return 0, err
	}
	if committed.Error != nil {
		return 0, fmt.Errorf("group %s: %w", group, committed.Error)
	}
	// Read commits before end offsets so concurrent consumption cannot create
	// an apparent negative lag. This is a sample, not an atomic broker snapshot.
	bounds, err := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{Topics: map[string][]kafka.OffsetRequest{topic: requests}})
	if err != nil {
		return 0, err
	}
	return consumerLag(partitions, committed.Topics[topic], bounds.Topics[topic])
}

func consumerLag(partitions []int, committed []kafka.OffsetFetchPartition, bounds []kafka.PartitionOffsets) (int64, error) {
	commits := make(map[int]kafka.OffsetFetchPartition, len(committed))
	for _, partition := range committed {
		commits[partition.Partition] = partition
	}
	offsets := make(map[int]kafka.PartitionOffsets, len(bounds))
	for _, partition := range bounds {
		offsets[partition.Partition] = partition
	}
	var lag int64
	for _, id := range partitions {
		commit, ok := commits[id]
		if !ok {
			return 0, fmt.Errorf("missing committed offset for partition %d", id)
		}
		if commit.Error != nil {
			return 0, fmt.Errorf("partition %d commit: %w", id, commit.Error)
		}
		span, ok := offsets[id]
		if !ok {
			return 0, fmt.Errorf("missing offset bounds for partition %d", id)
		}
		if span.Error != nil {
			return 0, fmt.Errorf("partition %d offsets: %w", id, span.Error)
		}
		if span.FirstOffset < 0 || span.LastOffset < span.FirstOffset || commit.CommittedOffset < -1 {
			return 0, fmt.Errorf("invalid offsets for partition %d", id)
		}
		start := commit.CommittedOffset
		if start < span.FirstOffset || start > span.LastOffset {
			start = span.FirstOffset
		}
		lag += span.LastOffset - start
	}
	return lag, nil
}
