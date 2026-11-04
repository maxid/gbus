package kafka

import (
	"context"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/maxid/gbus"
)

// Lag implements gbus.LagReporter using Kafka metadata, ListOffsets and the
// consumer group's committed offsets. It is an admin/read path, not part of the
// consume hot path: callers should poll it at a low rate.
func (c *Consumer) Lag(ctx context.Context) ([]gbus.PartitionLag, error) {
	metaReq := kmsg.NewPtrMetadataRequest()
	for _, topic := range c.cfg.Topics {
		physical := c.namer.Topic(topic)
		entry := kmsg.NewMetadataRequestTopic()
		entry.Topic = &physical
		metaReq.Topics = append(metaReq.Topics, entry)
	}
	metaResponse, err := c.client.Request(ctx, metaReq)
	if err != nil {
		return nil, fmt.Errorf("gbus/kafka: lag metadata: %w", err)
	}
	metadata, ok := metaResponse.(*kmsg.MetadataResponse)
	if !ok {
		return nil, fmt.Errorf("gbus/kafka: lag metadata: unexpected %T", metaResponse)
	}

	offsetReq := kmsg.NewPtrListOffsetsRequest()
	offsetReq.ReplicaID = -1
	for _, topic := range metadata.Topics {
		if topic.ErrorCode != 0 {
			return nil, fmt.Errorf("gbus/kafka: lag metadata topic %v: %w", topic.Topic, kerr.ErrorForCode(topic.ErrorCode))
		}
		if topic.Topic == nil {
			continue
		}
		entry := kmsg.NewListOffsetsRequestTopic()
		entry.Topic = *topic.Topic
		for _, partition := range topic.Partitions {
			if partition.ErrorCode != 0 {
				return nil, fmt.Errorf("gbus/kafka: lag metadata %s/%d: %w", *topic.Topic, partition.Partition, kerr.ErrorForCode(partition.ErrorCode))
			}
			part := kmsg.NewListOffsetsRequestTopicPartition()
			part.Partition = partition.Partition
			part.Timestamp = -1 // latest
			entry.Partitions = append(entry.Partitions, part)
		}
		if len(entry.Partitions) > 0 {
			offsetReq.Topics = append(offsetReq.Topics, entry)
		}
	}
	offsetResponse, err := c.client.Request(ctx, offsetReq)
	if err != nil {
		return nil, fmt.Errorf("gbus/kafka: lag list offsets: %w", err)
	}
	offsets, ok := offsetResponse.(*kmsg.ListOffsetsResponse)
	if !ok {
		return nil, fmt.Errorf("gbus/kafka: lag list offsets: unexpected %T", offsetResponse)
	}

	committed := c.client.CommittedOffsets()
	var out []gbus.PartitionLag
	for _, topic := range offsets.Topics {
		for _, partition := range topic.Partitions {
			if partition.ErrorCode != 0 {
				return nil, fmt.Errorf("gbus/kafka: lag offsets %s/%d: %w", topic.Topic, partition.Partition, kerr.ErrorForCode(partition.ErrorCode))
			}
			end := partition.Offset
			if end < 0 {
				end = 0
			}
			var position int64
			if byPartition, ok := committed[topic.Topic]; ok {
				position = byPartition[partition.Partition].Offset
			}
			if position < 0 {
				position = 0
			}
			lag := end - position
			if lag < 0 {
				lag = 0
			}
			out = append(out, gbus.PartitionLag{
				Topic:     c.namer.Strip(topic.Topic),
				Partition: partition.Partition,
				Committed: position,
				End:       end,
				Lag:       lag,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Partition < out[j].Partition
	})
	return out, nil
}

var _ gbus.LagReporter = (*Consumer)(nil)
