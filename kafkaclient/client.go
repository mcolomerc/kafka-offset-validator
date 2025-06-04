package kafkaclient

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaClient struct {
	sourceAdmin *kafka.AdminClient
	destAdmin   *kafka.AdminClient
}

// Union of all topic/partitions assigned in either cluster
type PartitionOffset struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Cluster   string `json:"cluster"` // "source" or "destination"
}

func NewKafkaClient(sourceConfig, destConfig *kafka.ConfigMap) (*KafkaClient, error) {
	sourceAdmin, err := kafka.NewAdminClient(sourceConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create source producer: %w", err)
	}
	destAdmin, err := kafka.NewAdminClient(destConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create source producer: %w", err)
	}
	return &KafkaClient{
		sourceAdmin: sourceAdmin,
		destAdmin:   destAdmin,
	}, nil
}

type ValidateResponse struct {
	GroupID       string                       `json:"group_id"`
	Matches       bool                         `json:"matches"`
	SourceOffsets map[string][]PartitionOffset `json:"source_offsets"`
	DestOffsets   map[string][]PartitionOffset `json:"destination_offsets"`
}

func (kc *KafkaClient) GetClusterAdmin(cluster string) (*kafka.AdminClient, error) {
	if cluster == "source" {
		return kc.sourceAdmin, nil
	} else if cluster == "destination" {
		return kc.destAdmin, nil
	}
	return nil, fmt.Errorf("invalid cluster: %s", cluster)
}

func (kc *KafkaClient) ValidateConsumerGroupOffsets(ctx context.Context, groupID string, topics []string) (ValidateResponse, error) {
	fmt.Println("Validating consumer group offsets for group:", groupID, "on topics:", topics)

	// Add a map to return an object result with the information of the offsets for the consumer group on each cluster
	// Include a boolean indicating if the offsets match
	response := ValidateResponse{
		GroupID:       groupID,
		Matches:       true,
		SourceOffsets: make(map[string][]PartitionOffset),
		DestOffsets:   make(map[string][]PartitionOffset),
	}

	result := make(map[string][]PartitionOffset)

	// List offsets for the consumer group in the source cluster
	sourceOffsets, err := kc.ListOffsets(ctx, kc.sourceAdmin, groupID, topics, true)
	if err != nil {
		return response, fmt.Errorf("failed to list offsets - source cluster - ensure topics exists: %w", err)
	}

	fmt.Println("Source offsets:", sourceOffsets)
	// List offsets for the consumer group in the destination cluster
	destOffsets, err := kc.ListOffsets(ctx, kc.destAdmin, groupID, topics, true)
	if err != nil {
		return response, fmt.Errorf("failed to list offsets - destination cluster - ensure topics exists: %w", err)
	}
	fmt.Println("Destination offsets:", destOffsets)
	// Compare offsets for each topic
	for _, topic := range topics {
		if sourceOffsets == nil || sourceOffsets.ConsumerGroupsTopicPartitions == nil {
			fmt.Printf("No offsets found for topic %s in source cluster\n", topic)
			return response, fmt.Errorf("failed to list offsets: %w", err)
		}
		if destOffsets == nil || destOffsets.ConsumerGroupsTopicPartitions == nil {
			fmt.Printf("No offsets found for topic %s in destination cluster\n", topic)
			continue
		}
		for _, resTopic := range sourceOffsets.ConsumerGroupsTopicPartitions {
			if resTopic.Group == groupID {
				for _, topicPartition := range resTopic.Partitions {
					if topicPartition.Topic != nil && *topicPartition.Topic == topic {
						result[groupID] = append(result[groupID], PartitionOffset{
							Topic:     *topicPartition.Topic,
							Partition: topicPartition.Partition,
							Offset:    int64(topicPartition.Offset),
							Cluster:   "source",
						})
						response.SourceOffsets[groupID] = append(response.SourceOffsets[groupID], PartitionOffset{
							Topic:     topic,
							Partition: topicPartition.Partition,
							Offset:    int64(topicPartition.Offset),
							Cluster:   "source",
						})
						fmt.Println("Source offset for topic", *topicPartition.Topic, "partition", topicPartition.Partition, "is", topicPartition.Offset)
					}
				}
			}
		}
		for _, resTopic := range destOffsets.ConsumerGroupsTopicPartitions {
			if resTopic.Group == groupID {
				for _, topicPartition := range resTopic.Partitions {
					if topicPartition.Topic != nil && *topicPartition.Topic == topic {
						result[groupID] = append(result[groupID], PartitionOffset{
							Topic:     *topicPartition.Topic,
							Partition: topicPartition.Partition,
							Offset:    int64(topicPartition.Offset),
							Cluster:   "destination",
						})
						response.DestOffsets[groupID] = append(response.DestOffsets[groupID], PartitionOffset{
							Topic:     *topicPartition.Topic,
							Partition: topicPartition.Partition,
							Offset:    int64(topicPartition.Offset),
							Cluster:   "destination",
						})
						fmt.Println("Destination offset for topic", *topicPartition.Topic, "partition", topicPartition.Partition, "is", topicPartition.Offset)
					}
				}
			}
		}
	}
	// Compare the offsets between the two clusters
	for _, partitions := range result {
		sourceOffsets := make(map[string]int64)
		destOffsets := make(map[string]int64)
		for _, p := range partitions {
			if p.Cluster == "source" {
				sourceOffsets[fmt.Sprintf("%s-%d", p.Topic, p.Partition)] = p.Offset
			} else if p.Cluster == "destination" {
				destOffsets[fmt.Sprintf("%s-%d", p.Topic, p.Partition)] = p.Offset
			}
		}
		for key, sourceOffset := range sourceOffsets {
			destOffset, exists := destOffsets[key]
			if !exists {
				fmt.Printf("Offset for %s in destination cluster does not exist\n", key)
				response.Matches = false
			}
			if sourceOffset != destOffset {
				fmt.Printf("Offsets do not match for %s: source=%d, destination=%d\n", key, sourceOffset, destOffset)
				response.Matches = false
			}
			fmt.Printf("Offsets match for %s: %d\n", key, sourceOffset)
		}
	}
	return response, nil
}

func (kc *KafkaClient) ListOffsets(
	ctx context.Context,
	a *kafka.AdminClient,
	groupID string,
	topics []string,
	requireStable bool,
) (*kafka.ListConsumerGroupOffsetsResult, error) {
	fmt.Println("Listing offsets for group:", groupID, "on topics:", topics)

	var gps []kafka.ConsumerGroupTopicPartitions
	for _, topic := range topics {
		// Describe the topic to get all partitions
		describeResult, err := a.DescribeTopics(
			ctx, kafka.NewTopicCollectionOfTopicNames([]string{topic}),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to describe topic %s: %w", topic, err)
		}
		partitions := []kafka.TopicPartition{}
		for _, topicDesc := range describeResult.TopicDescriptions {
			if topicDesc.Name != topic || topicDesc.Error.Code() != kafka.ErrNoError {
				continue
			}
			for _, p := range topicDesc.Partitions {
				partitions = append(partitions, kafka.TopicPartition{
					Topic:     &topic,
					Partition: int32(p.Partition),
				})
			}
		}
		gps = append(gps, kafka.ConsumerGroupTopicPartitions{
			Group:      groupID,
			Partitions: partitions,
		})
	}
	res, err := a.ListConsumerGroupOffsets(
		ctx, gps, kafka.SetAdminRequireStableOffsets(requireStable))
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer group offsets: %w", err)
	}
	return &res, nil
}

func (kc *KafkaClient) DescribeTopic(ctx context.Context, a *kafka.AdminClient, topics []string) (kafka.DescribeTopicsResult, error) {
	fmt.Println("Describing topics:", topics)
	// Describe a topic in the Kafka cluster
	describeTopicsResult, err := a.DescribeTopics(
		ctx, kafka.NewTopicCollectionOfTopicNames(topics),
		kafka.SetAdminOptionIncludeAuthorizedOperations(true),
	)
	if err != nil {
		fmt.Printf("Failed to describe topics: %s\n", err)
		return kafka.DescribeTopicsResult{}, err
	}
	return describeTopicsResult, nil
}

func (kc *KafkaClient) GetOffsetsAtTimestamp(
	ctx context.Context,
	a *kafka.AdminClient,
	topic string,
	timestamp int64,
) ([]kafka.TopicPartition, error) {
	fmt.Printf("Getting offsets for topic %s at timestamp %d\n", topic, timestamp)

	// First describe the topic to get all partitions
	describeResult, err := a.DescribeTopics(
		ctx, kafka.NewTopicCollectionOfTopicNames([]string{topic}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to describe topic %s: %w", topic, err)
	}

	var partitions []kafka.TopicPartition
	for _, topicDesc := range describeResult.TopicDescriptions {
		if topicDesc.Name != topic || topicDesc.Error.Code() != kafka.ErrNoError {
			continue
		}
		for _, p := range topicDesc.Partitions {
			partitions = append(partitions, kafka.TopicPartition{
				Topic:     &topic,
				Partition: int32(p.Partition),
			})
		}
	}

	// Build the map of TopicPartition to OffsetSpec for the given timestamp
	offsets := make(map[kafka.TopicPartition]kafka.OffsetSpec)
	for _, tp := range partitions {
		var offsetSpec kafka.OffsetSpec
		switch timestamp {
		case -1:
			offsetSpec = kafka.NewOffsetSpecForTimestamp(int64(kafka.OffsetEnd))
		case -2:
			offsetSpec = kafka.NewOffsetSpecForTimestamp(int64(kafka.OffsetBeginning))
		case 0:
			// If timestamp is 0 (not provided), use latest
			offsetSpec = kafka.NewOffsetSpecForTimestamp(int64(kafka.OffsetEnd))
		default:
			offsetSpec = kafka.NewOffsetSpecForTimestamp(timestamp)
		}
		offsets[tp] = offsetSpec
	}

	// Use ListOffsets to get offsets at the specified timestamp
	res, err := a.ListOffsets(ctx, offsets, kafka.SetAdminIsolationLevel(kafka.IsolationLevelReadCommitted))
	if err != nil {
		return nil, fmt.Errorf("failed to list offsets at timestamp: %w", err)
	}

	// Extract the actual offset values from the response
	var topicPartitions []kafka.TopicPartition
	for tp, resultInfo := range res.ResultInfos {
		if resultInfo.Error.Code() != kafka.ErrNoError {
			fmt.Printf("Error getting offset for partition %d: %v\n", tp.Partition, resultInfo.Error)
			continue
		}
		topicPartitions = append(topicPartitions, kafka.TopicPartition{
			Topic:     tp.Topic,
			Partition: tp.Partition,
			Offset:    resultInfo.Offset,
		})
		fmt.Printf("Found offset %d for partition %d at timestamp %d\n", resultInfo.Offset, tp.Partition, timestamp)
	}
	return topicPartitions, nil
}
