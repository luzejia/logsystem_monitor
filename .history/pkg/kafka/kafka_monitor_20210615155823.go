package kafka

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

func Count() {
	clientID := "kafka_exporter"
	config := sarama.NewConfig()
	config.ClientID = clientID
	kafkaVersion, _ := sarama.ParseKafkaVersion("2.6.0")
	config.Version = kafkaVersion
	KafkaClusterAddr := []string{"kafka-headless.prj-luzejia-test.svc.c1.uae:29092"}
	kafkaclient, _ := sarama.NewClient(KafkaClusterAddr, config)

	// offset如果被并发操作记得需要上锁！！
	offset := make(map[string]map[int32]int64)

	topics, _ := kafkaclient.Topics()
	fmt.Println("kafka topic:")
	for _, topic := range topics {
		fmt.Println("topic:", topic)
		partitions, _ := kafkaclient.Partitions(topic)
		offset[topic] = make(map[int32]int64, len(partitions))
		for _, partition := range partitions {
			currentOffset, _ := kafkaclient.GetOffset(topic, partition, sarama.OffsetNewest)
			offset[topic][partition] = currentOffset
			fmt.Println("partition:", partition, ",", "offset", currentOffset)
		}
	}
	/*
		        // create partition for topic
				old_partitions, _ := kafkaclient.Partitions("prj-luzejia-test")
				fmt.Println("old partition:")
				for _, partition := range old_partitions {
					currentOffset, _ := kafkaclient.GetOffset("prj-luzejia-test", partition, sarama.OffsetNewest)
					fmt.Println("topic: prj-luzejia-test", ",", "partition:", partition, ",", "offset", currentOffset)
				}
				fmt.Println("modify topic partition:")
			    // need to the controller broker,so you had better to range every broker to create
				test_broker, _ := kafkaclient.Broker(2)
				create_partiton_request := sarama.CreatePartitionsRequest{}
				create_partiton_request.TopicPartitions = make(map[string]*sarama.TopicPartition)
				create_partiton_request.TopicPartitions["prj-luzejia-test"] = &sarama.TopicPartition{}
				create_partiton_request.TopicPartitions["prj-luzejia-test"].Count = 6
				create_partiton_request.Timeout = time.Duration(10) * time.Second

				response, _ := test_broker.CreatePartitions(&create_partiton_request)
				fmt.Println(response.TopicPartitionErrors)

				new_partitions, _ := kafkaclient.Partitions("prj-luzejia-test")
				fmt.Println("new partition:")
				for _, partition := range new_partitions {
					currentOffset, _ := kafkaclient.GetOffset("prj-luzejia-test", partition, sarama.OffsetNewest)
					fmt.Println("topic: prj-luzejia-test", ",", "partition:", partition, ",", "offset", currentOffset)
				}
	*/
	fmt.Println("consumer group:")
	kafkabroker := kafkaclient.Brokers()
	for _, broker := range kafkabroker {
		groups, _ := broker.ListGroups(&sarama.ListGroupsRequest{})
		groupIds := make([]string, 0)
		for groupId := range groups.Groups {
			groupIds = append(groupIds, groupId)
		}
		describeGroups, _ := broker.DescribeGroups(&sarama.DescribeGroupsRequest{Groups: groupIds})
		for _, group := range describeGroups.Groups {
			offsetFetchRequest := sarama.OffsetFetchRequest{ConsumerGroup: group.GroupId, Version: 1}
			for _, member := range group.Members {
				assignment, err := member.GetMemberAssignment()
				if err != nil {
					fmt.Println("Cannot get GetMemberAssignment of group member %v : %v", member, err)
					return
				}
				for topic, partions := range assignment.Topics {
					for _, partition := range partions {
						offsetFetchRequest.AddPartition(topic, partition)
					}
				}
			}
			offsetFetchResponse, err := broker.FetchOffset(&offsetFetchRequest)
			if err != nil {
				fmt.Println("Cannot get offset of group %s: %v", group.GroupId, err)
				continue
			}

			for topic, partitions := range offsetFetchResponse.Blocks {
				// If the topic is not consumed by that consumer group, skip it
				topicConsumed := false
				for _, offsetFetchResponseBlock := range partitions {
					// Kafka will return -1 if there is no offset associated with a topic-partition under that consumer group
					if offsetFetchResponseBlock.Offset != -1 {
						topicConsumed = true
						break
					}
				}
				if !topicConsumed {
					continue
				}

				//var currentOffsetSum int64
				var lagSum int64
				for partition, offsetFetchResponseBlock := range partitions {
					err := offsetFetchResponseBlock.Err
					if err != sarama.ErrNoError {
						fmt.Println("Error for  partition %d :%v", partition, err.Error())
						continue
					}
					// currentOffset := offsetFetchResponseBlock.Offset
					// currentOffsetSum += currentOffset
					if offset, ok := offset[topic][partition]; ok {
						// If the topic is consumed by that consumer group, but no offset associated with the partition
						// forcing lag to -1 to be able to alert on that
						var lag int64
						if offsetFetchResponseBlock.Offset == -1 {
							lag = -1
						} else {
							lag = offset - offsetFetchResponseBlock.Offset
							lagSum += lag
						}
					} else {
						fmt.Println("No offset of topic %s partition %d, cannot get consumer group lag", topic, partition)
					}
				}
				fmt.Println("group:", group.GroupId, "topic:", topic, "lag:", lagSum)
			}

		}
	}

	fmt.Println("produce speed:")
	var producr_offset_now int64
	filebeat_partitions, _ := kafkaclient.Partitions("filebeat-data")

	for _, partition := range filebeat_partitions {
		currentOffset, _ := kafkaclient.GetOffset("filebeat-data", partition, sarama.OffsetNewest)
		producr_offset_now += currentOffset
	}
	time.Sleep(time.Duration(100) * time.Second)
	var produce_offset_after int64
	for _, partition := range filebeat_partitions {
		currentOffset, _ := kafkaclient.GetOffset("filebeat-data", partition, sarama.OffsetNewest)
		produce_offset_after += currentOffset
	}

	var produce_speed int64
	produce_speed = produce_offset_after - producr_offset_now
	produce_speed = produce_speed / 100
	fmt.Println(produce_speed)

	count := func() map[string]map[string]int64 {
		result := make(map[string]map[string]int64)
		kafkabroker := kafkaclient.Brokers()
		for _, broker := range kafkabroker {
			groups, _ := broker.ListGroups(&sarama.ListGroupsRequest{})
			groupIds := make([]string, 0)
			for groupId := range groups.Groups {
				groupIds = append(groupIds, groupId)
			}
			describeGroups, _ := broker.DescribeGroups(&sarama.DescribeGroupsRequest{Groups: groupIds})
			for _, group := range describeGroups.Groups {
				result[group.GroupId] = make(map[string]int64)
				offsetFetchRequest := sarama.OffsetFetchRequest{ConsumerGroup: group.GroupId, Version: 1}
				for _, member := range group.Members {
					assignment, err := member.GetMemberAssignment()
					if err != nil {
						fmt.Println("Cannot get GetMemberAssignment of group member %v : %v", member, err)
					}
					for topic, partions := range assignment.Topics {
						for _, partition := range partions {
							offsetFetchRequest.AddPartition(topic, partition)
						}
					}
				}
				offsetFetchResponse, err := broker.FetchOffset(&offsetFetchRequest)
				if err != nil {
					fmt.Println("Cannot get offset of group %s: %v", group.GroupId, err)
					continue
				}

				for topic, partitions := range offsetFetchResponse.Blocks {
					// If the topic is not consumed by that consumer group, skip it
					topicConsumed := false
					for _, offsetFetchResponseBlock := range partitions {
						// Kafka will return -1 if there is no offset associated with a topic-partition under that consumer group
						if offsetFetchResponseBlock.Offset != -1 {
							topicConsumed = true
							break
						}
					}
					if !topicConsumed {
						continue
					}

					//var currentOffsetSum int64
					var lagSum int64
					for partition, offsetFetchResponseBlock := range partitions {
						err := offsetFetchResponseBlock.Err
						if err != sarama.ErrNoError {
							fmt.Println("Error for  partition %d :%v", partition, err.Error())
							continue
						}
						// currentOffset := offsetFetchResponseBlock.Offset
						// currentOffsetSum += currentOffset
						if offset, ok := offset[topic][partition]; ok {
							// If the topic is consumed by that consumer group, but no offset associated with the partition
							// forcing lag to -1 to be able to alert on that
							var lag int64
							if offsetFetchResponseBlock.Offset == -1 {
								lag = -1
							} else {
								result[group.GroupId][topic] += offsetFetchResponseBlock.Offset
								lag = offset - offsetFetchResponseBlock.Offset
								lagSum += lag
							}
						} else {
							fmt.Println("No offset of topic %s partition %d, cannot get consumer group lag", topic, partition)
						}
					}
				}
			}
		}
		return result
	}
	offset1 := count()
	time.Sleep(time.Duration(100) * time.Second)
	offset2 := count()
	fmt.Println("consume speed:")

	for key1, value1 := range offset2 {
		fmt.Println("group:", key1)
		for key2, value2 := range value1 {
			fmt.Println("topic:", key2)
			fmt.Println("consume speed:", (value2-offset1[key1][key2])/100)
		}
	}
	defer kafkaclient.Close()
}
