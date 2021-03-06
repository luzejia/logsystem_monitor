package kafka

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	metrics "k8s.io/metrics/pkg/client/clientset/versioned"
)

func Count(clsuter string, KafkaClusterAddr []string) {
	fmt.Println("\n", clsuter, "kafka cluster status:\n")
	fmt.Println("kafka addr", KafkaClusterAddr)
	clientID := "kafka_exporter"
	config := sarama.NewConfig()
	config.ClientID = clientID
	kafkaVersion, _ := sarama.ParseKafkaVersion("2.6.0")
	config.Version = kafkaVersion
	kafkaclient, err := sarama.NewClient(KafkaClusterAddr, config)
	if err != nil {
		fmt.Println(err)
	}
	// offset如果被并发操作记得需要上锁！！
	offset := make(map[string]map[int32]int64)

	topics, err := kafkaclient.Topics()
	if err != nil {
		fmt.Println(err)
	}
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

	fmt.Println("consumer group:")
	kafkabroker := kafkaclient.Brokers()
	fmt.Println(kafkabroker)
	for _, broker := range kafkabroker {
		fmt.Println(broker.ID(), broker.Addr())
		groups, err := broker.ListGroups(&sarama.ListGroupsRequest{})
		if err != nil {
			fmt.Println(err)
			return
		}
		if groups != nil {
			fmt.Println("groups.Groups:", groups.Groups)
		} else {
			return
		}
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
	time.Sleep(time.Duration(15) * time.Second)
	var produce_offset_after int64
	for _, partition := range filebeat_partitions {
		currentOffset, _ := kafkaclient.GetOffset("filebeat-data", partition, sarama.OffsetNewest)
		produce_offset_after += currentOffset
	}

	var produce_speed int64
	produce_speed = produce_offset_after - producr_offset_now
	produce_speed = produce_speed / 15
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
	time.Sleep(time.Duration(15) * time.Second)
	offset2 := count()
	fmt.Println("consume speed:")

	for key1, value1 := range offset2 {
		fmt.Println("group:", key1)
		for key2, value2 := range value1 {
			fmt.Println("topic:", key2)
			fmt.Println("consume speed:", (value2-offset1[key1][key2])/15)
		}
	}
	defer kafkaclient.Close()
}

func Kafka_top() {
	// var kubeconfig, master string //empty, assuming inClusterConfig
	// config, err := clientcmd.BuildConfigFromFlags(master, kubeconfig)
	config, err := clientcmd.BuildConfigFromFlags("", "/root/.kube/config")
	if err != nil {
		panic(err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	fmt.Println("pod status:")
	pods, _ := clientset.CoreV1().Pods("default").List(context.Background(), metav1.ListOptions{})
	for _, pod := range pods.Items {
		fmt.Printf("Name: %s, Status: %s, CreateTime: %s\n", pod.ObjectMeta.Name, pod.Status.Phase, pod.ObjectMeta.CreationTimestamp)
	}

	fmt.Println("pvc status:")
	pvcs, _ := clientset.CoreV1().PersistentVolumeClaims("default").List(context.Background(), metav1.ListOptions{})
	for _, pvc := range pvcs.Items {
		fmt.Printf("Name: %s, Status: %s, CreateTime: %s Size: %s \n", pvc.ObjectMeta.Name, pvc.Status.Phase, pvc.ObjectMeta.CreationTimestamp, pvc.Spec.Resources.Requests.Storage().String())
	}

	mc, err := metrics.NewForConfig(config)
	if err != nil {
		panic(err)
	}
	podMetrics, err := mc.MetricsV1beta1().PodMetricses("default").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	for _, podMetric := range podMetrics.Items {
		fmt.Println("namespace:", podMetric.Namespace, "name:", podMetric.Name)
		podContainers := podMetric.Containers
		var podCpu int
		var podMemory int
		for _, container := range podContainers {
			cpuQuantity := container.Usage.Cpu().String()
			memQuantity := container.Usage.Memory().String()
			msg := fmt.Sprintf("Container Name: %s \n CPU usage: %s Memory usage: %s ", container.Name, cpuQuantity, memQuantity)
			fmt.Println(msg)
			if cpuQuantity != "0" {
				containerCpuNum, _ := strconv.Atoi(cpuQuantity[:(len(cpuQuantity) - 1)])
				podCpu += containerCpuNum
			}
			if memQuantity != "0" {
				containerMemoryNum, _ := strconv.Atoi(memQuantity[:(len(memQuantity) - 2)])
				podMemory += containerMemoryNum
			}
		}
		// ms = 1000us = 1000 * 1000 ns , Mi = 1024 Ki
		fmt.Println("pod cpu usage:", int(math.Ceil(float64(podCpu)/1000/1000)), "m", "pod memory usage:", math.Ceil(float64(podMemory)/1024), "Mi")
		fmt.Println("\n")
	}
}
