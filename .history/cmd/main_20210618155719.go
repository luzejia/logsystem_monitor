package main

import (
	"github.com/luzejia/logsystem_monitor/pkg/elasticsearch"
	"github.com/luzejia/logsystem_monitor/pkg/kafka"
)

func setup(kafkaAddr *map[string][]string) {
	(*kafkaAddr)["my-kafka-cluster1"] = []string{"my-kafka-headless.default.svc.cluster.local:9092"}
	(*kafkaAddr)["my-kafka-cluster2"] = []string{"ipv4:9092"}
	(*kafkaAddr)["my-kafka-cluster3"] = []string{"[ipv6]:9092"}
}

func main() {
	elasticsearch.Count()
	KunKafkaClusterAddr := make(map[string][]string)
	setup(&KunKafkaClusterAddr)
	for cluster, kafkaAddr := range KunKafkaClusterAddr {
		kafka.Count(cluster, kafkaAddr)
	}
	kafka.Kafka_top()
}
