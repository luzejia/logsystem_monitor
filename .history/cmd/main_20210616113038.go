package main

import (
	//"github.com/luzejia/logsystem_monitor/pkg/elasticsearch"
	"github.com/luzejia/logsystem_monitor/pkg/kafka"
)

func setup(kafkaAddr *map[string][]string) {
	(*kafkaAddr)["my-kafka"] = []string{"10.7.23.139:9092", "10.7.6.148:9092", "10.7.41.150:9092"}
}

func main() {
	//elasticsearch.Count()
	KunKafkaClusterAddr := make(map[string][]string)
	setup(&KunKafkaClusterAddr)
	for cluster, kafkaAddr := range KunKafkaClusterAddr {
		kafka.Count(cluster, kafkaAddr)
	}
	kafka.Kafka_top()
}
