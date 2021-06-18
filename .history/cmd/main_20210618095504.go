package main

import (
	"github.com/luzejia/logsystem_monitor/pkg/elasticsearch"
	"github.com/luzejia/logsystem_monitor/pkg/kafka"
)

func setup(kafkaAddr *map[string][]string) {
	//(*kafkaAddr)["my-kafka"] = []string{"my-kafka-headless.default.svc.cluster.local:9092"}
	//(*kafkaAddr)["my-kafka"] = []string{"10.7.23.139:9092"}
	//(*kafkaAddr)["my-kafka-c1"] = []string{"[2002:ac12:dc05::fe:c13f:2c0b]:9092"}
	//(*kafkaAddr)["my-kafka-c2"] = []string{"[2002:a43:b808::fe:8f72:d173]:9092"}
	//(*kafkaAddr)["my-kafka-c3"] = []string{"[2002:a42:b276::fe:b632:e00d]:9092"}
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
