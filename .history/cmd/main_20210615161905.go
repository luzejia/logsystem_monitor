package main

import (
	"github.com/luzejia/logsystem_monitor/pkg/elasticsearch"
	"github.com/luzejia/logsystem_monitor/pkg/kafka"
)

func main() {
	elasticsearch.Count()
	KunKafkaClusterAddr := make(map[string][]string)
	//KunKafkaClusterAddr := []string{"kafka-a1-log-headless.uae-system.svc.c1.uae:9092"}
	for cluster, kafkaAddr := range KunKafkaClusterAddr {
		kafka.Count(cluster, kafkaAddr)
	}
}
