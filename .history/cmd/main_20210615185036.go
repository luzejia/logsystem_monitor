package main

import (
	//"github.com/luzejia/logsystem_monitor/pkg/elasticsearch"
	"github.com/luzejia/logsystem_monitor/pkg/kafka"
)

func setup(kafkaAddr *map[string][]string) {
	//(*kafkaAddr)["uae-a1"] = []string{"kafka-a1-log-headless.uae-system.svc.c1.uae:9092"}
	//(*kafkaAddr)["uae-a2"] = []string{"kafka-a2-log-headless.uae-system.svc.c2.uae:9092"}
	//(*kafkaAddr)["uae-a3"] = []string{"kafka-a3-log-headless.uae-system.svc.c3.uae:9092"}
	//(*kafkaAddr)["uae-a4"] = []string{"kafka-headless.uae-system.svc.a4.uae:9092"}
	(*kafkaAddr)["uae-a4"] = []string{"2002:ac15:7d98:1:0:ff:aac:1a"}
	//(*kafkaAddr)["uae-c1"] = []string{"kafka-headless.uae-system.svc.c1.uae:9092"}
	//(*kafkaAddr)["uae-c2"] = []string{"kafka-headless.uae-system.svc.c2.uae:9092"}
	//(*kafkaAddr)["uae-c3"] = []string{"kafka-headless.uae-system.svc.c3.uae:9092"}
}

func main() {
	//elasticsearch.Count()
	KunKafkaClusterAddr := make(map[string][]string)
	setup(&KunKafkaClusterAddr)
	for cluster, kafkaAddr := range KunKafkaClusterAddr {
		kafka.Count(cluster, kafkaAddr)
	}
}
