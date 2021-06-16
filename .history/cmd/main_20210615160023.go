package main

import (
	"github.com/luzejia/logsystem_monitor/pkg/elasticsearch"
	"github.com/luzejia/logsystem_monitor/pkg/kafka"
)

func main() {
	elasticsearch.Count()
	kafka.Count()
}
