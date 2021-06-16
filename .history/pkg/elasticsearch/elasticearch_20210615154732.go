package elasticsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

type Index struct {
	Name  string `json:"index"`
	Size  string `json:"store.size"`
	Count string `json:"docs.count"`
}

// 截取小数位数
func FloatRound(f float64, n int) float64 {
	format := "%." + strconv.Itoa(n) + "f"
	res, _ := strconv.ParseFloat(fmt.Sprintf(format, f), 64)
	return res
}

func Count() {
	Querycmd := fmt.Sprintf("curl -s -X GET \"elasticsearch-logging.uae-system.svc.a1.uae:9200/_cat/indices/uae-*?v=true&h=index,store.size,docs.count&bytes=kb&format=json\"")

	fmt.Println("run:", Querycmd)
	cmd := exec.Command("/bin/bash", "-c", Querycmd)
	//创建获取命令输出管道
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Printf("Error:can not obtain stdout pipe for command:%s\n", err)
		return
	}
	//执行命令
	if err := cmd.Start(); err != nil {
		fmt.Println("Error:The command is err,", err)
		return
	}
	//读取所有输出
	mybytes, err := ioutil.ReadAll(stdout)
	if err != nil {
		fmt.Println("ReadAll Stdout:", err.Error())
		return
	}
	if err := cmd.Wait(); err != nil {
		fmt.Println("wait:", err.Error())
		return
	}

	nTime := time.Now()
	yesTime := nTime.AddDate(0, 0, -1)
	yesterday := yesTime.Format("2006.01.02")

	fmt.Println("transform to Json:")
	result := make([]Index, 0)
	reader := bytes.NewReader(mybytes)
	decoder := json.NewDecoder(reader)
	decoder.Decode(&result)
	var SizeCount int64 = 0
	var NumCount int64 = 0
	namespacecount := make(map[string]map[string]map[string]int64)
	cluster_count := make(map[string]map[string]int64)
	for _, index := range result {
		if strings.Contains(index.Name, yesterday) {
			// count all
			indexSize, _ := strconv.ParseInt(index.Size, 10, 64)
			messagenum, _ := strconv.ParseInt(index.Count, 10, 64)
			SizeCount += indexSize
			NumCount += messagenum

			// set cluster count map, in order to count by cluster
			if namespacecount[index.Name[:6]] == nil {
				namespacecount[index.Name[:6]] = make(map[string]map[string]int64)
			}
			namespacecount[index.Name[:6]][index.Name] = make(map[string]int64)
			namespacecount[index.Name[:6]][index.Name]["message_num"] = messagenum
			namespacecount[index.Name[:6]][index.Name]["message_size"] = indexSize
			// [:6] is use to get cluster nmae like uae-xx
			if cluster_count[index.Name[:6]] == nil {
				cluster_count[index.Name[:6]] = make(map[string]int64)
			}
			cluster_count[index.Name[:6]]["message_num"] += messagenum
			cluster_count[index.Name[:6]]["message_size"] += indexSize
		}
	}

	fmt.Println("kun every namespace message status:")
	for cluster, cluster_namespace := range namespacecount {
		fmt.Println("the namespace of", cluster, ":")
		for namesapce, value := range cluster_namespace {
			//fmt.Println("namespace:", namesapce, ",", " message_count:", value["message_num"], ",", " message_size:", value["message_size"]/1024, "Mb")
			fmt.Printf("namespace: %-40s, message_count: %-11d, message_size: %10dMb\n", namesapce, value["message_num"], value["message_size"]/1024)
		}
	}

	fmt.Println("kun every cluster message status:")
	for key, value := range cluster_count {
		fmt.Println("cluster:", key, ",", " message_count:", value["message_num"], ",", " message_size:", value["message_size"]/1024/1024, "Gb")
		fmt.Printf("cluster: %-15s, message_count: %-11d, message_size: %10dGb\n", key, value["message_num"], value["message_size"]/1024/1024)
	}

	fmt.Println(yesterday, "kun all namesapce message size:", FloatRound(float64(SizeCount)/1024/1024/1024, 2), "Tb", "kun all message message count:", NumCount)
	fmt.Printf("%s kun all message message count:%-11d, message size:%10fTb\n", yesterday, FloatRound(float64(SizeCount)/1024/1024/1024, 2), NumCount)
}
