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

func Count(cluster string) {
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
	logDay := yesTime.Format("20060102")
	fmt.Println(logDay)

	fmt.Println("transform to Json:")
	result := make([]Index, 0)
	reader := bytes.NewReader(mybytes)
	decoder := json.NewDecoder(reader)
	decoder.Decode(&result)
	var SizeCount int64 = 0
	var NumCount int64 = 0
	for _, index := range result {
		if strings.Contains(index.Name, "2021.06.14") {
			indexSize, _ := strconv.ParseInt(index.Size, 10, 64)
			messagenum, _ := strconv.ParseInt(index.Count, 10, 64)
			SizeCount += indexSize
			NumCount += messagenum
		}
	}
	fmt.Println("2021-06-14", cluster, "size:", SizeCount/1024, "Mb", "message count:", NumCount)
}
