package es

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/olivere/elastic/v7"
)

// 初始化es,准备接受kafka那边发来的数据
type LogData struct {
	Topic string `json:"topic"`
	Data  string `json:"data"`
}

var (
	client *elastic.Client
	ch     chan *LogData
)

// Init ...
func Init(address string, chanSize, nums int) (err error) {
	if !strings.HasPrefix(address, "http://") {
		address = "http://" + address
	}
	client, err = elastic.NewClient(elastic.SetURL(address))
	if err != nil {
		panic(err)
	}
	fmt.Println("connect to es sucess")
	ch = make(chan *LogData, chanSize)
	for i := 0; i < nums; i++ {
		go SendToES()
	}
	return
}

// SendToESChan 异步发送到一个通道
func SendToESChan(msg *LogData) {
	ch <- msg
}

// SendToES 发送数据到ES
func SendToES() {
	// 链式操作
	for {
		select {
		case msg := <-ch:
			put1, err := client.Index().Index(msg.Topic).BodyJson(msg).Do(context.Background())
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Printf("Indexed user %s to index %s, type %s\n", put1.Id, put1.Index, put1.Type)
		default:
			time.Sleep(time.Second)
		}
	}
}
