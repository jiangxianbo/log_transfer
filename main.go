package main

import (
	"fmt"
	"gopkg.in/ini.v1"
	"logTransfer/conf"
	"logTransfer/es"
	"logTransfer/kafka"
)

// log transfer
// 将日志数据取出发往es
var (
	cfg = new(conf.LogTransfer)
)

func main() {
	// 0.加载配置文件
	err := ini.MapTo(cfg, "./conf/cfg.ini")
	if err != nil {
		fmt.Printf("ini.MapTo failed, err:%v\n", err)
		return
	}
	// 1.初始化es
	// 1.1 初始化ES链接的一个client
	err = es.Init(cfg.ESCfg.Address, cfg.ESCfg.ChanSize, cfg.ESCfg.Nums)
	if err != nil {
		fmt.Printf("es.Init failed, err:%v\n", err)
		return
	}

	// 2.初始化kafka
	// 2.1 链接kafka，创建分区消费者
	// 2.2 每个分区的消费者分别取出数据，通过SendES()将数据发往ES
	err = kafka.Init([]string{cfg.KafkaCfg.Address}, cfg.KafkaCfg.Topic)
	if err != nil {
		fmt.Printf("kafka.Init failed, err:%v\n", err)
		return
	}

	select {}
}
