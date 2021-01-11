package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"log-agent-go/config"
	"log-agent-go/etcd"
	"log-agent-go/kafka"
	"log-agent-go/taillog"
	"sync"

	yaml "gopkg.in/yaml.v2"
)

func main() {
	// 从配置文件初始化 server 配置项
	conf := new(config.ServerConf)
	yamlFile, err := ioutil.ReadFile("config/server.yaml")
	if err != nil {
		log.Printf("yamlFile Get err #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, conf)
	if err != nil {
		log.Fatalf("yaml conf Unmarshal: %v", err)
	}

	// 初始化 kafka
	err = kafka.Init(conf.KafkaConf.Address, conf.ChanMaxSize)
	if err != nil {
		log.Printf("init kafka error : %v \n", err)
		return
	}
	log.Println("kafka 初始化成功")

	// 初始化 etcd
	err = etcd.Init(conf.EtcdConf.Address)
	if err != nil {
		log.Printf("init etcd error : %v \n", err)
		return
	}
	log.Println("etcd 初始化成功")

	var wg sync.WaitGroup
	// 从 etcd 获取日志收集项目的配置信息
	logKey := fmt.Sprintf(conf.TaillogConf.Key, conf.Name)

	logConfgis, err := etcd.GetLogConf(logKey)
	wg.Add(1)
	// 监听 etcd 的日志配置项变化
	go etcd.WatchConf(conf.TaillogConf.Key, taillog.TaskManager.UpdateConfChan)

	taillog.Run(logConfgis)
	wg.Wait()

}
