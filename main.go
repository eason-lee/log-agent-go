package main

import (
	"log-agent-go/config"
	"log-agent-go/etcd"
	"log-agent-go/handler"
	"log-agent-go/service"
	"log-agent-go/taillog"
	"log-agent-go/utils"
	"log-agent-go/writer"
)

func main() {
	conf := config.GetConf("config/config.yaml")

	group := service.NewServiceGroup()
	defer group.Stop()

	w := writer.NewWriter(&conf.KafkaConf)

	etcdClient := etcd.NewClient(&conf.EtcdConf)

	services := make(map[string]service.Service)
	for _, val := range conf.EtcdConf.Vals {
		server := taillog.NewTailTask(val.Filepath, val.Topic, w, conf, etcdClient)
		group.Add(server)
		services[utils.GetTailTaskKey(val.Filepath, val.Topic)] = server
	}

	if conf.EtcdConf.EnableWatch {
		go handler.ListenUpdateConf(services, group, conf, etcdClient)
	}

	group.Start()
}
