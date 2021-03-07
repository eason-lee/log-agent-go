package handler

import (
	"encoding/json"
	"log"
	"log-agent-go/config"
	"log-agent-go/etcd"
	"log-agent-go/service"
	"log-agent-go/taillog"
	"log-agent-go/utils"
	"log-agent-go/writer"
	"reflect"

	"gopkg.in/yaml.v2"
)

// ListenUpdateConf 监听 etcd 配置，动态更新服务
func ListenUpdateConf(tasks map[string]service.Service, group *service.ServiceGroup, conf *config.ServerConf, etcdClient *etcd.EtcdClient) {
	UpdateConfChan := make(chan []*etcd.LogEntryConf, 10)

	// 把现有的配置发送到 etcd 里
	key := conf.EtcdConf.Key
	vals := conf.EtcdConf.Vals
	val, err := json.Marshal(vals)
	if err != nil {
		log.Printf("json.marshal failed,err: %v", err)
	}

	if err != nil {
		log.Fatalf("unable to marshal config to YAML: %v", err)
	}

	etcdClient.Put(key, string(val))

	//监听 etcd key 的改动
	go etcdClient.WatchConf(conf.EtcdConf.Key, func(data []byte) (err error) {
		var updateConf []*etcd.LogEntryConf
		err = yaml.Unmarshal(data, &updateConf)
		if err != nil {
			log.Printf("WatchConf Unmarshal err: %v", err)
			return
		}

		UpdateConfChan <- updateConf
		return
	})

	for {
		select {
		case up := <-UpdateConfChan:
			for _, confg := range up {
				key := utils.GetTailTaskKey(confg.Filepath, confg.Topic)
				// 已有的配置不做改动
				_, ok := tasks[key]
				if ok {
					continue
				} else {
					// 创建新的 TailTask
					w := writer.NewWriter(&conf.KafkaConf)
					task := taillog.NewTailTask(confg.Filepath, confg.Topic, w, conf, etcdClient)
					group.DynamicAddAndStart(task)
					tasks[key] = task
				}
			}

			// 删除修改和删除的 TailTask
			for _, existTask := range tasks {
				isDelete := true
				val := reflect.ValueOf(existTask)
				et := val.Interface().(*taillog.TailTask)
				for _, newConf := range up {
					if newConf.Filepath == et.TailObj.Filename && newConf.Topic == et.Topic {
						isDelete = false
						continue
					}
				}
				if isDelete {
					et.Stop()
					delete(tasks, utils.GetTailTaskKey(et.Filepath, et.Topic))
				}
			}
			log.Printf("配置更新成功：%v", up)
		}
	}
}
