package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"go.etcd.io/etcd/clientv3"
)

var client *clientv3.Client

// LogEntryConf 日志收集配置
type LogEntryConf struct {
	FilePath string `json:"filepath"`
	Topic    string `json:"topic"`
}

// Init 初始化 etcd
func Init(address []string) (err error) {
	client, err = clientv3.New(clientv3.Config{
		Endpoints:   address,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return
	}
	return
}

// GetLogConf 获取日志配置
func GetLogConf(key string) (configs []*LogEntryConf, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := client.Get(ctx, key)
	cancel()
	if err != nil {
		fmt.Printf("get from etcd failed, err:%v\n", err)
		return
	}
	for _, ev := range resp.Kvs {
		err = json.Unmarshal(ev.Value, &configs)
		return
	}

	return

}

// Put 更新或新建数据
func Put(k, v string) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, err = client.Put(ctx, k, v)
	cancel()
	if err != nil {
		return
	}
	return
}

// Get 获取 key 对应的数据
func Get(key string) (resp *clientv3.GetResponse, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err = client.Get(ctx, key)
	cancel()
	if err != nil {
		fmt.Printf("get from etcd failed, err:%v\n", err)
		return
	}
	return
}


// WatchConf 监听配置改动
func WatchConf(key string, upChan chan []*LogEntryConf) {
	rch := client.Watch(context.Background(), key) // <-chan WatchResponse
	for wresp := range rch {
		for _, ev := range wresp.Events {
			// 如果配置有修改，把数据发送到 chan 中
			var updateConf []*LogEntryConf
			err := json.Unmarshal(ev.Kv.Value, &updateConf)
			if err != nil {
				log.Printf("WatchConf Unmarshal err: %v", err)
				continue
			}
			upChan <- updateConf

			log.Printf("监听到配置更新 Type: %s Key:%s Value:%s\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
		}
	}
}
