package etcd

import (
	"context"
	"encoding/json"
	"log"
	"log-agent-go/config"
	"log-agent-go/utils"
	"time"

	"go.etcd.io/etcd/clientv3"
)

type EtcdClient struct {
	client *clientv3.Client
}

// LogEntryConf 日志收集配置
type LogEntryConf struct {
	Filepath string `yaml:"filepath"`
	Topic    string `yaml:"topic"`
}

// Init 初始化 etcd
func NewClient(conf *config.EtcdConf) *EtcdClient {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   conf.Address,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("etcd 初始化失败，err: %v\n", err)
	}

	utils.AddShutdownListener(func() {
		client.Close()
	})

	log.Println("etcd 初始化成功")
	return &EtcdClient{
		client: client,
	}
}

// GetLogConf 获取日志配置
func (c *EtcdClient) GetLogConf(key string) (configs []*LogEntryConf, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := c.client.Get(ctx, key)
	cancel()
	if err != nil {
		log.Printf("get from etcd failed, err:%v\n", err)
		return
	}
	for _, ev := range resp.Kvs {
		err = json.Unmarshal(ev.Value, &configs)
		return
	}

	return

}

// Put 更新或新建数据
func (c *EtcdClient) Put(k, v string) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, err = c.client.Put(ctx, k, v)
	cancel()
	if err != nil {
		return
	}
	return
}

// Get 获取 key 对应的数据
func (c *EtcdClient) Get(key string) (resp *clientv3.GetResponse, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err = c.client.Get(ctx, key)
	cancel()
	if err != nil {
		log.Printf("get from etcd failed, err:%v\n", err)
		return
	}
	return
}

// WatchConf 监听配置改动
func (c *EtcdClient) WatchConf(key string, f func([]byte) error) {
	rch := c.client.Watch(context.Background(), key) // <-chan WatchResponse

	for wresp := range rch {
		for _, ev := range wresp.Events {
			// 如果配置有修改，把数据发送到 chan 中
			if err := f(ev.Kv.Value); err != nil {
				continue
			}
			log.Printf("监听到配置更新 Type: %s Key:%s Value:%s\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
		}
	}
}
