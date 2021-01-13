package config

import (
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v2"
)

// ServerConf 服务配置
type ServerConf struct {
	Name        string `yaml:"server_name"`
	KafkaConf   `yaml:"kafka"`
	EtcdConf    `yaml:"etcd"`
	TaillogConf `yaml:"taillog"`
}

// KafkaConf ...
type KafkaConf struct {
	Address     []string `yaml:"address,flow"`
	ChanMaxSize int      `yaml:"chan_max_size"`
}

// EtcdConf ...
type EtcdConf struct {
	Address []string `yaml:"address,flow"`
}

// TaillogConf ...
type TaillogConf struct {
	Key string `yaml:"key"`
}

// Conf 配置
var Conf = new(ServerConf)

// Init 初始化配置
func init() {
	yamlFile, err := ioutil.ReadFile("config/server.yaml")
	if err != nil {
		log.Fatalf("yamlFile Get err #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, Conf)
	if err != nil {
		log.Fatalf("yaml conf Unmarshal: %v", err)
	}

}
