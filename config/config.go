package config

import (
	"log"

	"github.com/spf13/viper"
)

type (
	// ServerConf 服务配置
	ServerConf struct {
		Name      string `mapstructure:"server_name"`
		KafkaConf `mapstructure:"kafka"`
		EtcdConf  `mapstructure:"etcd"`
	}

	// KafkaConf ...
	KafkaConf struct {
		Brokers      []string `mapstructure:"brokers"`
		BatchSize    int      `mapstructure:"batch_size,default=100"`
		RequiredAcks int      `mapstructure:"required_acks"`
	}

	// Val ...
	Val struct {
		Filepath string `mapstructure:"filepath"`
		Topic    string `mapstructure:"topic"`
	}

	// EtcdConf ...
	EtcdConf struct {
		Address          []string `mapstructure:"address"`
		Key              string   `mapstructure:"key"`
		Vals             []Val    `mapstructure:"vals"`
		EnableWatch      bool     `mapstructure:"enable_watch"`
		MarkOffsetPeriod int      `mapstructure:"mark_offset_period"`
	}
)

// GetConf ...
func GetConf(configName string) *ServerConf {
	conf := viper.New()
	conf.SetConfigFile(configName)

	// 设置默认值
	conf.SetDefault("kafka.batch_size", 100)
	conf.SetDefault("kafka.required_acks", -1)

	err := conf.ReadInConfig() // 查找并读取配置文件
	if err != nil {            // 处理读取配置文件的错误
		log.Fatalf("yamlFile Get err #%v ", err)
	}

	var c ServerConf
	err = conf.Unmarshal(&c)
	if err != nil { // 处理读取配置文件的错误
		log.Fatalf("Unmarshal yamlFile Get err #%v ", err)
	}

	return &c
}
