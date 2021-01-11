package config

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
