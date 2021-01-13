package utils

import (
	"fmt"
	"log-agent-go/config"
)

// GetTailTaskKey 获取 TailTask 的 key
func GetTailTaskKey(filePath, topic string) string {
	return fmt.Sprintf("%s_%s", topic, filePath)
}

// GetTaillogConfKey 获取 taillog 的 etcd key
func GetTaillogConfKey() string {
	return fmt.Sprintf("%s-%s", config.Conf.Name, config.Conf.TaillogConf.Key)
}
