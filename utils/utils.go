package utils

import (
	"fmt"
)

// GetTailTaskKey 获取 TailTask 的 key
func GetTailTaskKey(Filepath, topic string) string {
	return fmt.Sprintf("%s_%s", topic, Filepath)
}
