package utils

import "fmt"

// GetTailTaskKey 获取 TailTask 的 key
func GetTailTaskKey(filePath, topic string) string {
	return fmt.Sprintf("%s_%s", filePath, topic)
}
