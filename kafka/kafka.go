package kafka

import (
	"log"
	"log-agent-go/etcd"
	"log-agent-go/utils"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
)

var client sarama.SyncProducer // kafka 的生产者

// LogData 日志数据
type LogData struct {
	FilePath   string
	Topic      string
	Data       string
	FileOffset int64
}

// LogChan 日志 channel
var LogChan chan *LogData

// Init 初始化 kafka 连接
func Init(address []string, chanMaxSize int) (err error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll          // 发送完数据需要leader和follow都确认
	config.Producer.Partitioner = sarama.NewRandomPartitioner // 新选出一个partition
	config.Producer.Return.Successes = true                   // 成功交付的消息将在success channel返回

	// 连接kafka
	client, err = sarama.NewSyncProducer(address, config)
	if err != nil {
		return
	}
	//初始化 LogChan
	LogChan = make(chan *LogData, chanMaxSize)
	//
	go sendToKafka()
	return
}

// SendToChan 发送数据到通道中
func SendToChan(filePath, topic, data string, offset int64) {
	msg := LogData{
		FilePath:   filePath,
		Topic:      topic,
		Data:       data,
		FileOffset: offset,
	}
	LogChan <- &msg
}

// SendToKafka 从通道中读取数据发送到 kafka
func sendToKafka() {
	for {
		select {
		case logData := <-LogChan:
			// 构造一个消息
			msg := &sarama.ProducerMessage{}
			msg.Topic = logData.Topic
			msg.Value = sarama.StringEncoder(logData.Data)

			// defer client.Close()
			log.Printf(" kafka 发送消息 %v\n", logData.Data)
			// 发送消息
			pid, offset, err := client.SendMessage(msg)
			if err != nil {
				log.Printf("kafka 发送消息失败 : %v", err)
				continue
			}

			// 发送已读的日志 offset 到 etcd
			key := utils.GetTailTaskKey(logData.FilePath, logData.Topic)
			etcd.Put(key, strconv.FormatInt(logData.FileOffset, 10))

			log.Printf(" kafka 发送消息成功\n")
			log.Printf("partition:%v offset:%v\n", pid, offset)
		default:
			time.Sleep(time.Millisecond * 5)
		}

	}

}
