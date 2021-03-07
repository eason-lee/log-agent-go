package writer

import (
	"context"
	"log"

	"log-agent-go/config"
	"log-agent-go/utils"

	"github.com/segmentio/kafka-go"
)

// Writer ...
type Writer struct {
	client *kafka.Writer
}

// NewWriter ...
func NewWriter(conf *config.KafkaConf) *Writer {
	log.Println("batchsize", conf.BatchSize)
	client := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      conf.Brokers,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    conf.BatchSize,
		RequiredAcks: conf.RequiredAcks,
	})

	utils.AddShutdownListener(func() {
		client.Close()
	})

	return &Writer{client: client}
}

// Write ...
func (w *Writer) Write(val kafka.Message) {
	err := w.client.WriteMessages(context.Background(), val)
	log.Println("write", string(val.Value), "topic", val.Topic)
	if err != nil {
		log.Fatalf("execute kafka data err %v", err)
	}
}
