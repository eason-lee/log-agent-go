package taillog

import (
	"context"
	"log"
	"log-agent-go/config"
	"log-agent-go/etcd"
	"log-agent-go/service"
	"log-agent-go/utils"
	"log-agent-go/writer"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/hpcloud/tail"
	"github.com/segmentio/kafka-go"
)

type (
	writeMsg struct {
		offset int64
		msg    kafka.Message
	}

	// TailTask 日志收集任务
	TailTask struct {
		Filepath         string
		Topic            string
		TailObj          *tail.Tail
		conf             *config.ServerConf
		etcdClient       *etcd.EtcdClient
		offset           sync.Map
		consumer         *writer.Writer
		channel          chan *writeMsg
		producerRoutines *service.RoutineGroup
		consumerRoutines *service.RoutineGroup
		ctx              context.Context
		cancel           context.CancelFunc
	}
)

// NewTailTask ...
func NewTailTask(filepath, topic string, writer *writer.Writer, conf *config.ServerConf, etcdClient *etcd.EtcdClient) service.Service {
	offset := getFileOffset(filepath, topic, etcdClient)
	whence := os.SEEK_CUR
	if offset == int64(0) {
		whence = os.SEEK_END
	}

	TailObj, err := tail.TailFile(
		filepath,
		tail.Config{
			ReOpen:    true,
			Follow:    true,
			Location:  &tail.SeekInfo{Offset: offset, Whence: whence}, // 从文件的哪个地方读
			MustExist: false,
			Poll:      true,
		})
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())

	return &TailTask{
		conf:             conf,
		Filepath:         filepath,
		Topic:            topic,
		TailObj:          TailObj,
		etcdClient:       etcdClient,
		consumer:         writer,
		channel:          make(chan *writeMsg),
		producerRoutines: service.NewRoutineGroup(),
		consumerRoutines: service.NewRoutineGroup(),
		ctx:              ctx,
		cancel:           cancel,
	}

}

// Stop ...
func (t *TailTask) Stop() {
	t.cancel()
}

// Start ...
func (t *TailTask) Start() {
	t.startProducers()
	t.startConsumers()

	go service.RunSafe(t.periodMarkOffset)

	t.producerRoutines.Wait()
	close(t.channel)
	t.consumerRoutines.Wait()
}

func (t *TailTask) setOffset(offset int64) {
	t.offset.Store("offset", offset)
}

func (t *TailTask) startConsumers() {
	t.consumerRoutines.Run(func() {
		for msg := range t.channel {
			t.consumer.Write(msg.msg)
			t.setOffset(msg.offset)
		}
	})
}

func (t *TailTask) startProducers() {
	t.producerRoutines.Run(func() {
		for {
			select {
			case line := <-t.TailObj.Lines:
				offset, err := t.TailObj.Tell()
				if err != nil {
					log.Println("获取文件 offset 失败")
					offset = int64(0)
				}
				// log.Printf("文件的 offset %d\n", offset)
				msg := kafka.Message{
					Topic: t.Topic,
					Value: []byte(line.Text),
				}

				t.channel <- &writeMsg{
					offset: offset,
					msg:    msg,
				}

			case <-t.ctx.Done(): // context 收到结束命令
				t.TailObj.Stop()
				// log.Printf("TailTask 任务结束 %s_%s", t.Filepath, t.Topic)
				return
			}
		}
	})
}

func (t *TailTask) periodMarkOffset() {
	// 间隔时间 MarkOffset
	s := t.conf.MarkOffsetPeriod
	ticker := time.Tick(time.Duration(s) * time.Second)
	for range ticker {
		if offset, ok := t.offset.Load("offset"); ok && offset != int64(-1) {
			key := utils.GetTailTaskKey(t.Filepath, t.Topic)
			t.etcdClient.Put(key, strconv.FormatInt(offset.(int64), 10))
			// 如果当前 offset 等于最新的 offset 则说明这段时间内没有新的文件读取，设置为 -1，表示不用更新
			if last, _ := t.offset.Load("offset"); last == offset {
				t.setOffset(int64(-1))
			}
		}
	}
}

// 获取文件的 offset
func getFileOffset(filepath, topic string, etcdClient *etcd.EtcdClient) (offset int64) {
	key := utils.GetTailTaskKey(filepath, topic)
	offsetResp, err := etcdClient.Get(key)
	offset = int64(0)

	if err == nil {
		for _, ev := range offsetResp.Kvs {
			if ev.Value != nil {
				offset, err = strconv.ParseInt(string(ev.Value), 10, 64)
				break
			}

		}
	}

	return
}
