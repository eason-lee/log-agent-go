package taillog

import (
	"context"
	"fmt"
	"log"
	"log-agent-go/etcd"
	"log-agent-go/kafka"
	"log-agent-go/utils"
	"os"
	"strconv"
	"time"

	"github.com/hpcloud/tail"
)

// TaskManager 任务管理器
var TaskManager = TailTaskManager{
	TaskMap:        make(map[string]*TailTask, 64),
	UpdateConfChan: make(chan []*etcd.LogEntryConf),
}

// TailTask 日志收集任务
type TailTask struct {
	FileName string
	Topic    string
	TailObj  *tail.Tail
	Ctx      context.Context
	Cancel   context.CancelFunc
}

// TailTaskManager 任务管理
type TailTaskManager struct {
	TaskMap map[string]*TailTask
	// 配置修改的通道
	UpdateConfChan chan []*etcd.LogEntryConf
}

// 获取文件的 offset
func getFileOffset(filename, topic string) (offset int64) {
	key := utils.GetTailTaskKey(filename, topic)
	offsetResp, err := etcd.Get(key)
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

// NewTailTask 初始化 Tail
func NewTailTask(filename, topic string) (task TailTask, err error) {
	offset := getFileOffset(filename, topic)
	whence := os.SEEK_CUR
	if offset == int64(0) {
		whence = os.SEEK_END
	}
	log.Println("new task--", offset, whence, topic)
	TailObj, err := tail.TailFile(
		filename,
		tail.Config{
			ReOpen:    true,
			Follow:    true,
			Location:  &tail.SeekInfo{Offset: offset, Whence: whence}, // 从文件的哪个地方读
			MustExist: false,
			Poll:      true,
		})
	if err != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())

	task = TailTask{
		FileName: filename,
		Topic:    topic,
		TailObj:  TailObj,
		Ctx:      ctx,
		Cancel:   cancel,
	}
	go task.run()
	return
}

func (t *TailTask) run() {
	for {
		select {
		case line := <-t.TailObj.Lines:
			// 获取日志文件读取位置，放到 etcd
			offset, err := t.TailObj.Tell()
			if err != nil {
				log.Println("获取文件 offset 失败")
				offset = int64(0)
			}
			log.Printf("文件的 offset %d\n", offset)
			// 使用 chan 通道优化发送，把数据都放到 chan 里，发送的函数从 chan 中读取
			kafka.SendToChan(t.FileName, t.Topic, line.Text, offset)

		case <-t.Ctx.Done(): // context 收到结束命令
			log.Printf("TailTask 任务结束 %s_%s", t.FileName, t.Topic)
			return
		default:
			time.Sleep(time.Millisecond * 5)
		}
	}
}

func (m *TailTaskManager) listenUpdateConf() {
	for {
		select {
		case up := <-m.UpdateConfChan:
			for _, confg := range up {
				key := utils.GetTailTaskKey(confg.FilePath, confg.Topic)
				// 已有的配置不做改动
				_, ok := TaskManager.TaskMap[key]
				if ok {
					continue
				} else {
					// 创建新的 TailTask
					task, err := NewTailTask(confg.FilePath, confg.Topic)
					if err != nil {
						log.Printf("创建 TailTask 失败 : %v \n", err)
						continue
					}
					log.Printf("创建 TailTask 成功 %s:  \n", key)
					TaskManager.TaskMap[key] = &task
				}

			}
			// 删除修改和删除的 TailTask
			for _, existTask := range TaskManager.TaskMap {
				isDelete := true
				for _, newConf := range up {
					if newConf.FilePath == existTask.FileName && newConf.Topic == existTask.Topic {
						isDelete = false
						continue
					}
				}
				if isDelete {
					existTask.Cancel()
					delete(TaskManager.TaskMap, fmt.Sprintf("%s_%s", existTask.FileName, existTask.Topic))
				}
			}
			log.Printf("配置更新：%v", up)
		default:
		}
	}
}

// Run 开启日志收集器
func Run(configs []*etcd.LogEntryConf) {
	for _, v := range configs {
		log.Printf("log configs file path: %v topic %v\n", v.FilePath, v.Topic)
		task, err := NewTailTask(v.FilePath, v.Topic)
		if err != nil {
			log.Printf("创建 TailTask 失败: %v \n", err)
		}
		// 把 task 都保存到 map 里
		key := utils.GetTailTaskKey(v.FilePath, v.Topic)
		TaskManager.TaskMap[key] = &task

	}
	go TaskManager.listenUpdateConf()

}
