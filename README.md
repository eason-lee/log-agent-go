# log-agent-go
日志收集器

- 根据配置收集本地日志，发送到 kafka
- 监听 etcd 的 key 实现实时更新配置
- 记录已发送的日志 offset 到 etcd ，程序意外退出时保证消息不丢失

## etcd 配置
```bash
etcdctl --endpoints=http://127.0.0.1:2379 put panda-server-conf '[
    {
        "filepath":"/Users/lisen/go/src/log-agent-go/test.log",
        "topic":"test2"
    },
    {
        "filepath":"/Users/lisen/go/src/log-agent-go/go_run.log",
        "topic":"go_run"
    }
]'
```
