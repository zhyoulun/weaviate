```
cfg := Config{
    WeaviateConfig: ...,
    Log: LogConfig{
        Level:  "debug",              // panic/fatal/error/warn/info/debug/trace
        Format: "text",               // "text" 或其他值(走 json)
        Path:   "/tmp/weaviate.log",  // 空字符串则默认标准输出流
    },
}
```

`weaviateserver` 默认启用同进程 `no-network` 模式（通过 `WEAVIATE_EMBEDDED_NO_NETWORK=true`），不会再启动以下监听：

- memberlist gossip (`7946` TCP/UDP)
- internal cluster API (`7947`)
- raft transport (`8300`)
- raft internal rpc (`8301`)
- external gRPC (`50051`)
- pprof (`6060`)

如需回退为网络模式，可在进程环境中显式设置：

```bash
WEAVIATE_EMBEDDED_NO_NETWORK=false
```
