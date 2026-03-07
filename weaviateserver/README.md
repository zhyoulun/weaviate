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