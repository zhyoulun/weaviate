//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package weaviateserver

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"

	"github.com/go-openapi/loads"
	flags "github.com/jessevdk/go-flags"

	"github.com/weaviate/weaviate/adapters/handlers/rest"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/usecases/config"
)

// WeaviateServer 在进程内运行 Weaviate，启动流程与 cmd/weaviate-server/main.go 保持一致。
type WeaviateServer struct {
	server *rest.Server
	api    *operations.WeaviateAPI
}

// NewWeaviateServer 使用传入的 WeaviateConfig 初始化一个可嵌入的 Weaviate 服务实例。
func NewWeaviateServer(serverConfig config.WeaviateConfig) (*WeaviateServer, error) {
	// 加载编译进二进制的 swagger 规范。
	swaggerSpec, err := loads.Embedded(rest.SwaggerJSON, rest.FlatSwaggerJSON)
	if err != nil {
		return nil, fmt.Errorf("load embedded swagger spec: %w", err)
	}

	// 根据 swagger 规范构建生成的 operations API。
	api := operations.NewWeaviateAPI(swaggerSpec)

	// 基于生成的 API 创建 REST Server。
	server := rest.NewServer(api)

	// 配置 parser，并注册与 cmd/weaviate-server/main.go 一致的选项组。
	parser := flags.NewParser(server, flags.Default)
	parser.ShortDescription = "Weaviate REST API"
	parser.LongDescription = "# Introduction<br/> Weaviate is an open source, AI-native vector database that helps developers create intuitive and reliable AI-powered applications. <br/> ### Base Path <br/>The base path for the Weaviate server is structured as `[YOUR-WEAVIATE-HOST]:[PORT]/v1`. As an example, if you wish to access the `schema` endpoint on a local instance, you would navigate to `http://localhost:8080/v1/schema`. Ensure you replace `[YOUR-WEAVIATE-HOST]` and `[PORT]` with your actual server host and port number respectively. <br/> ### Questions? <br/>If you have any comments or questions, please feel free to reach out to us at the community forum [https://forum.weaviate.io/](https://forum.weaviate.io/). <br/>### Issues? <br/>If you find a bug or want to file a feature request, please open an issue on our GitHub repository for [Weaviate](https://github.com/weaviate/weaviate). <br/>### Need more documentation? <br/>For a quickstart, code examples, concepts and more, please visit our [documentation page](https://docs.weaviate.io/weaviate)."

	server.ConfigureFlags()
	for _, optsGroup := range api.CommandLineOptionsGroups {
		if opts, ok := optsGroup.Options.(*config.Flags); ok {
			// 使用内存配置，避免依赖 config 文件。
			opts.EmbeddedConfig = &serverConfig
		}
		if _, err := parser.AddGroup(optsGroup.ShortDescription, optsGroup.LongDescription, optsGroup.Options); err != nil {
			return nil, fmt.Errorf("add options group %q: %w", optsGroup.ShortDescription, err)
		}
	}

	// 解析空参数以应用默认 CLI 值，并保留与主程序一致的解析流程。
	if _, err := parser.ParseArgs([]string{}); err != nil {
		return nil, fmt.Errorf("parse weaviate server args: %w", err)
	}

	// 在进程内配置监听参数，嵌入场景默认使用 HTTP。
	configureServerListener(server, serverConfig)

	// 完成 handler、app state、模块及运行时依赖的装配。
	server.ConfigureAPI()

	// 返回进程内服务包装对象。
	return &WeaviateServer{server: server, api: api}, nil
}

func configureServerListener(server *rest.Server, serverConfig config.WeaviateConfig) {
	scheme := strings.TrimSpace(serverConfig.Scheme)
	if scheme == "" {
		scheme = "http"
	}
	server.EnabledListeners = []string{scheme}

	host := "127.0.0.1"
	port := 0
	if serverConfig.Hostname != "" {
		if parsedHost, parsedPort, err := net.SplitHostPort(serverConfig.Hostname); err == nil {
			host = parsedHost
			if parsedPortAsInt, convErr := strconv.Atoi(parsedPort); convErr == nil {
				port = parsedPortAsInt
			}
		} else {
			host = serverConfig.Hostname
		}
	}

	switch scheme {
	case "https":
		server.TLSHost = host
		server.TLSPort = port
	default:
		server.Host = host
		server.Port = port
	}
}

// // Start blocks while serving Weaviate.
// func (ws *WeaviateServer) Start() error {
// 	if ws == nil || ws.server == nil {
// 		return errors.New("weaviate server is not initialized")
// 	}
// 	return ws.server.Serve()
// }

// // Shutdown gracefully stops a running server.
// func (ws *WeaviateServer) Shutdown() error {
// 	if ws == nil || ws.server == nil {
// 		return nil
// 	}
// 	return ws.server.Shutdown()
// }

// // RESTServer returns the underlying generated REST server.
// func (ws *WeaviateServer) RESTServer() *rest.Server {
// 	if ws == nil {
// 		return nil
// 	}
// 	return ws.server
// }

func newInProcessRequest(ctx context.Context, method, path string) *http.Request {
	req := httptest.NewRequest(method, path, nil)
	if ctx != nil {
		req = req.WithContext(ctx)
	}
	return req
}
