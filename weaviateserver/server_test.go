package weaviateserver

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	opsobjects "github.com/weaviate/weaviate/adapters/handlers/rest/operations/objects"
	opsschema "github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/entities/models"
	clustercfg "github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestWeaviateServerInProcessReadWrite(t *testing.T) {
	ws, err := NewWeaviateServerWithConfig(newWeaviateConfigForTest(t))
	if err != nil {
		t.Fatalf("new weaviate server: %v", err)
	}

	className := fmt.Sprintf("ServerReadWriteClass%d", time.Now().UnixNano())
	objectID := uuid.NewString()
	wantContent := "hello from weaviateserver test"

	createClassResp, err := ws.SchemaObjectsCreate(opsschema.SchemaObjectsCreateParams{
		ObjectClass: &models.Class{
			Class:      className,
			Vectorizer: "none",
			Properties: []*models.Property{
				{
					Name:     "content",
					DataType: []string{"text"},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("create class: %v", err)
	}
	if createClassResp == nil || createClassResp.Payload == nil {
		t.Fatalf("create class response payload is nil")
	}
	moduleConfig, ok := createClassResp.Payload.ModuleConfig.(map[string]any)
	if !ok {
		t.Fatalf("class moduleConfig is not a map: %#v", createClassResp.Payload.ModuleConfig)
	}
	text2vecOpenAIConfig, ok := moduleConfig["text2vec-openai"].(map[string]any)
	if !ok {
		t.Fatalf("text2vec-openai moduleConfig missing or wrong type: %#v", moduleConfig["text2vec-openai"])
	}
	if baseURL, _ := text2vecOpenAIConfig["baseURL"].(string); baseURL != "https://openrouter.ai/api" {
		t.Fatalf("unexpected text2vec-openai baseURL, got=%q", baseURL)
	}

	createObjectResp, err := ws.ObjectsCreate(opsobjects.ObjectsCreateParams{
		Body: &models.Object{
			Class: className,
			ID:    strfmt.UUID(objectID),
			Properties: map[string]any{
				"content": wantContent,
			},
			Vector: models.C11yVector{0.11, 0.22, 0.33},
		},
	})
	if err != nil {
		t.Fatalf("create object: %v", err)
	}
	if createObjectResp == nil || createObjectResp.Payload == nil {
		t.Fatalf("create object response payload is nil")
	}

	getObjectResp, err := ws.ObjectsClassGet(opsobjects.ObjectsClassGetParams{
		ClassName: className,
		ID:        strfmt.UUID(objectID),
	})
	if err != nil {
		t.Fatalf("get object: %v", err)
	}
	if getObjectResp == nil || getObjectResp.Payload == nil {
		t.Fatalf("get object response payload is nil")
	}

	got := getObjectResp.Payload
	if got.Class != className {
		t.Fatalf("unexpected class, got=%q want=%q", got.Class, className)
	}
	if got.ID.String() != objectID {
		t.Fatalf("unexpected object id, got=%q want=%q", got.ID.String(), objectID)
	}
	properties, ok := got.Properties.(map[string]any)
	if !ok {
		t.Fatalf("object properties are not a map: %#v", got.Properties)
	}
	content, ok := properties["content"].(string)
	if !ok {
		t.Fatalf("object content property missing or not a string: %#v", properties["content"])
	}
	if content != wantContent {
		t.Fatalf("unexpected object content, got=%q want=%q", content, wantContent)
	}
}

func newWeaviateConfigForTest(t *testing.T) Config {
	t.Helper()

	return Config{
		WeaviateConfig: config.WeaviateConfig{
			Hostname: "127.0.0.1:0",
			Scheme:   "http",
			Config: config.Config{
				Persistence: config.Persistence{
					DataPath: t.TempDir(),
				},
				DefaultVectorizerModule: "text2vec-openai",
				EnableModules:           "text2vec-openai",
				EnableApiBasedModules:   false,
				DisableTelemetry:        true,
			},
		},
		ModuleConfig: map[string]any{
			"text2vec-openai": map[string]any{
				"baseURL": "https://openrouter.ai/api",
				"model":   "openai/text-embedding-3-small",
			},
		},
		Log: LogConfig{
			Level:  "warn",
			Format: "text",
		},
		OpenaiAPIKey: "test-openai-api-key",
	}
}

func TestApplyConfiguredModuleConfig(t *testing.T) {
	defaultModuleConfig := map[string]any{
		"text2vec-openai": map[string]any{
			"baseURL": "https://openrouter.ai/api",
			"model":   "openai/text-embedding-3-small",
		},
		"generative-openai": map[string]any{
			"model": "openai/gpt-4o-mini",
		},
	}

	t.Run("inject defaults when class module config is nil", func(t *testing.T) {
		ws := &WeaviateServer{
			cfg: Config{
				ModuleConfig: defaultModuleConfig,
			},
		}
		class := &models.Class{Class: "InjectDefaultsClass"}

		ws.applyConfiguredModuleConfig(class)

		got, ok := class.ModuleConfig.(map[string]any)
		if !ok {
			t.Fatalf("class moduleConfig type mismatch, got=%T", class.ModuleConfig)
		}
		if _, ok := got["text2vec-openai"]; !ok {
			t.Fatalf("expected text2vec-openai config to be injected")
		}
		if _, ok := got["generative-openai"]; !ok {
			t.Fatalf("expected generative-openai config to be injected")
		}

		got["new-key"] = "mutated"
		if _, ok := defaultModuleConfig["new-key"]; ok {
			t.Fatalf("module config should be copied, but default config was mutated")
		}
	})

	t.Run("merge defaults and keep class overrides", func(t *testing.T) {
		ws := &WeaviateServer{
			cfg: Config{
				ModuleConfig: defaultModuleConfig,
			},
		}
		class := &models.Class{
			Class: "MergeDefaultsClass",
			ModuleConfig: map[string]any{
				"text2vec-openai": map[string]any{
					"model": "openai/text-embedding-3-large",
				},
				"reranker-voyageai": map[string]any{
					"model": "rerank-2",
				},
			},
		}

		ws.applyConfiguredModuleConfig(class)

		got, ok := class.ModuleConfig.(map[string]any)
		if !ok {
			t.Fatalf("class moduleConfig type mismatch, got=%T", class.ModuleConfig)
		}

		openAIConfig, ok := got["text2vec-openai"].(map[string]any)
		if !ok {
			t.Fatalf("expected text2vec-openai config to be a map, got=%T", got["text2vec-openai"])
		}
		if model, _ := openAIConfig["model"].(string); model != "openai/text-embedding-3-large" {
			t.Fatalf("class override lost, got model=%q", model)
		}
		if _, ok := got["generative-openai"]; !ok {
			t.Fatalf("expected default module config to be merged")
		}
		if _, ok := got["reranker-voyageai"]; !ok {
			t.Fatalf("expected class-specific module config to be preserved")
		}
	})
}

func TestNewInProcessRequestInjectsOpenAIAPIKeyFromConfig(t *testing.T) {
	ws := &WeaviateServer{
		cfg: Config{
			OpenaiAPIKey: "openai-key-from-config",
		},
	}

	req := ws.newInProcessRequest(nil, "GET", "/v1/meta")
	key, ok := req.Context().Value("X-Openai-Api-Key").([]string)
	if !ok || len(key) == 0 {
		t.Fatalf("expected X-Openai-Api-Key to be injected, got=%#v", req.Context().Value("X-Openai-Api-Key"))
	}
	if key[0] != "openai-key-from-config" {
		t.Fatalf("unexpected X-Openai-Api-Key, got=%q", key[0])
	}
}

func TestNewInProcessRequestKeepsExplicitOpenAIAPIKey(t *testing.T) {
	ctx := context.WithValue(context.Background(), "X-Openai-Api-Key", []string{"explicit-key"})
	ws := &WeaviateServer{
		cfg: Config{
			OpenaiAPIKey: "openai-key-from-config",
		},
	}

	req := ws.newInProcessRequest(ctx, "GET", "/v1/meta")
	key, ok := req.Context().Value("X-Openai-Api-Key").([]string)
	if !ok || len(key) == 0 {
		t.Fatalf("expected X-Openai-Api-Key in request context, got=%#v", req.Context().Value("X-Openai-Api-Key"))
	}
	if key[0] != "explicit-key" {
		t.Fatalf("explicit X-Openai-Api-Key should win, got=%q", key[0])
	}
}

func TestApplyEmbeddedEnvOverridesDefaults(t *testing.T) {
	portKeys := []string{
		"CLUSTER_GOSSIP_BIND_PORT",
		"CLUSTER_DATA_BIND_PORT",
		"RAFT_PORT",
		"RAFT_INTERNAL_RPC_PORT",
		"GRPC_PORT",
	}
	allKeys := append([]string{
		"CLUSTER_IN_LOCALHOST",
		"WEAVIATE_EMBEDDED_NO_NETWORK",
		"GO_PROFILING_DISABLE",
		"GO_PROFILING_PORT",
	}, portKeys...)

	saved := snapshotAndUnsetEnv(t, allKeys...)
	defer restoreEnvironment(saved)

	restore, err := applyEmbeddedEnvOverrides(config.WeaviateConfig{})
	if err != nil {
		t.Fatalf("applyEmbeddedEnvOverrides: %v", err)
	}
	defer restore()

	seenPorts := map[int]struct{}{}
	for _, key := range portKeys {
		raw := os.Getenv(key)
		port, convErr := strconv.Atoi(raw)
		if convErr != nil || port <= 0 {
			t.Fatalf("%s must be a positive integer, got=%q err=%v", key, raw, convErr)
		}
		if _, exists := seenPorts[port]; exists {
			t.Fatalf("duplicate port allocated: %d", port)
		}
		seenPorts[port] = struct{}{}
	}

	if got := os.Getenv("CLUSTER_IN_LOCALHOST"); got != "true" {
		t.Fatalf("CLUSTER_IN_LOCALHOST should default to true, got=%q", got)
	}
	if got := os.Getenv("WEAVIATE_EMBEDDED_NO_NETWORK"); got != "true" {
		t.Fatalf("WEAVIATE_EMBEDDED_NO_NETWORK should default to true, got=%q", got)
	}

	if got := os.Getenv("GO_PROFILING_DISABLE"); got != "true" {
		t.Fatalf("GO_PROFILING_DISABLE should default to true, got=%q", got)
	}
}

func TestApplyEmbeddedEnvOverridesHonorsExplicitConfig(t *testing.T) {
	saved := snapshotAndUnsetEnv(t,
		"CLUSTER_GOSSIP_BIND_PORT",
		"CLUSTER_DATA_BIND_PORT",
		"RAFT_PORT",
		"RAFT_INTERNAL_RPC_PORT",
		"GRPC_PORT",
		"WEAVIATE_EMBEDDED_NO_NETWORK",
		"GO_PROFILING_DISABLE",
		"GO_PROFILING_PORT",
	)
	defer restoreEnvironment(saved)

	cfg := config.WeaviateConfig{
		Config: config.Config{
			Cluster: clustercfg.Config{
				GossipBindPort: 19446,
				DataBindPort:   19447,
				Localhost:      true,
			},
			Raft: config.Raft{
				Port:            18300,
				InternalRPCPort: 18301,
			},
			GRPC: config.GRPC{
				Port: 15051,
			},
			Profiling: config.Profiling{
				Disabled: true,
			},
		},
	}

	restore, err := applyEmbeddedEnvOverrides(cfg)
	if err != nil {
		t.Fatalf("applyEmbeddedEnvOverrides: %v", err)
	}
	defer restore()

	expectEnv := map[string]string{
		"CLUSTER_GOSSIP_BIND_PORT":     "19446",
		"CLUSTER_DATA_BIND_PORT":       "19447",
		"RAFT_PORT":                    "18300",
		"RAFT_INTERNAL_RPC_PORT":       "18301",
		"GRPC_PORT":                    "15051",
		"WEAVIATE_EMBEDDED_NO_NETWORK": "true",
		"GO_PROFILING_DISABLE":         "true",
		"CLUSTER_IN_LOCALHOST":         "true",
	}

	for key, expected := range expectEnv {
		if got := os.Getenv(key); got != expected {
			t.Fatalf("%s mismatch, got=%q want=%q", key, got, expected)
		}
	}
}

func snapshotAndUnsetEnv(t *testing.T, keys ...string) map[string]envVarState {
	t.Helper()

	saved := make(map[string]envVarState, len(keys))
	for _, key := range keys {
		value, exists := os.LookupEnv(key)
		saved[key] = envVarState{value: value, exists: exists}
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unset %s: %v", key, err)
		}
	}
	return saved
}
