package weaviateserver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	opsobjects "github.com/weaviate/weaviate/adapters/handlers/rest/operations/objects"
	opsschema "github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/entities/models"
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
