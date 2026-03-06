package weaviateserver

import (
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
	ws, err := NewWeaviateServer(newWeaviateConfigForTest(t))
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

func newWeaviateConfigForTest(t *testing.T) config.WeaviateConfig {
	t.Helper()

	return config.WeaviateConfig{
		Hostname: "127.0.0.1:0",
		Scheme:   "http",
		Config: config.Config{
			Persistence: config.Persistence{
				DataPath: t.TempDir(),
			},
			DefaultVectorizerModule: "none",
			EnableApiBasedModules:   false,
			DisableTelemetry:        true,
		},
	}
}
