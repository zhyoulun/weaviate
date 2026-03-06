package weaviateserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestWeaviateServerStartAndShutdown(t *testing.T) {
	ws, err := NewWeaviateServer(newWeaviateConfigForTest(t))
	if err != nil {
		t.Fatalf("new weaviate server: %v", err)
	}

	if err := ws.RESTServer().Listen(); err != nil {
		t.Fatalf("listen on HTTP port: %v", err)
	}

	baseURL := fmt.Sprintf("http://%s:%d", ws.RESTServer().Host, ws.RESTServer().Port)
	httpClient := &http.Client{Timeout: 2 * time.Second}

	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- ws.Start()
	}()

	readyURL := baseURL + "/v1/.well-known/ready"
	deadline := time.Now().Add(40 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("server did not become ready before timeout: %s", readyURL)
		}

		resp, reqErr := httpClient.Get(readyURL)
		if reqErr == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}

		time.Sleep(250 * time.Millisecond)
	}

	className := fmt.Sprintf("ServerReadWriteClass%d", time.Now().UnixNano())
	objectID := uuid.NewString()
	wantContent := "hello from weaviateserver test"

	doJSONRequest(t, httpClient, http.MethodPost, baseURL+"/v1/schema", map[string]any{
		"class":      className,
		"vectorizer": "none",
		"properties": []map[string]any{
			{
				"name":     "content",
				"dataType": []string{"text"},
			},
		},
	}, http.StatusOK)

	doJSONRequest(t, httpClient, http.MethodPost, baseURL+"/v1/objects", map[string]any{
		"class": className,
		"id":    objectID,
		"properties": map[string]any{
			"content": wantContent,
		},
		"vector": []float64{0.11, 0.22, 0.33},
	}, http.StatusOK)

	body := doJSONRequest(t, httpClient, http.MethodGet, fmt.Sprintf("%s/v1/objects/%s?class=%s", baseURL, objectID, url.QueryEscape(className)), nil, http.StatusOK)

	var got struct {
		Class      string         `json:"class"`
		ID         string         `json:"id"`
		Properties map[string]any `json:"properties"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode get object response: %v", err)
	}

	if got.Class != className {
		t.Fatalf("unexpected class, got=%q want=%q", got.Class, className)
	}
	if got.ID != objectID {
		t.Fatalf("unexpected object id, got=%q want=%q", got.ID, objectID)
	}
	content, ok := got.Properties["content"].(string)
	if !ok {
		t.Fatalf("object content property missing or not a string: %#v", got.Properties["content"])
	}
	if content != wantContent {
		t.Fatalf("unexpected object content, got=%q want=%q", content, wantContent)
	}

	if err := ws.Shutdown(); err != nil {
		t.Fatalf("shutdown weaviate server: %v", err)
	}

	select {
	case serveErr := <-serveErrCh:
		if serveErr != nil {
			t.Fatalf("server returned error: %v", serveErr)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("timeout waiting for server to exit after shutdown")
	}
}

func getFreeTCPPort(t *testing.T) int {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	defer listener.Close()

	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener address is not TCP: %T", listener.Addr())
	}

	return addr.Port
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

func doJSONRequest(t *testing.T, httpClient *http.Client, method, targetURL string, payload any, wantStatus int) []byte {
	t.Helper()

	var bodyReader io.Reader
	if payload != nil {
		body, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal request body for %s %s: %v", method, targetURL, err)
		}
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequest(method, targetURL, bodyReader)
	if err != nil {
		t.Fatalf("new request %s %s: %v", method, targetURL, err)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("request %s %s failed: %v", method, targetURL, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body for %s %s: %v", method, targetURL, err)
	}

	if resp.StatusCode != wantStatus {
		t.Fatalf("unexpected status for %s %s, got=%d want=%d, body=%s", method, targetURL, resp.StatusCode, wantStatus, string(respBody))
	}

	return respBody
}
