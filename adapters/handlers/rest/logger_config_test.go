package rest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
)

func TestLoggerUsesStartupLoggerConfigWithoutEnv(t *testing.T) {
	t.Setenv("LOG_LEVEL", "trace")
	t.Setenv("LOG_FORMAT", "text")

	restore := SetStartupLoggerConfig(StartupLoggerConfig{
		Level:      "error",
		Format:     "json",
		DisableEnv: true,
	})
	defer restore()

	got := logger()
	if got.Level != logrus.ErrorLevel {
		t.Fatalf("unexpected log level: got=%s want=%s", got.Level, logrus.ErrorLevel)
	}
	if _, ok := got.Formatter.(*WeaviateJSONFormatter); !ok {
		t.Fatalf("unexpected formatter type: got=%T want=*WeaviateJSONFormatter", got.Formatter)
	}
}

func TestLoggerWritesToConfiguredPath(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "weaviate.log")

	restore := SetStartupLoggerConfig(StartupLoggerConfig{
		Level:      "info",
		Format:     "text",
		Path:       logPath,
		DisableEnv: true,
	})
	defer restore()

	got := logger()
	got.WithField("action", "test").Info("log path check")

	content, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log file %q: %v", logPath, err)
	}
	if !strings.Contains(string(content), "log path check") {
		t.Fatalf("expected log file %q to contain test entry, got=%q", logPath, string(content))
	}
}
