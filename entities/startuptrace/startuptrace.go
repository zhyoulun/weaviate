package startuptrace

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type scopeState struct {
	started bool
	start   time.Time
	last    time.Time
}

var state struct {
	enabledOnce sync.Once
	enabled     bool
	writer      io.Writer

	mu     sync.Mutex
	scopes map[string]scopeState
}

func Enabled() bool {
	initState()
	return state.enabled
}

func Reset(scope string, phase string) {
	if !Enabled() {
		return
	}

	now := time.Now()
	scope = normalize(scope)

	state.mu.Lock()
	if state.scopes == nil {
		state.scopes = map[string]scopeState{}
	}
	state.scopes[scope] = scopeState{
		started: true,
		start:   now,
		last:    now,
	}
	writeLocked(now, scope, phase, 0, 0)
	state.mu.Unlock()
}

func Mark(scope string, phase string) {
	if !Enabled() {
		return
	}

	now := time.Now()
	scope = normalize(scope)

	state.mu.Lock()
	if state.scopes == nil {
		state.scopes = map[string]scopeState{}
	}

	scopeEntry := state.scopes[scope]
	if !scopeEntry.started {
		scopeEntry = scopeState{
			started: true,
			start:   now,
			last:    now,
		}
		state.scopes[scope] = scopeEntry
		writeLocked(now, scope, phase, 0, 0)
		state.mu.Unlock()
		return
	}

	step := now.Sub(scopeEntry.last)
	total := now.Sub(scopeEntry.start)
	scopeEntry.last = now
	state.scopes[scope] = scopeEntry
	writeLocked(now, scope, phase, step, total)
	state.mu.Unlock()
}

func initState() {
	state.enabledOnce.Do(func() {
		switch strings.ToLower(strings.TrimSpace(os.Getenv("WEAVIATE_STARTUP_TRACE"))) {
		case "1", "true", "yes", "on":
			state.enabled = true
		}

		if !state.enabled {
			return
		}

		path := strings.TrimSpace(os.Getenv("WEAVIATE_STARTUP_TRACE_FILE"))
		if path == "" {
			state.writer = os.Stderr
			return
		}

		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			state.writer = os.Stderr
			return
		}

		f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			state.writer = os.Stderr
			return
		}

		state.writer = f
	})
}

func writeLocked(now time.Time, scope string, phase string, step time.Duration, total time.Duration) {
	writer := state.writer
	if writer == nil {
		writer = os.Stderr
	}

	_, _ = fmt.Fprintf(
		writer,
		"[wv-startup] ts=%s scope=%s phase=%s step=%s total=%s\n",
		now.Format(time.RFC3339Nano),
		scope,
		normalize(phase),
		step,
		total,
	)
}

func normalize(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return "-"
	}
	return strings.ReplaceAll(text, " ", "_")
}
