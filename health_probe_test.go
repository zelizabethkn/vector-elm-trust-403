package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestChannelHealthProbeClearsErrorCountOnStreamingSuccess(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/responses" {
			t.Fatalf("path = %q, want /v1/responses", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer sk-upstream" {
			t.Fatalf("authorization = %q", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: response.created\ndata: {\"type\":\"response.created\"}\n\n")
		_, _ = io.WriteString(w, "event: response.completed\ndata: {\"type\":\"response.completed\"}\n\n")
	}))
	defer upstream.Close()

	cfg := &ChannelConfig{Channels: []Channel{
		{Name: "probe", BaseURL: upstream.URL + "/v1", APIKey: "sk-upstream", Weight: 1, ErrorCount: 2},
	}}
	store := newChannelStateStore(filepath.Join(t.TempDir(), "channel.json"), cfg)
	probeCfg := defaultAppConfig().Probe
	probeCfg.Model = "gpt-test"
	probeCfg.Prompt = "probe prompt"
	probe := newChannelHealthProbe(store, slog.New(slog.NewTextHandler(io.Discard, nil)), probeCfg)

	probe.runOnce(context.Background())

	if cfg.Channels[0].ErrorCount != 0 {
		t.Fatalf("errorCount = %d, want 0", cfg.Channels[0].ErrorCount)
	}
}
