package main

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDailyLogWriterWritesTodayFile(t *testing.T) {
	dir := t.TempDir()
	writer, err := newDailyLogWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	if _, err := writer.Write([]byte("client=alice status=200\n")); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, time.Now().Format("2006-01-02")+".log")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "client=alice status=200") {
		t.Fatalf("log file body = %q", string(raw))
	}
}

func TestAuditLogHandlerWritesOnlyRequestComplete(t *testing.T) {
	var out strings.Builder
	handler := newAuditLogHandler(&out, slog.LevelInfo)
	now := time.Now()

	start := slog.NewRecord(now, slog.LevelInfo, "请求开始", 0)
	start.AddAttrs(slog.String("event", "request_start"), slog.String("client", "alice"))
	if err := handler.Handle(context.Background(), start); err != nil {
		t.Fatal(err)
	}

	complete := slog.NewRecord(now, slog.LevelInfo, "请求完成", 0)
	complete.AddAttrs(slog.String("event", "request_complete"), slog.String("client", "alice"))
	if err := handler.Handle(context.Background(), complete); err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("lines = %d, body = %q", len(lines), out.String())
	}
	if !strings.Contains(lines[0], "event=request_complete") || strings.Contains(lines[0], "request_start") {
		t.Fatalf("unexpected audit line: %q", lines[0])
	}
}
