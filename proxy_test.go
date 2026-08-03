package main

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestProxyForwardsResponsesRequestTransparently(t *testing.T) {
	var seen atomic.Bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.Store(true)
		if r.URL.Path != "/v1/responses" {
			t.Errorf("path = %q, want /v1/responses", r.URL.Path)
		}
		if r.URL.RawQuery != "debug=1" {
			t.Errorf("query = %q, want debug=1", r.URL.RawQuery)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer upstream-key" {
			t.Errorf("Authorization = %q, want upstream key", got)
		}
		if got := r.Header.Get("X-Forwarded-For"); got != "" {
			t.Errorf("X-Forwarded-For = %q, want empty for transparent proxy", got)
		}
		body, _ := io.ReadAll(r.Body)
		wantBody := `{"model":"gpt-5.5","input":"你好，你是什么大模型？","stream":false}`
		if string(body) != wantBody {
			t.Errorf("body = %q, want %q", string(body), wantBody)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"resp_test","output_text":"我是一个测试模型"}`))
	}))
	defer upstream.Close()

	server := testProxy(t, []Channel{{Name: "test", BaseURL: upstream.URL + "/v1", APIKey: "upstream-key", Weight: 1}})

	req := httptest.NewRequest(http.MethodPost, "/v1/responses?debug=1", strings.NewReader(`{"model":"gpt-5.5","input":"你好，你是什么大模型？","stream":false}`))
	req.Header.Set("Authorization", "Bearer client-key")
	rec := httptest.NewRecorder()

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !seen.Load() {
		t.Fatal("upstream was not called")
	}
	if !strings.Contains(rec.Body.String(), "测试模型") {
		t.Fatalf("unexpected response body: %s", rec.Body.String())
	}
}

func TestProxyRejectsInvalidClientToken(t *testing.T) {
	server := testProxy(t, []Channel{{Name: "unused", BaseURL: "http://127.0.0.1:1/v1", APIKey: "upstream-key", Weight: 1}})
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer wrong")
	rec := httptest.NewRecorder()

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestProxyLogsRequestStartWithSelectedChannel(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	var logs strings.Builder
	server, err := newProxyServer(
		[]ClientToken{{Name: "alice", APIKey: "client-key"}},
		[]Channel{{Name: "primary", BaseURL: upstream.URL + "/v1", APIKey: "upstream-key", Weight: 1}},
		nil,
		slog.New(newHumanLogHandler(&logs, slog.LevelInfo)),
	)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/responses?debug=1", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer client-key")
	req.RemoteAddr = "192.0.2.10:34567"
	rec := httptest.NewRecorder()

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	body := logs.String()
	for _, want := range []string{
		"请求开始",
		"event=request_start",
		"client=alice",
		"method=POST",
		"path=/v1/responses?debug=1",
		"channel=primary",
		"upstream=" + upstream.URL + "/v1/responses?debug=1",
		"remote=192.0.2.10:34567",
		"upgrade=false",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("log missing %q in %q", want, body)
		}
	}
}

func TestProxyKeepsConcurrentUsersIsolated(t *testing.T) {
	const users = 20
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer upstream-key" {
			t.Errorf("Authorization = %q, want upstream key", got)
		}
		body, _ := io.ReadAll(r.Body)
		// Stagger responses so concurrent requests complete out of order.
		time.Sleep(time.Duration(len(body)%7) * 10 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"echo":%q}`, string(body))
	}))
	defer upstream.Close()

	tokens := make([]ClientToken, users)
	for i := 0; i < users; i++ {
		tokens[i] = ClientToken{Name: fmt.Sprintf("user-%02d", i), APIKey: fmt.Sprintf("client-key-%02d", i)}
	}
	server, err := newProxyServer(
		tokens,
		[]Channel{{Name: "concurrent", BaseURL: upstream.URL + "/v1", APIKey: "upstream-key", Weight: 1}},
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errCh := make(chan string, users)
	for i := 0; i < users; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			body := fmt.Sprintf(`{"user":"user-%02d","message":"hello-%02d"}`, i, i)
			req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
			req.Header.Set("Authorization", "Bearer "+tokens[i].APIKey)
			rec := httptest.NewRecorder()

			server.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				errCh <- fmt.Sprintf("user-%02d status = %d body = %s", i, rec.Code, rec.Body.String())
				return
			}
			want := fmt.Sprintf(`"echo":%q`, body)
			if !strings.Contains(rec.Body.String(), want) {
				errCh <- fmt.Sprintf("user-%02d got mismatched response: %s, want body %s", i, rec.Body.String(), body)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for msg := range errCh {
		t.Error(msg)
	}
}

func TestProxyPassesThroughUpstreamErrorBody(t *testing.T) {
	upstreamBody := `{"error":{"message":"exact upstream error","type":"upstream_error","code":"bad_model"}}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("X-Request-Id", "rid_passthrough_error")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(upstreamBody))
	}))
	defer upstream.Close()

	server := testProxy(t, []Channel{
		{Name: "first", BaseURL: upstream.URL + "/v1", APIKey: "upstream-key", Weight: 1},
		{Name: "unused", BaseURL: "http://127.0.0.1:1/v1", APIKey: "unused-key", Weight: 1},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.5","input":"bad","stream":false}`))
	req.Header.Set("Authorization", "Bearer client-key")
	rec := httptest.NewRecorder()

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	if rec.Body.String() != upstreamBody {
		t.Fatalf("body = %q, want exact upstream body %q", rec.Body.String(), upstreamBody)
	}
	if rec.Header().Get("X-Request-Id") != "rid_passthrough_error" {
		t.Fatalf("x-request-id not passed through: %q", rec.Header().Get("X-Request-Id"))
	}
}

func TestProxyFlushesStreamingResponses(t *testing.T) {
	firstChunk := make(chan time.Time, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer upstream-key" {
			t.Errorf("unexpected auth header: %q", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		_, _ = io.WriteString(w, "event: response.output_text.delta\ndata: {\"delta\":\"hello\"}\n\n")
		flusher.Flush()
		time.Sleep(250 * time.Millisecond)
		_, _ = io.WriteString(w, "event: response.completed\ndata: {\"type\":\"response.completed\"}\n\n")
		flusher.Flush()
	}))
	defer upstream.Close()

	server := httptest.NewServer(testProxy(t, []Channel{{Name: "stream", BaseURL: upstream.URL + "/v1", APIKey: "upstream-key", Weight: 1}}))
	defer server.Close()

	req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/responses", strings.NewReader(`{"model":"gpt-5.5","input":"你好","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer client-key")
	req.Header.Set("Accept", "text/event-stream")
	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	buf := make([]byte, 64)
	n, err := resp.Body.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	firstChunk <- time.Now()
	if !strings.Contains(string(buf[:n]), "response.output_text.delta") {
		t.Fatalf("first chunk = %q", string(buf[:n]))
	}
	if elapsed := (<-firstChunk).Sub(start); elapsed > 200*time.Millisecond {
		t.Fatalf("first streaming chunk arrived too late: %s", elapsed)
	}
}

func TestConfigReloaderReloadsTokenConfigWhenFileChanges(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "token.json")
	if err := os.WriteFile(tokenPath, []byte(`{"tokens":[{"name":"old","apiKey":"old-key"}]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	tokenCfg, err := loadTokenConfig(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	server, err := newProxyServer(
		tokenCfg.Tokens,
		[]Channel{{Name: "upstream", BaseURL: upstream.URL + "/v1", APIKey: "upstream-key", Weight: 1}},
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatal(err)
	}
	reloader := newConfigReloader(tokenPath, filepath.Join(dir, "missing-channel.json"), server.auth, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer old-key")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("old token before reload status = %d", rec.Code)
	}

	time.Sleep(20 * time.Millisecond)
	if err := os.WriteFile(tokenPath, []byte(`{"tokens":[{"name":"new","apiKey":"new-key"}]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	reloader.reloadTokens()

	req = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer old-key")
	rec = httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("old token after reload status = %d, want 401", rec.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer new-key")
	rec = httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("new token after reload status = %d body = %s", rec.Code, rec.Body.String())
	}
}

func TestConfigReloaderReloadsChannelConfigWhenFileChanges(t *testing.T) {
	var firstHits atomic.Int64
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstHits.Add(1)
		_, _ = w.Write([]byte(`{"upstream":"first"}`))
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer second-key" {
			t.Errorf("Authorization = %q, want second key", got)
		}
		_, _ = w.Write([]byte(`{"upstream":"second"}`))
	}))
	defer second.Close()

	dir := t.TempDir()
	channelPath := filepath.Join(dir, "channel.json")
	writeChannels := func(raw string) {
		t.Helper()
		if err := os.WriteFile(channelPath, []byte(raw), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeChannels(fmt.Sprintf(`{"channels":[{"name":"first","baseURL":%q,"apiKey":"first-key","weight":1}]}`, first.URL+"/v1"))
	channelCfg, err := loadChannelConfig(channelPath)
	if err != nil {
		t.Fatal(err)
	}
	state := newChannelStateStore(channelPath, channelCfg)
	server, err := newProxyServer(
		[]ClientToken{{Name: "client", APIKey: "client-key"}},
		channelCfg.Channels,
		state,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatal(err)
	}
	reloader := newConfigReloader(filepath.Join(dir, "missing-token.json"), channelPath, nil, state, slog.New(slog.NewTextHandler(io.Discard, nil)))

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer client-key")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "first") {
		t.Fatalf("first response status = %d body = %s", rec.Code, rec.Body.String())
	}

	time.Sleep(20 * time.Millisecond)
	writeChannels(fmt.Sprintf(`{"channels":[{"name":"second","baseURL":%q,"apiKey":"second-key","weight":1}]}`, second.URL+"/v1"))
	reloader.reloadChannels()

	req = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer client-key")
	rec = httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "second") {
		t.Fatalf("second response status = %d body = %s", rec.Code, rec.Body.String())
	}
	if got := firstHits.Load(); got != 1 {
		t.Fatalf("first upstream hits = %d, want 1", got)
	}
}

func TestBuildTargetURLAvoidsDuplicateV1(t *testing.T) {
	in := httptest.NewRequest(http.MethodPost, "/v1/responses/compact?a=b", nil).URL
	got, err := buildTargetURL("http://example.com/v1", in)
	if err != nil {
		t.Fatal(err)
	}
	if got.String() != "http://example.com/v1/responses/compact?a=b" {
		t.Fatalf("target = %s", got.String())
	}
}

func TestEffectiveChannelWeightUsesLogPenalty(t *testing.T) {
	base := effectiveChannelWeight(Channel{Name: "ok", Weight: 10, ErrorCount: 0})
	lowError := effectiveChannelWeight(Channel{Name: "low", Weight: 10, ErrorCount: 1})
	highError := effectiveChannelWeight(Channel{Name: "high", Weight: 10, ErrorCount: 3000})

	if base != 10 {
		t.Fatalf("base effective weight = %v, want 10", base)
	}
	if !(lowError < base) {
		t.Fatalf("low error effective weight %v should be below base %v", lowError, base)
	}
	if !(highError < lowError) {
		t.Fatalf("high error effective weight %v should be below low error %v", highError, lowError)
	}
	if highError <= 0 {
		t.Fatalf("high error effective weight should remain selectable, got %v", highError)
	}
}

func testProxy(t *testing.T, channels []Channel) http.Handler {
	t.Helper()
	server, err := newProxyServer(
		[]ClientToken{{Name: "client", APIKey: "client-key"}},
		channels,
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatal(err)
	}
	return server
}
