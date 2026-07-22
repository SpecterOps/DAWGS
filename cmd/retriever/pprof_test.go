package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPprofHandler(t *testing.T) {
	server := httptest.NewServer(newPprofHandler())
	defer server.Close()

	for _, path := range []string{
		"/debug/pprof/",
		"/debug/pprof/heap?debug=1",
		"/debug/pprof/allocs?debug=1",
		"/debug/pprof/goroutine?debug=1",
	} {
		response, err := server.Client().Get(server.URL + path)
		if err != nil {
			t.Fatalf("get %s: %v", path, err)
		}

		body, readErr := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if readErr != nil {
			t.Fatalf("read %s: %v", path, readErr)
		}
		if closeErr != nil {
			t.Fatalf("close %s: %v", path, closeErr)
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("get %s: status %d", path, response.StatusCode)
		}
		if len(body) == 0 {
			t.Fatalf("get %s: empty response", path)
		}
	}

	for _, path := range []string{
		"/debug/pprof/profile?seconds=1",
		"/debug/pprof/trace?seconds=1",
	} {
		response, err := server.Client().Get(server.URL + path)
		if err != nil {
			t.Fatalf("get %s: %v", path, err)
		}
		_ = response.Body.Close()
		if response.StatusCode == http.StatusNotFound {
			t.Fatalf("profiling endpoint %s is not registered", path)
		}
	}

	response, err := server.Client().Get(server.URL + "/debug/pprof/cmdline")
	if err != nil {
		t.Fatalf("get cmdline endpoint: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("cmdline endpoint status = %d, want %d", response.StatusCode, http.StatusNotFound)
	}
}

func TestPprofServerLifecycle(t *testing.T) {
	var output bytes.Buffer

	server, err := startPprofServer("127.0.0.1:0", &output)
	if err != nil {
		t.Fatalf("start pprof server: %v", err)
	}
	if server == nil {
		t.Fatal("start pprof server: expected server")
	}

	client := &http.Client{
		Timeout: 5 * time.Second,
	}
	endpoint := "http://" + server.listener.Addr().String() + "/debug/pprof/"
	response, err := client.Get(endpoint)
	if err != nil {
		t.Fatalf("get pprof index: %v", err)
	}
	if closeErr := response.Body.Close(); closeErr != nil {
		t.Fatalf("close pprof response: %v", closeErr)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("get pprof index: status %d", response.StatusCode)
	}
	if !strings.Contains(output.String(), endpoint) {
		t.Fatalf("startup output %q does not contain %q", output.String(), endpoint)
	}

	if err := server.close(); err != nil {
		t.Fatalf("close pprof server: %v", err)
	}
}

func TestPprofServerDisabledAndInvalidAddress(t *testing.T) {
	server, err := startPprofServer("  ", io.Discard)
	if err != nil {
		t.Fatalf("disabled pprof server: %v", err)
	}
	if server != nil {
		t.Fatal("disabled pprof server: expected nil server")
	}

	server, err = startPprofServer("127.0.0.1:not-a-port", io.Discard)
	if err == nil || (!strings.Contains(err.Error(), "pprof listen address") && !strings.Contains(err.Error(), "listen for pprof")) {
		t.Fatalf("invalid pprof address: expected listen error, got %v", err)
	}
	if server != nil {
		t.Fatal("invalid pprof address: expected nil server")
	}
}

func TestPprofServerRejectsNonLoopbackAddress(t *testing.T) {
	for _, address := range []string{"0.0.0.0:6060", "[::]:6060", "192.0.2.1:6060", ":6060", "example.com:6060"} {
		t.Run(address, func(t *testing.T) {
			server, err := startPprofServer(address, io.Discard)
			if err == nil || !strings.Contains(err.Error(), "not loopback") {
				t.Fatalf("expected loopback validation error, got %v", err)
			}
			if server != nil {
				t.Fatal("expected nil pprof server")
			}
		})
	}

	for _, address := range []string{"127.0.0.1:0", "[::1]:0", "localhost:0"} {
		t.Run(address, func(t *testing.T) {
			if err := validatePprofListenAddress(address); err != nil {
				t.Fatalf("validate loopback address: %v", err)
			}
		})
	}
}

func TestLongRunningCommandsExposePprofFlag(t *testing.T) {
	for _, command := range []string{"dump", "load", "verify", "bench"} {
		var (
			stdout bytes.Buffer
			stderr bytes.Buffer
		)
		runtime := commandRuntime{
			stdout: &stdout,
			stderr: &stderr,
		}

		err := runtime.run(context.Background(), []string{command, "-h"})
		if !errors.Is(err, flag.ErrHelp) {
			t.Fatalf("%s help: expected flag.ErrHelp, got %v", command, err)
		}
		if !strings.Contains(stderr.String(), "-pprof-listen") {
			t.Fatalf("%s help does not include -pprof-listen", command)
		}
		if command == "dump" && !strings.Contains(stderr.String(), "-resume") {
			t.Fatalf("dump help does not include -resume")
		}
	}
}
