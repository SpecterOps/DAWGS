package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	httppprof "net/http/pprof"
	"strings"
	"time"
)

const (
	pprofReadHeaderTimeout = 5 * time.Second
	pprofIdleTimeout       = 60 * time.Second
	pprofShutdownTimeout   = 5 * time.Second
)

type pprofServer struct {
	server    *http.Server
	listener  net.Listener
	serveDone chan error
}

func commonPprofFlag(flags *flag.FlagSet, address *string) {
	flags.StringVar(address, "pprof-listen", "", "Optional pprof HTTP listen address, such as 127.0.0.1:6060; disabled when empty.")
}

func newPprofHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", httppprof.Index)
	mux.HandleFunc("/debug/pprof/profile", httppprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", httppprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", httppprof.Trace)

	return mux
}

func startPprofServer(address string, output io.Writer) (*pprofServer, error) {
	trimmedAddress := strings.TrimSpace(address)
	if trimmedAddress == "" {
		return nil, nil
	}
	if err := validatePprofListenAddress(trimmedAddress); err != nil {
		return nil, err
	}

	listener, err := net.Listen("tcp", trimmedAddress)
	if err != nil {
		return nil, fmt.Errorf("listen for pprof on %q: %w", trimmedAddress, err)
	}

	server := &http.Server{
		Handler:           newPprofHandler(),
		ReadHeaderTimeout: pprofReadHeaderTimeout,
		IdleTimeout:       pprofIdleTimeout,
	}
	result := &pprofServer{
		server:    server,
		listener:  listener,
		serveDone: make(chan error, 1),
	}

	go func() {
		result.serveDone <- server.Serve(listener)
	}()

	if output != nil {
		fmt.Fprintf(output, "pprof: http://%s/debug/pprof/\n", listener.Addr())
	}

	return result, nil
}

func validatePprofListenAddress(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid pprof listen address %q: %w", address, err)
	}

	if strings.EqualFold(host, "localhost") {
		return nil
	}

	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("pprof listen address %q is not loopback; use 127.0.0.1, [::1], or localhost", address)
	}

	return nil
}

func (s *pprofServer) close() error {
	if s == nil {
		return nil
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), pprofShutdownTimeout)
	defer cancel()

	shutdownErr := s.server.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		shutdownErr = errors.Join(shutdownErr, s.server.Close())
	}

	serveErr := <-s.serveDone
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}

	return errors.Join(shutdownErr, serveErr)
}

func stopPprofServer(server *pprofServer, output io.Writer) {
	if err := server.close(); err != nil && output != nil {
		fmt.Fprintf(output, "pprof: shutdown: %v\n", err)
	}
}
