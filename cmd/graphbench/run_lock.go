// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// destructiveRunLock prevents two local GraphBench processes from clearing
// and reloading the same benchmark targets concurrently. Distributed runners
// must additionally allocate a unique disposable database, as documented by
// the command.
type destructiveRunLock struct {
	file *os.File
}

func acquireDestructiveRunLock(path string) (*destructiveRunLock, error) {
	if path == "" {
		return nil, fmt.Errorf("destructive lock path must not be empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create destructive lock directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open destructive lock: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("another GraphBench process holds destructive lock %s: %w", path, err)
	}
	if err := file.Truncate(0); err == nil {
		_, _ = fmt.Fprintf(file, "pid=%d\n", os.Getpid())
	}
	return &destructiveRunLock{file: file}, nil
}

func (s *destructiveRunLock) Close() error {
	if s == nil || s.file == nil {
		return nil
	}
	unlockErr := syscall.Flock(int(s.file.Fd()), syscall.LOCK_UN)
	closeErr := s.file.Close()
	if unlockErr != nil {
		return unlockErr
	}
	return closeErr
}
