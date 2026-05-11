// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func gitOutput(ctx context.Context, dir string, args ...string) (string, error) {
	output, err := runCommand(ctx, dir, nil, "git", args...)
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(output)), nil
}

func runCommand(ctx context.Context, dir string, env []string, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Env = os.Environ()
	if len(env) > 0 {
		cmd.Env = append(cmd.Env, env...)
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return output, commandError{
			Name:   name,
			Args:   args,
			Err:    err,
			Output: output,
		}
	}

	return output, nil
}

type commandError struct {
	Name   string
	Args   []string
	Err    error
	Output []byte
}

func (err commandError) Error() string {
	var builder strings.Builder
	builder.WriteString(err.Name)
	if len(err.Args) > 0 {
		builder.WriteByte(' ')
		builder.WriteString(strings.Join(err.Args, " "))
	}
	builder.WriteString(": ")
	builder.WriteString(err.Err.Error())

	output := bytes.TrimSpace(err.Output)
	if len(output) > 0 {
		builder.WriteString("\n")
		builder.Write(outputTail(output, 4096))
	}

	return builder.String()
}

func outputTail(output []byte, limit int) []byte {
	if len(output) <= limit {
		return output
	}

	prefix := []byte(fmt.Sprintf("... truncated %d bytes ...\n", len(output)-limit))
	return append(prefix, output[len(output)-limit:]...)
}
