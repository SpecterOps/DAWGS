// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckedInSPI2ProtocolV2MatchesCompiledContract(t *testing.T) {
	path := filepath.Join("..", "..", "benchmark", "testdata", "scale", "protocols", "sp_i2_distance_v2.json")
	protocol, digest, err := loadSPI2ProtocolV2(path)
	require.NoError(t, err)
	require.Len(t, digest, 64)
	require.Equal(t, spI2GenerationV2, protocol.Generation)
	require.Equal(t, spI2HierBootstrapV2, protocol.Identities.StatisticalImplementation)
}

func TestCheckedInSPI2V1TerminalRejection(t *testing.T) {
	path := filepath.Join("..", "..", "benchmark", "testdata", "scale", "protocols", "sp_i2_distance_v1_rejection.json")
	rejection, err := loadSPI2V1Rejection(path)
	require.NoError(t, err)
	require.True(t, rejection.Terminal)
	require.False(t, rejection.FreezeCreated)
	require.False(t, rejection.HoldoutOpened)
}

func TestSPI2V1CannotCreateFreezeOrAuthorizeHoldout(t *testing.T) {
	_, err := createSPI2QualificationReport("baseline", "candidate", "resource", "", "", "freeze.json", "report.json", SPI2QualificationOptions{})
	require.ErrorContains(t, err, "terminally rejected")
	require.True(t, spI2V1TerminallyRejected())
}

func TestSPI2ProtocolV2StrictlyRejectsDuplicateUnknownAndTrailingData(t *testing.T) {
	path := filepath.Join("..", "..", "benchmark", "testdata", "scale", "protocols", "sp_i2_distance_v2.json")
	raw, err := os.ReadFile(path)
	require.NoError(t, err)

	for name, mutated := range map[string][]byte{
		"duplicate": append([]byte(`{"schema":"collision",`), raw[1:]...),
		"unknown":   append([]byte(`{"unknown":true,`), raw[1:]...),
		"trailing":  append(append([]byte(nil), raw...), []byte(`{}`)...),
	} {
		t.Run(name, func(t *testing.T) {
			mutatedPath := filepath.Join(t.TempDir(), "protocol.json")
			require.NoError(t, os.WriteFile(mutatedPath, mutated, 0o600))
			_, _, err := loadSPI2ProtocolV2(mutatedPath)
			require.Error(t, err)
		})
	}
}
