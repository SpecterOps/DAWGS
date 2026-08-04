// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
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

package testutil

import "runtime/debug"

type BaselineMetadata struct {
	DAWGSVersion string `json:"dawgs_version"`
}

func ResolveBaselineMetadata(dawgsVersion string) BaselineMetadata {
	if dawgsVersion == "" {
		dawgsVersion = currentDAWGSVersion()
	}

	return BaselineMetadata{
		DAWGSVersion: dawgsVersion,
	}
}

func currentDAWGSVersion() string {
	buildInfo, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}

	version := buildInfo.Main.Version
	if version == "" {
		version = "(devel)"
	}

	for _, setting := range buildInfo.Settings {
		if setting.Key == "vcs.revision" && setting.Value != "" {
			return version + "@" + setting.Value
		}
	}

	return version
}
