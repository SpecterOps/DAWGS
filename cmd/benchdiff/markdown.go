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
	"bufio"
	"bytes"
	"strings"
	"time"
)

type markdownBenchmarkRow struct {
	Query   string
	Dataset string
	Median  time.Duration
}

func parseBenchmarkMarkdown(data []byte) []markdownBenchmarkRow {
	var rows []markdownBenchmarkRow
	scanner := bufio.NewScanner(bytes.NewReader(data))

	for scanner.Scan() {
		columns := splitMarkdownTableRow(scanner.Text())
		if len(columns) < 5 {
			continue
		}
		if columns[0] == "Query" || strings.HasPrefix(columns[0], "---") {
			continue
		}

		median, err := parseBenchmarkDuration(columns[2])
		if err != nil {
			continue
		}

		rows = append(rows, markdownBenchmarkRow{
			Query:   columns[0],
			Dataset: columns[1],
			Median:  median,
		})
	}

	return rows
}

func splitMarkdownTableRow(line string) []string {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, "|") || !strings.HasSuffix(trimmed, "|") {
		return nil
	}

	trimmed = strings.TrimPrefix(trimmed, "|")
	trimmed = strings.TrimSuffix(trimmed, "|")

	rawColumns := strings.Split(trimmed, "|")
	columns := make([]string, 0, len(rawColumns))
	for _, column := range rawColumns {
		columns = append(columns, strings.TrimSpace(column))
	}

	return columns
}
