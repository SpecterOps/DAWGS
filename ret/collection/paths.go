package collection

import (
	"fmt"
	"net/url"
	"path"
	"path/filepath"
	"strings"

	"github.com/specterops/dawgs/ret/jsonl"
)

func SafeJoin(root, relative string) (string, error) {
	if root == "" {
		return "", fmt.Errorf("safe join root is empty")
	}
	if relative == "" || relative == "." {
		return "", fmt.Errorf("safe join path is empty")
	}
	if strings.Contains(relative, `\`) {
		return "", fmt.Errorf("safe join path contains a backslash: %q", relative)
	}
	if strings.ContainsRune(relative, '\x00') {
		return "", fmt.Errorf("safe join path contains NUL: %q", relative)
	}
	if path.IsAbs(relative) || filepath.IsAbs(relative) {
		return "", fmt.Errorf("safe join path is absolute: %q", relative)
	}
	if clean := path.Clean(relative); clean != relative {
		return "", fmt.Errorf("safe join path is not clean: %q", relative)
	}
	if relative == ".." || strings.HasPrefix(relative, "../") {
		return "", fmt.Errorf("safe join path traverses its root: %q", relative)
	}

	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("resolve safe join root: %w", err)
	}
	joined := filepath.Join(absoluteRoot, filepath.FromSlash(relative))
	contained, err := filepath.Rel(absoluteRoot, joined)
	if err != nil {
		return "", fmt.Errorf("check safe join containment: %w", err)
	}
	if contained == "." || contained == ".." || strings.HasPrefix(contained, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("safe join path escapes its root: %q", relative)
	}

	return joined, nil
}

func NodeJSONLPath(graph string, shard int, codec jsonl.Codec) string {
	return entityJSONLPath(graph, "nodes", shard, codec)
}

func RelationshipJSONLPath(graph string, shard int, codec jsonl.Codec) string {
	return entityJSONLPath(graph, "relationships", shard, codec)
}

func NodeParquetPath(graph string, shard int) string {
	return entityParquetPath(graph, "nodes", shard)
}

func RelationshipParquetPath(graph string, shard int) string {
	return entityParquetPath(graph, "relationships", shard)
}

func entityJSONLPath(graph, entityType string, shard int, codec jsonl.Codec) string {
	var suffix string
	switch codec {
	case jsonl.CodecNone:
		suffix = ".jsonl"
	case jsonl.CodecGzip:
		suffix = ".jsonl.gz"
	case jsonl.CodecZstd:
		suffix = ".jsonl.zst"
	default:
		panic(fmt.Sprintf("unsupported JSONL codec %q", codec))
	}

	return entityPath(graph, entityType, shard, suffix)
}

func entityParquetPath(graph, entityType string, shard int) string {
	return entityPath(graph, entityType, shard, ".parquet")
}

func entityPath(graph, entityType string, shard int, suffix string) string {
	if shard < 1 {
		panic(fmt.Sprintf("shard index must be at least one: %d", shard))
	}

	return fmt.Sprintf("graphs/%s/%s/%06d%s", url.PathEscape(graph), entityType, shard, suffix)
}
