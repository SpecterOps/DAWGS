//go:build !linux && !darwin

package ret

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeygenRemainsPortableWithoutArchivePublication(t *testing.T) {
	root := t.TempDir()
	privatePath := filepath.Join(root, "private.json")
	publicPath := filepath.Join(root, "public.json")

	err := Keygen(KeygenConfig{
		PrivateKeyPath: privatePath,
		PublicKeyPath:  publicPath,
	})

	require.NoError(t, err)
	require.FileExists(t, privatePath)
	require.FileExists(t, publicPath)
}
