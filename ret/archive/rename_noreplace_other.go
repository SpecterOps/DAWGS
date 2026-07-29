//go:build !linux && !darwin && !windows

package archive

import (
	"os"
	"runtime"
)

func renameNoReplace(_ *os.File, _ string, _, _ string) error {
	return requirePlatformSupport(runtime.GOOS)
}
