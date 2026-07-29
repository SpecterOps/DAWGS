//go:build !linux && !darwin

package main

import (
	"os"
)

func forceRenameNoReplace(_ *os.File, _, _ string) error {
	return validateForcePlatform("unsupported")
}
