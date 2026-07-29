//go:build darwin

package main

import (
	"os"

	"golang.org/x/sys/unix"
)

func forceRenameNoReplace(directory *os.File, oldName, newName string) error {
	return unix.RenameatxNp(
		int(directory.Fd()),
		oldName,
		int(directory.Fd()),
		newName,
		unix.RENAME_EXCL,
	)
}
