//go:build linux

package archive

import (
	"os"

	"golang.org/x/sys/unix"
)

func renameNoReplace(directory *os.File, _ string, oldName, newName string) error {
	return unix.Renameat2(
		int(directory.Fd()),
		oldName,
		int(directory.Fd()),
		newName,
		unix.RENAME_NOREPLACE,
	)
}
