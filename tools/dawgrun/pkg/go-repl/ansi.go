package repl

import (
	"fmt"
	"os"
)

// TODO: dont use the functions that aren't supported by Windows

const _ESC = "\033"

func csi1(n int, char byte) {
	fmt.Fprintf(os.Stdout, "%s[%d%c", _ESC, n, char)
}

func csi2(n int, m int, char byte) {
	fmt.Fprintf(os.Stdout, "%s[%d;%d%c", _ESC, n, m, char)
}

func esc1(c byte) {
	fmt.Fprintf(os.Stdout, "%s[%c", _ESC, c)
}

func control(char byte) {
	fmt.Fprintf(os.Stdout, "%c", char)
}

func moveLeft() {
	csi1(1, 'D')
}

func moveRight() {
	csi1(1, 'C')
}

func clearScreen() {
	csi1(2, 'J')
}

func moveToRowStart() {
	csi1(1, 'G')
}

func moveToScreenStart() {
	csi2(1, 1, 'H')
}

func moveToRow(y int) {
	csi2(y+1, 1, 'H')
}

func clearRow() {
	csi1(2, 'K')
}

func clearRowAfterCursor() {
	csi1(0, 'K')
}

func clearRows(n int) {
	for i := 0; i < n; i++ {
		csi1(2, 'K')

		csi1(1, 'F')
	}
}

// input: 0-based
// moves to 1-based
func moveToCol(x int) {
	csi1(x+1, 'G')
}

func queryCursorPos() {
	csi1(6, 'n')
}

// from 0-based to 1-based!
func moveCursorTo(x, y int) {
	csi2(y+1, x+1, 'H')
}

func highlight() {
	// black text (30) on a grey/white background
	fmt.Fprintf(os.Stdout, "%s[48;5;247m%s[30m", _ESC, _ESC)
}

func resetDecorations() {
	fmt.Fprintf(os.Stdout, "%s[0m", _ESC)
}
