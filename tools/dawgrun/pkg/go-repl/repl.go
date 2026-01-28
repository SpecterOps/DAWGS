// Lightweight Golang REPL library, inspired by GNU readline. You provide the Eval function, and go-repl does the rest.
package repl

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"golang.org/x/term"
)

var (
	// Period between polls for terminal size changes.
	// 10ms is the default, human reaction times are an order of magnitude slower than this interval,
	// and auto generated escape sequence bytes are an order of magnitude faster than this interval.
	SIZE_POLLING_INTERVAL = 10 * time.Millisecond

	// Used by the package maintainer:
	DEBUG = "" // a non-empty string specifies the destination file for debugging info
)

type (
	StatusWidgetFn  func(*Repl) string
	StatusWidgetFns struct {
		Left  StatusWidgetFn
		Right StatusWidgetFn
	}
)

type Repl struct {
	handler Handler

	StatusWidgets *StatusWidgetFns

	history     [][]byte // simply keep everything, it doesn't matter
	historyIdx  int      // -1 for last
	historyFile *os.File // open history file, so we can keep appending

	phraseRe *regexp.Regexp

	reader *_StdinReader

	buffer    []byte // input bytes are accumulated
	backup    []byte // we can go into a history line, and start editing it
	prevDel   []byte // previous deletion
	filter    []byte // for reverse search
	bufferPos int    // position in the buffer (0-based)
	viewStart int    // usually 0, but can be positive in case of very large inputs
	viewEnd   int    //
	promptRow int    // 0-based
	width     int
	height    int

	onEnd func()
	debug *os.File
}

type Options struct {
	// HistoryFilePath is the path to the file that console history is stored in
	HistoryFilePath string
	// HistoryFileMaxLines is the maximum number of lines of the history that should be kept
	// before it gets truncated
	HistoryMaxLines int
	// StatusWidgets is a struct containing widgets used for content when rendering the status
	// bar to screen
	StatusWidgets *StatusWidgetFns
}

// Create a new Repl using your custom Handler.
func NewRepl(handler Handler, opts *Options) *Repl {
	r := &Repl{
		handler:     handler,
		history:     make([][]byte, 0),
		historyIdx:  -1,
		historyFile: nil,
		phraseRe:    regexp.MustCompile(`([0-9a-zA-Z_\-\.]+)`),
		reader:      newStdinReader(),
		buffer:      nil,
		backup:      nil,
		prevDel:     nil,
		filter:      nil,
		bufferPos:   0,
		viewStart:   0,
		viewEnd:     -1,
		promptRow:   -1,
		width:       0,
		height:      0,
		onEnd:       nil,
		debug:       nil,
	}

	if DEBUG != "" {
		debug, err := os.Create(DEBUG)
		if err != nil {
			panic(fmt.Errorf("error start repl (debug): %w", err))
		}
		r.debug = debug
	}

	if opts != nil {
		if err := r.loadOptions(opts); err != nil {
			panic(fmt.Errorf("error starting repl (options): %w", err))
		}
	}

	return r
}

///////////////////
// internal methods
///////////////////

func (r *Repl) loadOptions(opts *Options) error {
	// Open history file from HistoryFilePath
	historyFile, err := os.OpenFile(opts.HistoryFilePath, os.O_CREATE|os.O_RDWR|io.SeekStart, 0o660)
	if err != nil {
		return fmt.Errorf("could not open history file: %w", err)
	}

	r.historyFile = historyFile

	// Load history file into the buffer
	if err := r.loadHistory(); err != nil {
		return fmt.Errorf("could not load history from file: %w", err)
	}

	r.StatusWidgets = opts.StatusWidgets

	return nil
}

func (r *Repl) loadHistory() error {
	historyReader := bufio.NewReader(r.historyFile)
	reading := true
	for reading {
		line, err := historyReader.ReadBytes('\n')
		if err != nil {
			switch err {
			case io.EOF:
				reading = false
				break
			default:
				return fmt.Errorf("could not read from history file: %w", err)
			}
		}

		if line == nil {
			continue
		}

		r.history = append(r.history, bytes.TrimSpace(line))
	}

	return nil
}

func (r *Repl) saveHistory() error {
	if err := r.historyFile.Truncate(0); err != nil {
		return fmt.Errorf("could not truncate history file: %w", err)
	}

	historyWriter := bufio.NewWriter(r.historyFile)
	for _, line := range r.history {
		line := bytes.TrimSpace(line)
		// Filter blank lines
		if len(line) == 0 {
			continue
		}

		if n, err := historyWriter.Write(append(line, byte('\n'))); n < len(line) || err != nil {
			return fmt.Errorf("could not write history to file (%d bytes written): %w", n, err)
		}
	}

	if err := historyWriter.Flush(); err != nil {
		return fmt.Errorf("could not flush writer: %w", err)
	}

	if err := r.historyFile.Close(); err != nil {
		return fmt.Errorf("could not close history file: %w", err)
	}

	return nil
}

func (r *Repl) getWidth() int {
	return r.width
}

func (r *Repl) getHeight() int {
	return r.height
}

func (r *Repl) innerHeight() int {
	if r.statusVisible() {
		return r.getHeight() - 1
	} else {
		return r.getHeight()
	}
}

func (r *Repl) log(format string, args ...interface{}) {
	if r.debug != nil {
		fmt.Fprintf(r.debug, format, args...)
	}
}

func (r *Repl) notifySizeChange() {
	getSize := func() (int, int) {
		w, h, err := term.GetSize(0)
		if err != nil {
			panic(err)
		}

		return w, h
	}

	r.width, r.height = getSize()

	go func() {
		for {
			<-time.After(SIZE_POLLING_INTERVAL)

			newW, newH := getSize()

			r.resize(newW, newH)
		}
	}()
}

func (r *Repl) resize(w, h int) {
	if w != r.width || h != r.height {
		r.width, r.height = w, h

		r.force(r.buffer, r.bufferPos)
	}
}

func (r *Repl) searchActive() bool {
	return r.filter != nil
}

func (r *Repl) stopSearch() {
	r.filter = nil

	r.clearStatus()
	r.writeStatus()
}

// turn stdin bytes into something useful
func (r *Repl) dispatch(b []byte) {
	n := len(b)

	r.log("keypress: %v\n", b)

	if n == 1 {
		switch b[0] {
		case 0: // NULL, or CTRL-2
			return
		case 1: // CTRL-A
			r.moveToBufferStart()
		case 2: // CTRL-B
			r.moveLeftOneChar()
		case 3: // CTRL-C
			if r.searchActive() {
				r.stopSearch()
			}

			r.clearBuffer()
			r.writeStatus()
		case 4: // CTRL-D
			r.quit()
		case 5: // CTRL-E
			r.moveToBufferEnd()
		case 6: // CTRL-F
			r.moveRightOneChar()
		case 8: // CTRL-H
			r.backspaceActiveBuffer()
		case 9: // TAB
			if r.searchActive() {
				r.stopSearch()
			} else {
				r.tab()
			}
		case 10: // SHIFT-ENTER
			if r.searchActive() {
				r.stopSearch()
			} else {
				r.clearStatus()
				r.addBytesToBuffer([]byte{'\n'})
				r.writeStatus()
			}
		case 11: // CTRL-K
			if r.searchActive() {
				r.stopSearch()
			} else {
				r.clearToEnd()
			}
		case 12: // CTRL-L
			r.redrawScreen()
		case 13: // RETURN
			if r.searchActive() {
				r.stopSearch()
			} else {
				r.evalBuffer()
			}
		case 14: // CTRL-N
			r.historyForward()
		case 16: // CTRL-P
			r.historyBack()
		case 17: // CTRL-Q
			if r.searchActive() {
				r.stopSearch()
			} else {
				r.clearOnePhraseRight()
			}
		case 18: // CTRL-R
			if !r.searchActive() {
				r.startReverseSearch()
			}
		case 21: // CTRL-U
			if r.searchActive() {
				r.stopSearch()
			} else {
				r.clearToStart()
			}
		case 22: // CTRL-V
			return
		case 25: // CTRL-Y
			if r.searchActive() {
				r.stopSearch()
			} else {
				r.clearStatus()
				r.insertPrevDel()
				r.writeStatus()
			}
		case 23: // CTRL-W
			if r.searchActive() {
				r.stopSearch()
			} else {
				r.clearOnePhraseLeft()
			}
		case 27: // ESC
			if r.searchActive() {
				r.stopSearch()
			} else {
				r.clearBuffer()
				r.writeStatus()
			}
		case 127: // BACKSPACE
			r.backspaceActiveBuffer()
		default:
			if b[0] >= 32 {
				if r.searchActive() {
					r.filter = append(r.filter, b[0])

					r.updateSearchResult()
				} else {
					r.clearStatus()
					r.addBytesToBuffer([]byte{b[0]})
				}
				r.writeStatus()
			}
		}
	} else if n == 2 && b[0] == 195 {
		// ALT + KEY
	} else if n > 2 && b[0] == 27 && b[1] == 79 { // [ESCAPE, O, ...]
		switch b[2] {
		case 80: // F1
		case 81: // F2
		// ...
		default:
			// function keys not yet supported
		}
	} else if n > 2 && b[0] == 27 && b[1] == 91 { // [ESCAPE, OPEN_BRACKET, ...]
		if n == 3 {
			switch b[2] {
			case 65:
				r.historyBack()
			case 66:
				r.historyForward()
			case 67: // ArrowRight
				r.moveRightOneChar()
			case 68: // ArrowLeft
				r.moveLeftOneChar()
			case 72:
				r.moveToBufferStart()
			case 70:
				r.moveToBufferEnd()
			}
		} else if n == 4 {
			if b[2] == 51 && b[3] == 126 {
				r.deleteChar()
			}
		} else if n == 6 && b[2] == 49 && b[3] == 59 {
			if b[4] == 53 && b[5] == 68 { // CTRL-ArrowLeft
				r.moveLeftOnePhrase()
			} else if b[4] == 53 && b[5] == 67 {
				r.moveRightOnePhrase()
			} else if b[4] == 53 && b[5] == 66 {
				// r.moveDownOneLine()
			} else if b[4] == 53 && b[5] == 65 {
				// r.moveUpOneLine()
			}
		} else if len(b) > 5 && b[n-1] == 82 {
			parts := strings.Split(string(b[2:n-1]), ";")
			row, err := strconv.Atoi(parts[0])
			if err == nil {
				col, err := strconv.Atoi(parts[1])
				if err == nil {
					r.handleCursorQuery(col-1, row-1)
				}
			}
		}
	} else if len(b) > 6 && b[n-1] == 82 {
		// go backwards until the esc char
		for i := n - 2; i >= 0; i-- {
			if b[i] == 27 && b[i+1] == 91 {
				parts := strings.Split(string(b[i+2:n-1]), ";")
				row, err := strconv.Atoi(parts[0])
				if err == nil {
					col, err := strconv.Atoi(parts[1])
					if err == nil {
						r.handleCursorQuery(col-1, row-1)
					}
				}

				printable := make([]byte, 0)
				for _, b_ := range b[0:i] {
					if b_ >= 32 {
						printable = append(printable, b_)
					}
				}

				if len(printable) > 0 {
					r.clearStatus()
					r.addBytesToBuffer(printable)
					r.writeStatus()
				}

				break
			}
		}
	} else {
		r.cleanAndAddToBuffer(b)
	}

	return
}

func (r *Repl) handleCursorQuery(x, y int) {
	r.updatePromptRow(y)

	r.writeStatus()
}

func (r *Repl) printPrompt() {
	moveToRowStart()
	fmt.Print(r.handler.Prompt())
}

func (r *Repl) resetBuffer() {
	r.bufferPos = 0
	r.buffer = make([]byte, 0)
	r.printPrompt()
	r.viewStart = 0
	r.viewEnd = -1
}

func (r *Repl) overflow() bool {
	b := r.calcHeight() > r.innerHeight()
	if !b {
		r.viewStart = 0
		r.viewEnd = -1
	}
	return b
}

func (r *Repl) viewOverflow() bool {
	return r.calcViewHeight() > r.innerHeight()
}

func (r *Repl) boundPromptRow() {
	n := r.viewEnd
	if n < 0 {
		n = r.bufferLen()
	}

	xe, ye := r.cursorCoord(n)

	if ye >= r.innerHeight() {
		moveCursorTo(xe, ye)
		fmt.Print("\n")
		r.updatePromptRow(r.promptRow - (ye + 1 - r.innerHeight()))
	}
}

func (r *Repl) addBytesToBuffer(bs []byte) {
	if r.bufferPos == r.bufferLen() {
		xBef, _ := r.cursorCoord(-1)

		r.bufferPos += len(bs)
		len_ := r.bufferLen()
		r.buffer = append(r.buffer, bs...)

		if !r.overflow() {
			needSync := false
			for _, b := range bs {
				r.writeByte(b)

				if b != '\n' && xBef == r.getWidth()-1 {
					needSync = true
				}
			}

			if needSync {
				r.syncCursor()
			}

			r.boundPromptRow()

			return
		} else {
			// reset prev changes
			r.bufferPos -= len(bs)
			r.buffer = r.buffer[0:len_]
		}
	}

	tail := r.buffer[r.bufferPos:]

	newBuffer := make([]byte, 0)
	newBuffer = append(newBuffer, r.buffer[0:r.bufferPos]...)
	newBuffer = append(newBuffer, bs...)
	newBuffer = append(newBuffer, tail...)

	newPos := r.bufferPos + len(bs)

	r.force(newBuffer, newPos) // force should take into account extra long lines
}

func (r *Repl) promptLen() int {
	return len(r.handler.Prompt())
}

func (r *Repl) bufferLen() int {
	return len(r.buffer)
}

func relCursorCoord(buffer []byte, x0 int, bufferPos int, w int) (int, int) {
	x := x0
	y := 0

	for j, c := range buffer {
		if j >= bufferPos {
			break
		} else if c == '\n' {
			x = 0
			y += 1
		} else {
			x += 1
		}

		if x == w {
			x = 0
			y += 1
		}
	}

	return x, y
}

func calcHeight(buffer []byte, x0 int, w int) int {
	_, y := relCursorCoord(buffer, x0, len(buffer), w)
	return y + 1
}

func (r *Repl) calcHeight() int {
	return calcHeight(r.buffer, r.promptLen(), r.getWidth())
}

func (r *Repl) calcViewHeight() int {
	if r.viewEnd > r.bufferLen() {
		r.viewEnd = r.bufferLen()
	}

	return calcHeight(r.buffer[r.viewStart:r.viewEnd], r.promptLen(), r.getWidth())
}

func (r *Repl) calcViewStartHeight() int {
	return calcHeight(r.buffer[0:r.viewStart], r.promptLen(), r.getWidth())
}

func (r *Repl) calcViewEndHeight() int {
	return r.calcHeight() - r.calcViewHeight()
}

// i is 0-based index in current buffer
func (r *Repl) cursorCoord(bufferPos int) (int, int) {
	w := r.getWidth()

	if bufferPos < 0 {
		bufferPos = r.bufferPos
	}

	x, y := relCursorCoord(r.buffer[r.viewStart:], r.promptLen(), bufferPos-r.viewStart, w)

	y += r.promptRow

	return x, y
}

// return bufferPos that matches (x,y) as best as possible
func (r *Repl) calcBufferPos(x, y int) int {
	xc := r.promptLen()
	yc := r.promptRow

	for i, c := range r.buffer[r.viewStart:] {
		if yc > y {
			r.log("overshoot\n")
			return i - 1 + r.viewStart
		} else if yc == y && xc >= x {
			r.log("calc pos for %d,%d -> %d (%d,%d)\n", x, y, i+r.viewStart, xc, yc)
			return i + r.viewStart
		}

		if c == '\n' {
			xc = 0
			yc += 1
		} else {
			xc += 1
		}

		if xc == r.getWidth() {
			xc = 0
			yc += 1
		}

	}

	if r.viewEnd >= 0 {
		return r.viewEnd
	} else {
		return r.bufferLen()
	}
}

func (r *Repl) clearAfterPrompt() {
	moveCursorTo(0, r.getHeight()-1)

	if r.promptRow < 0 {
		r.updatePromptRow(0)
	}

	dy := (r.getHeight() - 1 - r.promptRow)

	clearRows(dy)
}

// clear as much as possible
func (r *Repl) clearBuffer() {
	moveCursorTo(0, r.getHeight()-1)

	r.log("clearing buffer\n")
	if r.promptRow < 0 {
		r.updatePromptRow(0)
	}

	dy := (r.getHeight() - 1 - r.promptRow)

	clearRows(dy)
	clearRow()

	r.resetBuffer()
}

func copyBytes(b []byte) []byte {
	l := make([]byte, len(b))

	for i, c := range b {
		l[i] = c
	}

	return l
}

func (r *Repl) adjustBufferView() {
	if r.bufferPos < r.viewStart {
		r.viewStart = r.bufferPos
		r.viewEnd = r.bufferLen()

		for r.viewOverflow() {
			r.viewEnd -= 1
		}
	} else if r.bufferPos > r.viewEnd {
		r.viewEnd = r.bufferPos
		for r.viewOverflow() {
			r.viewStart += 1
		}
	} else if r.viewOverflow() {
		r.viewEnd = r.bufferLen()

		for r.viewOverflow() {
			r.viewEnd -= 1
		}
	} else {
		for !r.viewOverflow() && r.viewEnd < r.bufferLen() {
			r.viewEnd += 1
		}

		for r.viewOverflow() {
			r.viewEnd -= 1
		}
	}
}

// this works for a single line
func (r *Repl) force(newBuffer []byte, bufferPos int) {
	newBuffer = copyBytes(newBuffer)

	r.clearStatus()

	r.log("overflow? %d vs %d\n", calcHeight(newBuffer, r.promptLen(), r.getWidth()), r.innerHeight())
	if calcHeight(newBuffer, r.promptLen(), r.getWidth()) > r.innerHeight() {
		viewStart_, viewEnd_ := r.viewStart, r.viewEnd
		r.clearScreen()
		r.buffer = newBuffer
		r.bufferPos = bufferPos
		r.viewStart, r.viewEnd = viewStart_, viewEnd_
		r.log("viewStart: %d, viewEnd: %d\n", r.viewStart, r.viewEnd)
		r.adjustBufferView()

		r.log("writing bytes from %d to %d (instead of 0 to %d) (bpos: %d)\n", r.viewStart, r.viewEnd, r.bufferLen(), r.bufferPos)

		for _, b := range r.buffer[r.viewStart:r.viewEnd] {
			r.writeByte(b)
		}

		r.syncCursor()
		// what is the appropriate bufferOffset? The minimal movement to keep the /move
	} else {
		r.clearBuffer()

		// TODO: writeBytes instead
		r.addBytesToBuffer(newBuffer)

		if bufferPos >= r.bufferLen() {
			bufferPos = r.bufferLen()
		}

		r.bufferPos = bufferPos

		r.log("bufferPos: %d, bufferLen: %d, width: %d\n", r.bufferPos, len(r.buffer), r.getWidth())
		r.syncCursor()
	}

	r.writeStatus()
}

func (r *Repl) syncCursor() {
	x, y := r.cursorCoord(-1)
	moveCursorTo(x, y)
}

func (r *Repl) evalBuffer() {
	r.clearStatus()

	r.newLine()

	// input that is sent to stdin while the handler is blocking, is returned the next time we read bytes from the stdinreader, followed by a sequence indicating the new cursor position (due to queryCursorPos() being called below), so the routine that handles the cursor pos query should also handle any preceding bytes
	out := r.handler.Eval(strings.TrimSpace(string(r.buffer)))

	if len(out) > 0 {
		outLines := strings.Split(out, "\n")

		for _, outLine := range outLines {
			fmt.Print(outLine)
			r.newLine()
		}
	}

	r.appendToHistory(r.buffer)
	r.historyIdx = -1

	r.backup = nil

	r.resetBuffer()

	queryCursorPos()
}

func (r *Repl) redraw() {
	r.force(r.buffer, r.bufferPos)
}

func (r *Repl) syncCursorOverflow() {
	if r.overflow() {
		r.redraw()
	} else {
		r.syncCursor()
	}
}

func (r *Repl) moveToBufferEnd() {
	if r.searchActive() {
		r.stopSearch()
	} else {
		r.bufferPos = r.bufferLen()

		r.syncCursorOverflow()
	}
}

func (r *Repl) moveToBufferStart() {
	if r.searchActive() {
		r.stopSearch()
	} else {
		r.bufferPos = 0

		r.syncCursorOverflow()
	}
}

func (r *Repl) moveLeftOneChar() {
	if r.searchActive() {
		r.stopSearch()
	} else {
		if r.bufferPos > 0 {
			r.bufferPos -= 1

			if r.overflow() {
				if r.bufferPos <= r.viewStart {
					r.redraw()
					return
				}
			}

			r.syncCursor()
		}
	}
}

func (r *Repl) moveRightOneChar() {
	if r.searchActive() {
		r.stopSearch()
	} else {
		if r.bufferPos < r.bufferLen() {
			r.bufferPos += 1

			if r.overflow() {
				if r.bufferPos >= r.viewEnd {
					r.redraw()
					return
				}
			}

			r.syncCursor()
		}
	}
}

func (r *Repl) moveUpOneLine() {
	x, y := r.cursorCoord(-1)

	h0 := r.calcViewStartHeight()
	_, y0 := r.cursorCoord(r.viewStart)

	if ((h0 > 0) && (y >= y0)) || y > y0 {
		// problem is that y is in view space, and
		r.bufferPos = r.calcBufferPos(x, y-1)

		if r.overflow() {
			if r.bufferPos <= r.viewStart {
				r.redraw()
				return
			}
		}

		r.syncCursor()
	}
}

func (r *Repl) moveDownOneLine() {
	x, y := r.cursorCoord(-1)

	_, ye := r.cursorCoord(r.viewEnd)
	he := r.calcViewEndHeight()

	if y < ye || (y <= ye && he > 0) {
		r.bufferPos = r.calcBufferPos(x, y+1)

		if r.overflow() {
			if r.bufferPos >= r.viewEnd {
				r.redraw()
				return
			}
		}

		r.syncCursor()
	}
}

func (r *Repl) moveLeftOnePhrase() {
	newPos, ok := r.prevPhrasePos()
	if ok {
		r.bufferPos = newPos

		if r.overflow() {
			if r.bufferPos <= r.viewStart {
				r.redraw()
				return
			}
		}

		r.syncCursor()
	}
}

func (r *Repl) moveRightOnePhrase() {
	newPos, ok := r.nextPhrasePos()
	if ok {
		r.bufferPos = newPos

		if r.overflow() {
			if r.bufferPos >= r.viewEnd {
				r.redraw()
				return
			}
		}

		r.syncCursor()
	}
}

// dont append if the same as the previous
func (r *Repl) appendToHistory(entry []byte) {
	n := len(r.history)

	if n == 0 {
		r.history = append(r.history, entry)
	} else if string(r.history[n-1]) != string(entry) {
		r.history = append(r.history, entry)
	}
}

func (r *Repl) useHistoryEntry(i int) {
	if i == -1 {
		r.historyIdx = -1

		if r.backup != nil {
			r.force(r.backup, len(r.backup))
		}

		r.backup = nil
	} else {
		if r.backup == nil {
			r.backup = r.buffer
		}

		r.historyIdx = i

		entry := r.history[i]

		r.force(entry, len(entry))
	}
}

func (r *Repl) historyForward() {
	if r.searchActive() {
		if r.historyIdx >= 0 && r.historyIdx < len(r.history)-1 {
			for i := r.historyIdx + 1; i < len(r.history); i++ {
				if r.filterMatches(r.history[i]) {
					r.useHistoryEntry(i)
					return
				}
			}
		}
	} else {
		if r.historyIdx != -1 {
			if r.historyIdx < len(r.history)-1 {
				r.useHistoryEntry(r.historyIdx + 1)
			} else {
				r.useHistoryEntry(-1)
			}
		}
	}
}

func (r *Repl) historyBack() {
	if r.searchActive() {
		if r.historyIdx > 0 {
			for i := r.historyIdx - 1; i >= 0; i-- {
				if r.filterMatches(r.history[i]) {
					r.useHistoryEntry(i)
					return
				}
			}
		}
	} else {
		if r.historyIdx == -1 {
			if len(r.history) > 0 {
				r.useHistoryEntry(len(r.history) - 1)
			}
		} else if r.historyIdx > 0 {
			r.useHistoryEntry(r.historyIdx - 1)
		}
	}
}

func (r *Repl) startReverseSearch() {
	r.filter = make([]byte, 0)

	r.clearStatus()
	r.writeStatus()
}

func (r *Repl) tab() {
	prec := string(r.buffer[0:r.bufferPos])

	extra := r.handler.Tab(prec)

	if len(extra) > 0 {
		r.addBytesToBuffer([]byte(extra))
	}
}

func (r *Repl) quit() {
	r.clearAfterPrompt()
	r.saveHistory()

	fmt.Print("\n\r")

	moveToRowStart()

	r.UnmakeRaw()

	os.Exit(0)
}

func (r *Repl) redrawScreen() {
	buffer := r.buffer
	bufferPos := r.bufferPos

	r.clearScreen()

	r.force(buffer, bufferPos)
}

func (r *Repl) clearScreen() {
	clearScreen()

	moveToScreenStart()

	r.updatePromptRow(0)

	r.resetBuffer()
}

func (r *Repl) backspaceActiveBuffer() {
	if r.searchActive() {
		n := len(r.filter)
		if n > 0 {
			r.filter = r.filter[0 : n-1]
		}

		r.updateSearchResult()

		r.clearStatus()
		r.writeStatus()
	} else {
		r.backspace()
	}
}

func (r *Repl) backspace() {
	n := r.bufferLen()

	if n > 0 {
		if r.bufferPos > 0 {
			newPos := r.bufferPos - 1
			newBuffer := append(r.buffer[0:newPos], r.buffer[newPos+1:len(r.buffer)]...)

			_, y0 := r.cursorCoord(-1)
			x1, y1 := r.cursorCoord(newPos)

			if y0 == y1 && r.bufferPos == len(r.buffer) && !r.overflow() {
				moveToCol(x1)
				clearRowAfterCursor()
				r.buffer = newBuffer
				r.bufferPos = newPos
			} else {
				r.force(newBuffer, newPos)
			}
		}
	}
}

func (r *Repl) deleteChar() {
	if r.searchActive() {
		r.stopSearch()
	} else {
		if r.bufferPos < r.bufferLen() {
			newBuffer := make([]byte, 0)
			newBuffer = append(newBuffer, r.buffer[0:r.bufferPos]...)

			if r.bufferPos < r.bufferLen()-1 {
				newBuffer = append(newBuffer, r.buffer[r.bufferPos+1:]...)
			}

			newPos := r.bufferPos

			r.force(newBuffer, newPos)
		}
	}
}

func (r *Repl) clearToEnd() {
	if r.bufferPos != r.bufferLen() {
		newBuffer := r.buffer[0:r.bufferPos]

		r.prevDel = r.buffer[r.bufferPos:]

		r.force(newBuffer, r.bufferPos)
	}
}

func (r *Repl) clearToStart() {
	if r.bufferPos > 0 {
		newBuffer := r.buffer[r.bufferPos:]

		r.prevDel = r.buffer[0:r.bufferPos]

		r.force(newBuffer, 0)
	}
}

func (r *Repl) phraseStartPositions() []int {
	if len(r.buffer) == 0 {
		return []int{0}
	}

	re := r.phraseRe

	indices := re.FindAllIndex(r.buffer, -1)

	res := make([]int, 0)

	for i, match := range indices {
		start := match[0]
		stop := match[1]
		if i == 0 && start != 0 {
			res = append(res, 0)
		}

		res = append(res, start, stop)

		if i == len(indices)-1 && stop != len(r.buffer) {
			res = append(res, len(r.buffer))
		}
	}

	if len(res) == 0 || res[len(res)-1] != len(r.buffer) {
		res = append(res, len(r.buffer))
	}

	return res
}

func (r *Repl) nextPhrasePos() (int, bool) {
	var res int
	if r.bufferPos == r.bufferLen() {
		res = r.bufferLen()
	} else {
		indices := r.phraseStartPositions()

		for _, idx := range indices {
			if idx > r.bufferPos {
				res = idx
				break
			}
		}
	}

	return res, res != r.bufferPos
}

func (r *Repl) prevPhrasePos() (int, bool) {
	var res int
	if r.bufferPos == 0 {
		res = 0
	} else {
		indices := r.phraseStartPositions()

		for i := len(indices) - 1; i >= 0; i-- {
			idx := indices[i]
			if idx < r.bufferPos {
				res = idx
				break
			}
		}
	}

	return res, res != r.bufferPos
}

func (r *Repl) clearOnePhraseLeft() {
	idx, ok := r.prevPhrasePos()
	if ok {
		newBuffer := append(r.buffer[0:idx], r.buffer[r.bufferPos:]...)

		newPos := idx

		r.prevDel = r.buffer[idx:r.bufferPos]

		_, y0 := r.cursorCoord(-1)
		x1, y1 := r.cursorCoord(newPos)

		if r.bufferPos == r.bufferLen() && y0 == y1 && x1 > 0 && !r.overflow() {
			r.bufferPos = newPos
			r.buffer = newBuffer
			r.syncCursor()
			clearRowAfterCursor()
		} else {
			r.force(newBuffer, newPos)
		}
	}
}

func (r *Repl) clearOnePhraseRight() {
	idx, ok := r.nextPhrasePos()
	if ok {
		newBuffer := make([]byte, 0)
		newBuffer = append(newBuffer, r.buffer[0:r.bufferPos]...)
		newBuffer = append(newBuffer, r.buffer[idx:]...)

		newPos := r.bufferPos

		r.prevDel = r.buffer[r.bufferPos:idx]

		r.force(newBuffer, newPos)
	}
}

func (r *Repl) cleanAndAddToBuffer(msg []byte) {
	// remove bad chars
	// XXX: what about unicode?
	filtered := make([]byte, 0)

	for _, c := range msg {
		if c == '\t' {
			filtered = append(filtered, ' ')
		} else if c >= 32 && c < 127 {
			filtered = append(filtered, c)
		}
	}

	r.addBytesToBuffer(filtered)
}

func (r *Repl) insertPrevDel() {
	r.addBytesToBuffer(r.prevDel)
}

func (r *Repl) updatePromptRow(row int) {
	if row >= r.getHeight() {
		row = r.getHeight() - 1
	} else if row < 0 {
		row = 0
	}

	r.promptRow = row

	r.log("prompt row %d/%d\n", r.promptRow, r.innerHeight()-1)
}

func (r *Repl) writeByte(b byte) {
	if b == '\n' {
		r.newLine()
	} else {
		// should be a printable character
		fmt.Fprintf(os.Stdout, "%c", b)
	}
}

func (r *Repl) newLine() {
	fmt.Fprintf(os.Stdout, "\n\r")

	// every newLine means the status line is pushed below
}

func (r *Repl) CwdStatusWidget() string {
	cwd, err := os.Getwd()
	if err != nil {
		cwd = ""
	}

	return cwd
}

func (r *Repl) VisStatusWidget() string {
	vis := "All"

	if r.viewEnd < 0 {
		r.viewEnd = r.bufferLen()
	}

	if r.viewEnd < r.bufferLen() && r.viewStart == 0 {
		vis = "Start"
	} else if r.viewEnd == r.bufferLen() && r.viewStart > 0 {
		vis = "End"
	} else if r.viewEnd < r.bufferLen() && r.viewStart > 0 {
		vis = fmt.Sprintf("%d", int(float64(r.bufferPos)/float64(r.bufferLen())*100)) + "%"
	}

	return vis
}

// one left aligned and one right aligned
func (r *Repl) statusFields() (string, string) {
	var leftWidget, rightWidget string
	if r.StatusWidgets == nil || r.StatusWidgets.Left == nil {
		leftWidget = r.CwdStatusWidget()
	} else {
		leftWidget = r.StatusWidgets.Left(r)
	}

	if r.StatusWidgets == nil || r.StatusWidgets.Right == nil {
		rightWidget = r.VisStatusWidget()
	} else {
		rightWidget = r.StatusWidgets.Right(r)
	}

	return leftWidget, rightWidget
}

func (r *Repl) statusVisible() bool {
	if r.getWidth() < 10 {
		return false
	} else {
		return true
	}
}

func (r *Repl) clearStatus() {
	if r.statusVisible() {
		moveCursorTo(0, r.getHeight()-1)

		clearRow()

		r.syncCursor()
	}
}

func (r *Repl) filterStatus() string {
	tot := 0
	cur := -1
	for i := len(r.history) - 1; i >= 0; i-- {
		entry := r.history[i]
		if r.filterMatches(entry) {
			if i == r.historyIdx {
				cur = tot
			}

			tot += 1
		}
	}

	if tot == 0 {
		return "No matches"
	} else if cur != -1 {
		return fmt.Sprintf("%d/%d matches", cur+1, tot)
	} else {
		panic("unexpected")
	}
}

func (r *Repl) writeStatus() {
	if !r.statusVisible() {
		r.syncCursor()
		return
	}

	r.boundPromptRow()

	moveCursorTo(0, r.getHeight()-1)

	w := r.getWidth()
	if r.searchActive() {
		pref := "Reverse-search: "
		fmt.Print(pref)
		fmt.Print(string(r.filter)) // cursor stays here

		// print some status about the matches
		if len(r.filter) > 0 && w > len(r.filter)+len(pref)+10 {
			info := r.filterStatus()

			for i := 0; i < w-len(info)-len(pref)-len(r.filter); i++ {
				fmt.Print(" ")
			}

			fmt.Print(info)

			moveToCol(len(pref) + len(r.filter))
		}
	} else {
		left, right := r.statusFields()
		if len(left) > w-len(right) {
			left = left[0 : w-len(right)]
		}

		// Start highlighting
		highlight()
		fmt.Print(left)

		// Re-highlight in case a custom status widget blew up the colors
		highlight()
		for i := 0; i < w-len(left)-len(right); i++ {
			fmt.Print(" ")
		}

		// Re-highlight in case a custom status widget blew up the colors
		highlight()
		fmt.Print(right)

		// end highlighting
		resetDecorations()

		r.syncCursor()
	}
}

// use a simple match criterium now, could be improved
func (r *Repl) filterMatches(bs []byte) bool {
	return strings.Contains(string(bs), string(r.filter))
}

func (r *Repl) updateSearchResult() {
	if r.filter == nil || len(r.history) == 0 || len(r.filter) == 0 {
		return
	}

	// prefer currently selected entry
	if r.historyIdx != -1 {
		if r.filterMatches(r.buffer) {
			return
		}
	}

	for i := len(r.history) - 1; i >= 0; i-- {
		if r.filterMatches(r.history[i]) {
			r.useHistoryEntry(i)
			return
		}
	}
}

///////////////////
// exported methods
///////////////////

// Start the REPL loop.
//
// Loop sets the terminal to raw mode, so any further calls to fmt.Print or similar, might not behave as expected and can garble your REPL.
func (r *Repl) Loop() error {
	// the terminal needs to be in raw mode, so we can intercept the control sequences
	// (the default canonical mode isn't good enough for repl's)
	if err := r.MakeRaw(); err != nil {
		return err
	}

	r.reader.start()

	r.notifySizeChange()

	r.printPrompt()

	queryCursorPos() // get initial prompt position

	// loop forever
	for {
		r.reader.read()

		bts := <-r.reader.bytes

		r.dispatch(bts)
	}

	return nil
}

// Exit the REPL program cleanly. Performs the following steps:
//  1. cleans the screen
//  2. returns the cursor to the appropriate position
//  3. unsets terminal raw mode
//
// Important: use this method instead of os.Exit.
func (r *Repl) Quit() {
	r.quit()
}

// Unset the raw mode in case you want to run a curses-like command inside your REPL session (e.g. vi or top). Remember to call MakeRaw after the command finishes.
func (r *Repl) UnmakeRaw() {
	r.onEnd()

	r.onEnd = nil
}

// Explicitely set the terminal back to raw mode after a call to UnmakeRaw.
func (r *Repl) MakeRaw() error {
	// we need the term package as a platform independent way of setting the connected terminal emulator to raw mode
	fd := int(os.Stdin.Fd())

	oldState, err := term.MakeRaw(fd)
	if err != nil {
		return err
	}

	r.onEnd = func() {
		term.Restore(fd, oldState)
	}

	return nil
}

func (r *Repl) ReadLine(echo bool) string {
	buffer := make([]byte, 0)

	for {
		r.reader.read()

		bts := <-r.reader.bytes

		// a mini version of dispatch
		if len(bts) == 1 && bts[0] == 13 {
			if echo {
				fmt.Print("\n\r")
			}
			break
		} else {
			for _, b := range bts {
				if b == 27 {
					break
				} else if b >= 32 {
					if echo {
						fmt.Print(string([]byte{b}))
					}

					buffer = append(buffer, b)
				}
			}
		}
	}

	return string(buffer)
}
