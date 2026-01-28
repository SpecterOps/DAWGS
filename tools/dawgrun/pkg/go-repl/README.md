# go-repl

## NOTICE

Forked from https://github.com/OpenEngineer/go-repl @ 9990dd87c3fb9c039d3c2d1f00ceace8906b6644.
Changes applied:
- Added `repl.Options` to manage internal settings knobs
- Added the ability to render custom status bar components
- Support for OS-level paste into input line
- Added history load/save to file

## Original README

Lightweight Golang REPL library, inspired by GNU Readline. You provide the `Eval` function, and `go-repl` does the rest.

Your REPLs that use this library will enjoy the following features:
* Session history with *reverse-search*
  * Ctrl-R to start *reverse-search*
  * Most edit commands, except the most basic ones, exit the *reverse-search* mode
  * Use Up/Down to cycle through a filtered list of history entries
* The input buffer is redrawn when a resize is detected
* Status bar at bottom with current working dir and other info
* Truncation of very long inputs (status bar displays info about cursor position)
* Common edit and movement commands:
   * Right/Left: move cursor one character at a time
   * Ctrl-Right/Left: move cursor one word at a time
   * Up/Down: cycle through history
   * Backspace/Delete: works as expected
   * Shift-Enter: insert newline into buffer without invoking `Eval`
   * Ctrl-A or Home: move to start of buffer
   * Ctrl-E or End: move to end of buffer
   * Ctrl-W: delete preceding word
   * Ctrl-Q: delete following word
   * Ctrl-C or Esc: ignore current input and reset buffer
   * Ctrl-D: quit REPL
   * Ctrl-U: clear buffer to start
   * Ctrl-K: clear buffer to end
   * Ctrl-L: reset prompt at top and redraw buffer
   * Ctrl-Y: insert previous deletion (from Ctrl-K, Ctrl-U, Ctrl-Q or Ctrl-W)

Notes: 
* Doesn't depend on *ncurses*
* Performance hasn't yet been optimized and I haven't yet tested all corner cases exhaustively
* Might not work in Windows command prompt (keystroke codes could differ, ANSI escape sequences might not be supported, the method that sets terminal to raw mode might not work)
* No vi edit mode
* No support for clipboards yet

# Usage

Fetch this library with the following command:
```shell
$ go get -u github.com/openengineer/go-repl
```

In order to create your own REPL you have to define a type that implements the `Handler` interface:
```golang
type Handler interface {
  Prompt() string
  Tab(buffer string) string
  Eval(line string) string
}
```

Here is a complete example (can also be found in `./examples/basic_repl.go`):

```golang
package main

import (
  "fmt"
  "log"
  "strconv"
  "strings"

  repl "github.com/openengineer/go-repl"
)

var helpMessage = `help              display this message
add <int> <int>   add two numbers
quit              quit this program`

// implements repl.Handler interface
type MyHandler struct {
  r *repl.Repl
}

func main() {
  fmt.Println("Welcome, type \"help\" for more info")

  h := &MyHandler{}
  h.r = repl.NewRepl(h)

  // start the terminal loop
  if err := h.r.Loop(); err != nil {
    log.Fatal(err)
  }
}

func (h *MyHandler) Prompt() string {
  return "> " 
}

func (h *MyHandler) Tab(buffer string) string {
  return "" // do nothing
}

func (h *MyHandler) Eval(line string) string {
  fields := strings.Fields(line)

  if len(fields) == 0 {
    return ""
  } else {
    cmd, args := fields[0], fields[1:]

    switch cmd {
    case "help":
      return helpMessage
    case "add":
      if len(args) != 2 {
        return "\"add\" expects 2 args"
      } else {
        return add(args[0], args[1])
      }
    case "quit":
      h.r.Quit()
      return ""
    default:
      return fmt.Sprintf("unrecognized command \"%s\"", cmd)
    }
  }
}

func add(a_ string, b_ string) string {
  a, err := strconv.Atoi(a_)
  if err != nil {
    return "first arg is not an integer"
  }

  b, err := strconv.Atoi(b_)
  if err != nil {
    return "second arg is not an integer"
  }

  return strconv.Itoa(a + b)
}
```
