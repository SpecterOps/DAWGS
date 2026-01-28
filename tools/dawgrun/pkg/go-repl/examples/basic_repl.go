package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	repl "github.com/openengineer/go-repl"
)

var helpMessage = `help              display this message
add <int> <int>   add two numbers
sleep             sleep for 5s
read              read some user input
quit              quit this program`

// implements repl.Handler interface
type MyHandler struct {
	r *repl.Repl
}

func main() {
	fmt.Println("Welcome, type \"help\" for more info")

	h := &MyHandler{}
	h.r = repl.NewRepl(h)

	if err := h.r.Loop(); err != nil {
		log.Fatal(err)
	}
}

func (h *MyHandler) Prompt() string {
	return "> "
}

func (h *MyHandler) Tab(buffer string) string {
	return ""
}

// first return value is for stdout, second return value is for history
func (h *MyHandler) Eval(buffer string) string {
	fields := strings.Fields(buffer)

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
		case "sleep":
			time.Sleep(5 * time.Second)
			return ""
		case "read":
			info := h.r.ReadLine(true)
			return "read=" + info
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
