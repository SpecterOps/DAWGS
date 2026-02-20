package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	repl "github.com/openengineer/go-repl"
)

type ShellWrapper struct {
	r *repl.Repl
}

func (h *ShellWrapper) Prompt() string {
	return "> "
}

func (h *ShellWrapper) Tab(buffer string) string {
	// a tab is simply 2 spaces here
	return "  "
}

func (h *ShellWrapper) Eval(buffer string) string {
	// upon eval the Stdin should be unblocked
	if strings.TrimSpace(buffer) != "" {
		endCmd := make(chan bool)

		if buffer == "quit" {
			h.r.Quit()
			return ""
		}

		go func() {
			h.r.UnmakeRaw()
			defer h.r.MakeRaw()

			fields := strings.Fields(buffer)

			cmdName, args := fields[0], fields[1:]

			cmd := exec.Command(cmdName, args...)

			cmd.Stdout = os.Stdout
			cmd.Stdin = os.Stdin
			cmd.Stderr = os.Stderr

			err := cmd.Start()
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())

				endCmd <- false
			} else {
				cmd.Wait()
				endCmd <- true
			}
		}()

		ok := <-endCmd

		if ok {
			return ""
		} else {
			return ""
		}
	} else {
		return ""
	}
}

func main() {
	fmt.Println("Try \"vi\" or \"top\" and see what happens...")

	h := &ShellWrapper{}
	h.r = repl.NewRepl(h)

	if err := h.r.Loop(); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
	}
}
