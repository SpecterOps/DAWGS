package repl

// Implement this interface in order to use `Repl` with your custom logic.
type Handler interface {
	Prompt() string
	Eval(buffer string) string
	Tab(buffer string) string
}

// Completion is the optional richer tab-completion response for Completer.
type Completion struct {
	// Insert is appended to the buffer at the active cursor position.
	Insert string
	// Candidates are rendered as completion choices.
	Candidates []string
	// Message is rendered above Candidates when set.
	Message string
}

// Completer can be optionally implemented by a Handler to provide richer tab
// completion behavior (for example: candidate lists), while keeping Handler.Tab
// backwards-compatible.
type Completer interface {
	Complete(buffer string) Completion
}
