package jsonl

type State int

const (
	Open State = iota
	Failed
	Closed
)
