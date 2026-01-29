package graph

type Literal struct {
	Value any    `json:"value"`
	Key   string `json:"key"`
}

type Literals []Literal
