package main

import (
	"testing"

	"github.com/specterops/dawgs/retriever"
)

func TestParseWorkerList(t *testing.T) {
	workers, err := parseWorkerList("1,2,4,2")
	if err != nil {
		t.Fatalf("parse worker list: %v", err)
	}

	if got, want := len(workers), 3; got != want {
		t.Fatalf("worker count length = %d, want %d", got, want)
	}

	if workers[0] != 1 || workers[1] != 2 || workers[2] != 4 {
		t.Fatalf("unexpected workers: %v", workers)
	}

	if _, err := parseWorkerList("0"); err == nil {
		t.Fatalf("expected invalid worker count error")
	}
}

func TestFlagListTypes(t *testing.T) {
	var graphs stringList
	if err := graphs.Set(" default "); err != nil {
		t.Fatalf("set graph: %v", err)
	}

	if err := graphs.Set(""); err == nil {
		t.Fatalf("expected empty graph error")
	}

	if graphs.String() != "default" {
		t.Fatalf("graph list string = %q", graphs.String())
	}

	var workers workerList

	if err := workers.Set("2,4"); err != nil {
		t.Fatalf("set workers: %v", err)
	}

	if workers.String() != "2,4" {
		t.Fatalf("worker list string = %q", workers.String())
	}
}

func TestWorkerListAppendsRepeatedFlags(t *testing.T) {
	var workers workerList

	if err := workers.Set("1,2"); err != nil {
		t.Fatalf("set initial workers: %v", err)
	}

	if err := workers.Set("2,4"); err != nil {
		t.Fatalf("set repeated workers: %v", err)
	}

	if workers.String() != "1,2,4" {
		t.Fatalf("worker list string = %q", workers.String())
	}

	if err := workers.Set("bad"); err == nil {
		t.Fatalf("expected invalid worker count")
	}

	if workers.String() != "1,2,4" {
		t.Fatalf("invalid worker update changed list to %q", workers.String())
	}
}

func TestBenchOptionsValidate(t *testing.T) {
	bench := benchOptions{
		Workers:    []int{1},
		BatchSize:  1,
		SampleSize: 1,
		ZstdLevel:  retriever.DefaultZstdLevel,
	}
	if err := bench.validate(); err != nil {
		t.Fatalf("valid bench options: %v", err)
	}

	bench.Workers = nil

	if err := bench.validate(); err == nil {
		t.Fatalf("expected missing workers")
	}

	bench.Workers = []int{2}

	if err := bench.validate(); err != nil {
		t.Fatalf("valid parallel bench workers: %v", err)
	}

	bench.Workers = []int{0}

	if err := bench.validate(); err == nil {
		t.Fatalf("expected invalid worker count")
	}

	bench.Workers = []int{1}
	bench.SampleSize = -1

	if err := bench.validate(); err == nil {
		t.Fatalf("expected invalid sample size")
	}
}
