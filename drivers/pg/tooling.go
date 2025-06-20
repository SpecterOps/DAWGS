package pg

import (
	"regexp"
	"sync"

	"github.com/specterops/dawgs/drivers"
)

type IterationOptions interface {
	Once()
}

type QueryHookOptions interface {
	Trace() IterationOptions
}

type QueryHook interface {
	OnStatementMatch(statement string) QueryHookOptions
	OnStatementRegex(re *regexp.Regexp) QueryHookOptions
}

type actionType int

const (
	actionTrace actionType = iota
)

type queryHook struct {
	statementMatch   *string
	statementRegex   *regexp.Regexp
	action           actionType
	actionIterations int
}

func (s *queryHook) Execute(query string, arguments ...any) {
	switch s.action {
	case actionTrace:
	}
}

func (s *queryHook) Catches(query string, arguments ...any) bool {
	if s.statementMatch != nil {
		if query == *s.statementMatch {
			return true
		}
	}

	if s.statementRegex != nil {
		if s.statementRegex.MatchString(query) {
			return true
		}
	}

	return false
}

func (s *queryHook) Once() {
	s.actionIterations = 1
}

func (s *queryHook) Times(actionIterations int) {
	s.actionIterations = actionIterations
}

func (s *queryHook) Trace() IterationOptions {
	s.action = actionTrace
	return s
}

func (s *queryHook) OnStatementMatch(statement string) QueryHookOptions {
	s.statementMatch = &statement
	return s
}

func (s *queryHook) OnStatementRegex(re *regexp.Regexp) QueryHookOptions {
	s.statementRegex = re
	return s
}

type QueryPathInspector interface {
	Hook() QueryHook
}

type queryPathInspector struct {
	hooks []*queryHook
	lock  *sync.RWMutex
}

func (s *queryPathInspector) Inspect(query string, arguments ...any) {
	if !drivers.IsQueryAnalysisEnabled() {
		return
	}

	s.lock.RLock()
	defer s.lock.RUnlock()

	for _, hook := range s.hooks {
		if hook.Catches(query, arguments) {
			hook.Execute(query, arguments)
		}
	}
}

func (s *queryPathInspector) Hook() QueryHook {
	s.lock.Lock()
	defer s.lock.Unlock()

	hook := &queryHook{}
	s.hooks = append(s.hooks, hook)

	return hook
}

var inspectorInst = &queryPathInspector{
	lock: &sync.RWMutex{},
}

func inspector() *queryPathInspector {
	return inspectorInst
}

func Inspector() QueryPathInspector {
	return inspectorInst
}
