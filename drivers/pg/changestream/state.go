package changestream

import (
	"log/slog"
	"sync"
	"time"
)

type StateManager interface {
	IsEnabled() bool
	EnableDaemon()
	DisableDaemon()
}

type stateManager struct {
	lastTablePartitionAssertion time.Time
	lastFeatureFlagCheck        time.Time
	featureFlagCheckInterval    time.Duration
	stateLock                   *sync.RWMutex
	// todo: how to pass this bloodhound type into dawgs?
	// flags                       appcfg.GetFlagByKeyer
	enabled bool
}

// todo: initial implementation took { flags appcfg.GetFlagByKeyer } param
func newStateManager() *stateManager {
	return &stateManager{
		featureFlagCheckInterval: time.Minute,
		stateLock:                &sync.RWMutex{},
		// flags:                    flags,
		enabled: false,
	}
}

func (s *stateManager) IsEnabled() bool {
	s.stateLock.RLock()
	defer s.stateLock.RUnlock()

	return s.enabled
}

func (s *stateManager) EnableDaemon() {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	// Don't spam the log and allow the function to be reentrant
	if !s.enabled {
		slog.Info("enabling changelog")
		s.enabled = true
	}
}

func (s *stateManager) DisableDaemon() {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	// Don't spam the log and allow the function to be reentrant
	if s.enabled {
		slog.Info("disabling changelog")
		s.enabled = false
	}
}

// todo: the following is for testing but yuck
type MockStateManager struct {
	enabled bool
}

func NewMockStateManager() *MockStateManager {
	return &MockStateManager{enabled: true} // default to enabled
}

func (m *MockStateManager) EnableDaemon() {
	m.enabled = true
}

func (m *MockStateManager) DisableDaemon() {
	m.enabled = false
}

func (m *MockStateManager) IsEnabled() bool {
	return m.enabled
}
