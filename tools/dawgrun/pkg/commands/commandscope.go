package commands

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/tools/dawgrun/pkg/stubs"
	"github.com/specterops/dawgs/tools/dawgrun/pkg/types"
)

type (
	// Scope holds persistent command state that can be shared across invocations.
	Scope struct {
		mu                sync.RWMutex
		connections       map[string]graph.Database
		connectionConfigs map[string]types.ConnectionConfig
		connKindMaps      map[string]stubs.KindMap
		openDatabase      databaseOpener
	}

	databaseOpener        func(context.Context, string, openConnectionOptions) (graph.Database, string, error)
	openConnectionOptions struct {
		driverName       string
		defaultGraphName string
		initGraphOnFail  bool
		quiet            bool
	}
)

// NewScope creates an empty shared scope for command state.
func NewScope() *Scope {
	return &Scope{
		connections:       make(map[string]graph.Database),
		connectionConfigs: make(map[string]types.ConnectionConfig),
		connKindMaps:      make(map[string]stubs.KindMap),
		openDatabase:      openDAWGSDatabase,
	}
}

// GetNumConnections returns the number of tracked database connections.
func (s *Scope) GetNumConnections() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.connections)
}

// GetConnectionNames returns sorted names for tracked database connections.
func (s *Scope) GetConnectionNames() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return slices.Sorted(maps.Keys(s.connections))
}

// GetUnopenedConnectionNames returns sorted names for configured connections that do not have open handles yet.
func (s *Scope) GetUnopenedConnectionNames() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	unopenedConnections := make(map[string]struct{})
	for connName := range s.connectionConfigs {
		if _, isOpen := s.connections[connName]; !isOpen {
			unopenedConnections[connName] = struct{}{}
		}
	}

	return slices.Sorted(maps.Keys(unopenedConnections))
}

// AddConnection stores or replaces a named database connection in scope.
func (s *Scope) AddConnection(name string, querier graph.Database, config types.ConnectionConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.connections[name] = querier
	s.connectionConfigs[name] = config
}

// DropConnection removes a named connection and any cached kind map.
func (s *Scope) DropConnection(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.connections, name)
	delete(s.connectionConfigs, name)
	delete(s.connKindMaps, name)
}

func (s *Scope) GetConnection(name string) (graph.Database, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	conn, ok := s.connections[name]
	return conn, ok
}

func (s *Scope) GetConnectionString(name string) (string, bool) {
	config, ok := s.GetConnectionConfig(name)
	return config.ConnectionString, ok
}

func (s *Scope) GetConnectionConfig(name string) (types.ConnectionConfig, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	config, ok := s.connectionConfigs[name]
	return config, ok
}

func (s *Scope) SetConnectionStrings(connectionStrings map[string]string) {
	connectionConfigs := make(map[string]types.ConnectionConfig, len(connectionStrings))
	for name, connStr := range connectionStrings {
		connectionConfigs[name] = types.ConnectionConfig{ConnectionString: connStr}
	}

	s.SetConnectionConfigs(connectionConfigs)
}

func (s *Scope) SetConnectionConfigs(connectionConfigs map[string]types.ConnectionConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for name, config := range connectionConfigs {
		s.connectionConfigs[name] = config
	}
}

func (s *Scope) GetOpenConnectionStrings() map[string]string {
	connectionConfigs := s.GetOpenConnectionConfigs()
	connectionStrings := make(map[string]string, len(connectionConfigs))
	for name, config := range connectionConfigs {
		connectionStrings[name] = config.ConnectionString
	}

	return connectionStrings
}

func (s *Scope) GetOpenConnectionConfigs() map[string]types.ConnectionConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()

	connectionConfigs := make(map[string]types.ConnectionConfig, len(s.connections))
	for name := range s.connections {
		if config, ok := s.connectionConfigs[name]; ok {
			connectionConfigs[name] = config
		}
	}

	return connectionConfigs
}

func (s *Scope) CloseConnections(ctx context.Context) error {
	s.mu.Lock()
	connections := s.connections
	s.connections = make(map[string]graph.Database)
	s.connKindMaps = make(map[string]stubs.KindMap)
	s.mu.Unlock()

	var closeErrs []string
	for _, connName := range slices.Sorted(maps.Keys(connections)) {
		if err := connections[connName].Close(ctx); err != nil {
			closeErrs = append(closeErrs, fmt.Sprintf("%s: %s", connName, err.Error()))
		}
	}

	if len(closeErrs) > 0 {
		return fmt.Errorf("could not close connection(s): %s", strings.Join(closeErrs, "; "))
	}

	return nil
}
