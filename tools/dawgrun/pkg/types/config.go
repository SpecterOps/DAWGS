package types

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const DefaultConfigFileName = "config.json"

type Config struct {
	Connections map[string]ConnectionConfig `json:"connections"`
}

type ConnectionConfig struct {
	Driver           string `json:"driver,omitempty"`
	ConnectionString string `json:"connection_string"`
}

func LoadConfig(configPath string) (Config, error) {
	contents, err := os.ReadFile(configPath)
	if err != nil {
		return Config{}, fmt.Errorf("could not read config file %s: %w", configPath, err)
	}

	config := Config{}
	if err := json.Unmarshal(contents, &config); err != nil {
		return Config{}, fmt.Errorf("could not parse config file %s: %w", configPath, err)
	}

	if config.Connections == nil {
		config.Connections = make(map[string]ConnectionConfig)
	}

	return config, nil
}

func (config Config) Save(configPath string) error {
	if config.Connections == nil {
		config.Connections = make(map[string]ConnectionConfig)
	}

	contents, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("could not marshal config: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(configPath), 0o750); err != nil {
		return fmt.Errorf("could not create config directory for %s: %w", configPath, err)
	}

	contents = append(contents, '\n')
	if err := os.WriteFile(configPath, contents, 0o600); err != nil {
		return fmt.Errorf("could not write config file %s: %w", configPath, err)
	}

	return nil
}

func (s *ConnectionConfig) UnmarshalJSON(contents []byte) error {
	var legacyConnectionString string
	if err := json.Unmarshal(contents, &legacyConnectionString); err == nil {
		s.ConnectionString = legacyConnectionString
		s.Driver = ""
		return nil
	}

	type connectionConfig ConnectionConfig
	var config connectionConfig
	if err := json.Unmarshal(contents, &config); err != nil {
		return err
	}

	s.ConnectionString = config.ConnectionString
	s.Driver = strings.ToLower(strings.TrimSpace(config.Driver))
	return nil
}

func DefaultConfigPath(appConfigBaseDir string) string {
	return filepath.Join(appConfigBaseDir, DefaultConfigFileName)
}
