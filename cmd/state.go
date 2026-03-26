package cmd

import (
	"encoding/json"
	"os"
	"time"

	"github.com/rothgar/k3s-to-talos/internal/k3s"
)

// MigrationState tracks progress across phases so runs can be resumed.
type MigrationState struct {
	Host           string          `json:"host"`
	BackupDir      string          `json:"backup_dir"`
	ClusterName    string          `json:"cluster_name"`
	TalosVersion   string          `json:"talos_version"`
	TalosConfigDir string          `json:"talos_config_dir"`
	KubeconfigPath string          `json:"kubeconfig_path"`
	ClusterInfo    *k3s.ClusterInfo `json:"cluster_info,omitempty"`
	Phases         map[string]bool `json:"phases"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

func loadOrInitState(path, host string) (*MigrationState, error) {
	if data, err := os.ReadFile(path); err == nil {
		var s MigrationState
		if err := json.Unmarshal(data, &s); err == nil && s.Host == host {
			return &s, nil
		}
	}
	return &MigrationState{
		Host:      host,
		BackupDir: flagBackupDir,
		Phases:    make(map[string]bool),
	}, nil
}

func (s *MigrationState) PhaseCompleted(phase string) bool {
	return s.Phases[phase]
}

func (s *MigrationState) MarkPhaseComplete(phase string) {
	if s.Phases == nil {
		s.Phases = make(map[string]bool)
	}
	s.Phases[phase] = true
	s.UpdatedAt = time.Now()
}

func (s *MigrationState) Save(path string) error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0600)
}
