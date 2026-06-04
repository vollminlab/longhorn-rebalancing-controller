package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"sigs.k8s.io/yaml"
)

type Config struct {
	DryRun      bool             `json:"dryRun"`
	Rebalance   RebalanceConfig  `json:"rebalance"`
	SteadyState SteadyStateConfig `json:"steadyState"`
}

type RebalanceConfig struct {
	NodeUsageThreshold  float64 `json:"nodeUsageThreshold"`
	MaxEvictionsPerDay  int     `json:"maxEvictionsPerDay"`
	CooldownMinutes     int     `json:"cooldownMinutes"`
	MaintenanceWindow   string  `json:"maintenanceWindow"`
	SmallestFirst       bool    `json:"smallestFirst"`
	GraduateAfterCycles int     `json:"graduateAfterCycles"`
}

type SteadyStateConfig struct {
	ImbalanceRatio     float64 `json:"imbalanceRatio"`
	MaxEvictionsPerDay int     `json:"maxEvictionsPerDay"`
	CooldownMinutes    int     `json:"cooldownMinutes"`
}

// MaintenanceWindow holds parsed start/end durations from midnight.
type MaintenanceWindow struct {
	Start time.Duration
	End   time.Duration
}

// Contains reports whether t falls within the maintenance window.
func (w MaintenanceWindow) Contains(t time.Time) bool {
	midnight := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	since := t.Sub(midnight)
	if w.Start <= w.End {
		return since >= w.Start && since < w.End
	}
	// Overnight window (e.g. 23:00-02:00)
	return since >= w.Start || since < w.End
}

func Default() *Config {
	return &Config{
		DryRun: true,
		Rebalance: RebalanceConfig{
			NodeUsageThreshold:  75.0,
			MaxEvictionsPerDay:  2,
			CooldownMinutes:     30,
			MaintenanceWindow:   "02:00-05:00",
			SmallestFirst:       true,
			GraduateAfterCycles: 3,
		},
		SteadyState: SteadyStateConfig{
			ImbalanceRatio:     1.5,
			MaxEvictionsPerDay: 5,
			CooldownMinutes:    10,
		},
	}
}

// ParseYAML parses a YAML-encoded config and returns a validated Config.
func ParseYAML(data []byte) (*Config, error) {
	cfg := Default()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	if _, err := ParseWindow(cfg.Rebalance.MaintenanceWindow); err != nil {
		return nil, fmt.Errorf("maintenanceWindow: %w", err)
	}
	return cfg, nil
}

// ParseWindow parses a "HH:MM-HH:MM" window string.
func ParseWindow(s string) (MaintenanceWindow, error) {
	parts := strings.SplitN(s, "-", 2)
	if len(parts) != 2 {
		return MaintenanceWindow{}, fmt.Errorf("expected HH:MM-HH:MM, got %q", s)
	}
	start, err := parseHHMM(parts[0])
	if err != nil {
		return MaintenanceWindow{}, fmt.Errorf("start: %w", err)
	}
	end, err := parseHHMM(parts[1])
	if err != nil {
		return MaintenanceWindow{}, fmt.Errorf("end: %w", err)
	}
	return MaintenanceWindow{Start: start, End: end}, nil
}

func parseHHMM(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return 0, fmt.Errorf("expected HH:MM, got %q", s)
	}
	h, err := strconv.Atoi(parts[0])
	if err != nil || h < 0 || h > 23 {
		return 0, fmt.Errorf("invalid hour in %q", s)
	}
	m, err := strconv.Atoi(parts[1])
	if err != nil || m < 0 || m > 59 {
		return 0, fmt.Errorf("invalid minute in %q", s)
	}
	return time.Duration(h)*time.Hour + time.Duration(m)*time.Minute, nil
}
