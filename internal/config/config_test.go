package config

import "testing"

func TestParseYAML_MinDestinationFreePctDefault(t *testing.T) {
	cfg, err := ParseYAML([]byte("dryRun: false\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MinDestinationFreePct != 25.0 {
		t.Fatalf("default minDestinationFreePct = %.1f, want 25.0", cfg.MinDestinationFreePct)
	}
}

func TestParseYAML_MoveDefaults(t *testing.T) {
	cfg, err := ParseYAML([]byte("dryRun: false\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Move.TimeoutMinutes != 90 {
		t.Fatalf("default move.timeoutMinutes = %d, want 90", cfg.Move.TimeoutMinutes)
	}
	if cfg.Move.MaxFailuresPerDay != 3 {
		t.Fatalf("default move.maxFailuresPerDay = %d, want 3", cfg.Move.MaxFailuresPerDay)
	}
}

func TestParseYAML_MoveOverrideAndValidation(t *testing.T) {
	cfg, err := ParseYAML([]byte("move:\n  timeoutMinutes: 120\n  maxFailuresPerDay: 1\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Move.TimeoutMinutes != 120 {
		t.Fatalf("move.timeoutMinutes = %d, want 120", cfg.Move.TimeoutMinutes)
	}
	if cfg.Move.MaxFailuresPerDay != 1 {
		t.Fatalf("move.maxFailuresPerDay = %d, want 1", cfg.Move.MaxFailuresPerDay)
	}

	if _, err := ParseYAML([]byte("move:\n  timeoutMinutes: 0\n")); err == nil {
		t.Fatal("expected validation error for move.timeoutMinutes=0")
	}
	if _, err := ParseYAML([]byte("move:\n  maxFailuresPerDay: -1\n")); err == nil {
		t.Fatal("expected validation error for move.maxFailuresPerDay=-1")
	}
}

func TestParseYAML_MinDestinationFreePctOverrideAndValidation(t *testing.T) {
	cfg, err := ParseYAML([]byte("minDestinationFreePct: 30\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MinDestinationFreePct != 30.0 {
		t.Fatalf("minDestinationFreePct = %.1f, want 30.0", cfg.MinDestinationFreePct)
	}

	if _, err := ParseYAML([]byte("minDestinationFreePct: 100\n")); err == nil {
		t.Fatal("expected validation error for minDestinationFreePct=100")
	}
	if _, err := ParseYAML([]byte("minDestinationFreePct: -1\n")); err == nil {
		t.Fatal("expected validation error for minDestinationFreePct=-1")
	}
}
