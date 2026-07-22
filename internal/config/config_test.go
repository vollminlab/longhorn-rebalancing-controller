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
