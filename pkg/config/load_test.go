package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadTime(t *testing.T) {
	os.Setenv("POLL_DIRTY_SUCCESS_TIMEOUT", "")

	cfg, err := Load()
	if err != nil {
		t.Errorf("failed to load: %s", err)
	}

	if cfg.PollDirty.SuccessTimeout != DefaultDuration(time.Second) {
		t.Errorf("expected 1s, got: %d", cfg.PollDirty.SuccessTimeout)
	}
}

func TestLoadInterval(t *testing.T) {
	os.Setenv("DOTMESH_UPGRADES_INTERVAL_SECONDS", "")

	cfg, err := Load()
	if err != nil {
		t.Errorf("failed to load: %s", err)
	}

	if int(cfg.Upgrades.IntervalSeconds) != 0 {
		t.Errorf("expected 0, got: %d", int(cfg.Upgrades.IntervalSeconds))
	}
}
