package config

import (
	"strconv"
	"time"
)

type (
	// Config stores the configuration settings.
	Config struct {

		// Disable filesystem polling, used by hub
		DisableDirtyPolling DefaultBool `default:"false" envconfig:"DISABLE_DIRTY_POLLING"`
		DisableFlexVolume   DefaultBool `default:"false" envconfig:"DISABLE_FLEXVOLUME"`

		DotmeshUpgradesURL string

		PollDirty struct {
			SuccessTimeout DefaultDuration `default:"1s" envconfig:"POLL_DIRTY_SUCCESS_TIMEOUT"`
			ErrorTimeout   DefaultDuration `default:"1s" envconfig:"POLL_DIRTY_ERROR_TIMEOUT"`
		}

		Upgrades struct {
			URL             string `envconfig:"DOTMESH_UPGRADES_URL"`
			IntervalSeconds int    `default:"300" envconfig:"DOTMESH_UPGRADES_INTERVAL_SECONDS"`
		}
	}
)

type DefaultBool bool

func (b *DefaultBool) Decode(value string) error {
	if value == "" {
		*b = false
		return nil
	}
	val, err := strconv.ParseBool(value)
	if err != nil {
		return err
	}

	*b = DefaultBool(val)
	return nil
}

type DefaultDuration time.Duration

func (b *DefaultDuration) Decode(value string) error {
	if value == "" {
		*b = DefaultDuration(time.Second)
		return nil
	}
	dur, err := time.ParseDuration(value)
	if err != nil {
		return err
	}
	*b = DefaultDuration(dur)
	return nil
}

func (b *DefaultDuration) Duration() time.Duration {
	return time.Duration(*b)
}
