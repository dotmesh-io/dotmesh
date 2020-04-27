package config

import "time"

type (
	// Config stores the configuration settings.
	Config struct {
		PollDirty struct {
			// Disable filesystem polling, used by hub
			Disabled       bool          `default:"false" envconfig:"DISABLE_DIRTY_POLLING"`
			SuccessTimeout time.Duration `default:"1s" envconfig:"POLL_DIRTY_SUCCESS_TIMEOUT"`
			ErrorTimeout   time.Duration `default:"1s" envconfig:"POLL_DIRTY_ERROR_TIMEOUT"`
		}
	}
)
