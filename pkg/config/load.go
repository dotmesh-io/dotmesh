package config

import (
	"time"

	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

// Load loads the configuration from the environment.
func Load() (Config, error) {

	err := godotenv.Load()
	if err != nil {
		// not a problem
	}

	config := Config{}
	err = envconfig.Process("", &config)
	if err != nil {
		return config, err
	}
	if config.PollDirty.SuccessTimeout < DefaultDuration(time.Second) {
		config.PollDirty.SuccessTimeout = DefaultDuration(time.Second)
	}
	if config.PollDirty.ErrorTimeout < DefaultDuration(time.Second) {
		config.PollDirty.ErrorTimeout = DefaultDuration(time.Second)
	}
	return config, err
}

// MustLoad loads the configuration from the environment
// and panics if an error is encountered.
func MustLoad() Config {
	config, err := Load()
	if err != nil {
		panic(err)
	}
	return config
}
