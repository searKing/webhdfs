package webhdfs

import (
	"fmt"

	http_ "github.com/searKing/webhdfs/http"
	"github.com/go-playground/validator/v10"
)

// Code borrowed from https://github.com/kubernetes/kubernetes
// call chains: NewConfig -> Complete -> [Validate] -> New|Apply
type Config struct {
	// Addresses specifies the namenode(s) to connect to.
	Addresses []string

	// Set this to `true` to disable SSL when sending requests. Defaults
	// to `false`.
	DisableSSL bool

	HttpConfig *http_.Config

	Validator *validator.Validate
}

type completedConfig struct {
	*Config

	//===========================================================================
	// values below here are filled in during completion
	//===========================================================================
}

type CompletedConfig struct {
	// Embed a private pointer that cannot be instantiated outside of this package.
	*completedConfig
}

// NewConfig returns a Config struct with the default values
func NewConfig() *Config {
	return &Config{
		Addresses:  nil,
		HttpConfig: http_.NewConfig(),
	}
}

// Complete fills in any fields not set that are required to have valid data and can be derived
// from other fields. If you're going to ApplyOptions, do that first. It's mutating the receiver.
func (o *Config) Complete() CompletedConfig {
	if o.Validator == nil {
		o.Validator = validator.New()
	}
	return CompletedConfig{&completedConfig{o}}
}

// Validate checks Config and return a slice of found errs.
func (o *Config) Validate() []error {
	errs := o.HttpConfig.Validate()
	if len(o.Addresses) == 0 {
		errs = append(errs, fmt.Errorf("missing namenode addresses"))
	}
	return errs
}

// New creates a new server which logically combines the handling chain with the passed server.
// The handler chain in particular can be difficult as it starts delgating.
// New usually called after Complete
func (c completedConfig) New() (*Client, error) {
	httpClient, err := c.HttpConfig.Complete().New()
	if err != nil {
		return nil, err
	}

	return &Client{
		httpClient: httpClient,
		opts:       c.Config,
	}, nil
}
