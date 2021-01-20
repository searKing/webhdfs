package http

import (
	"net/http"

	"github.com/jcmturner/gokrb5/v8/spnego"

	"github.com/searKing/webhdfs/kerberos"
)

// Code borrowed from https://github.com/kubernetes/kubernetes
// call chains: NewConfig -> Complete -> [Validate] -> New|Apply
type Config struct {
	HttpClient     *http.Client
	KerberosConfig *kerberos.Config
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
		HttpClient:     nil,
		KerberosConfig: kerberos.NewConfig(),
	}
}

// Complete fills in any fields not set that are required to have valid data and can be derived
// from other fields. If you're going to ApplyOptions, do that first. It's mutating the receiver.
func (o *Config) Complete() CompletedConfig {
	return CompletedConfig{&completedConfig{o}}
}

// Validate checks Config and return a slice of found errs.
func (o *Config) Validate() []error {
	return nil
}

// New creates a new server which logically combines the handling chain with the passed server.
// The handler chain in particular can be difficult as it starts delgating.
// New usually called after Complete
func (c completedConfig) New() (func() Client, error) {
	if c.KerberosConfig != nil {
		krbClient, err := c.KerberosConfig.Complete().New()
		if err != nil {
			return nil, err
		}
		if krbClient != nil {
			return func() Client {
				return spnego.NewClient(krbClient, c.HttpClient, c.KerberosConfig.ServicePrincipleName)
			}, nil
		}
	}

	return func() Client {
		if c.HttpClient != nil {
			return c.HttpClient
		}
		return http.DefaultClient
	}, nil

}
