// Copyright 2022 The searKing Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package webhdfs

import (
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/searKing/golang/go/exp/types"
	path_ "github.com/searKing/golang/go/path"

	http_ "github.com/searKing/webhdfs/http"
)

// Config
// Code borrowed from https://github.com/kubernetes/kubernetes
// call chains: NewConfig -> Complete -> [Validate] -> New|Apply
type Config struct {
	// Addresses specifies the namenode(s) to connect to.
	Addresses []string `validate:"required"`

	// The authenticated user
	Username *string

	// Set this to `true` to disable SSL when sending requests. Defaults
	// to `false`.
	DisableSSL bool

	HttpConfig *http_.Config `validate:"dive"`

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
	if o.HttpConfig.Validator == nil {
		o.HttpConfig.Validator = o.Validator
	}
	return CompletedConfig{&completedConfig{o}}
}

// Validate checks Config.
func (o *completedConfig) Validate() error {
	return o.Validator.Struct(o)
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
		username:   c.proxyUser(),
		opts:       c.Config,
	}, nil
}

func (c completedConfig) proxyUser() *string {
	if c.Username != nil {
		return c.Username
	}
	if c.HttpConfig == nil || c.HttpConfig.KerberosConfig == nil {
		return nil
	}
	username := c.HttpConfig.KerberosConfig.UserName
	if username == "" {
		return nil
	}

	i := strings.Index(username, string(path_.Separator))
	if i >= 0 {
		return types.Pointer(username[:i])
	}
	return types.Pointer(username)
}
