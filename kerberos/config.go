package kerberos

import (
	"fmt"

	"github.com/go-playground/validator/v10"
	krb "github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/credentials"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/searKing/golang/go/errors"
)

// Config
// Code borrowed from https://github.com/kubernetes/kubernetes
// call chains: NewConfig -> Complete -> [Validate] -> New|Apply
type Config struct {
	UserName             string // hdfs/quickstart.cloudera
	ServicePrincipleName string // <SERVICE>/<FQDN>, hdfs/quickstart.cloudera
	Realm                string // EXAMPLE.COM, CLOUDERA

	// Load Order If Not Empty
	Password string

	CCacheString string
	KeyTabString string
	ConfigString string

	CCacheFile string
	KeyTabFile string
	ConfigFile string
	Validator  *validator.Validate
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
		UserName:             "", // "hdfs/quickstart.cloudera"
		ServicePrincipleName: "", // "HTTP/quickstart.cloudera"
		Realm:                "", // CLOUDERA
		CCacheFile:           "/tmp/krb5cc_0",
		ConfigFile:           "/etc/krb5.conf",
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

// Validate checks Config.
func (c *completedConfig) Validate() error {
	return c.Validator.Struct(c)
}

// New creates a new server which logically combines the handling chain with the passed server.
// The handler chain in particular can be difficult as it starts delgating.
// New usually called after Complete
func (c completedConfig) New() (*krb.Client, error) {
	err := c.Validate()
	if err != nil {
		return nil, err
	}
	return c.loadKerberosClient()
}

func (c *Config) loadKerberosClient() (*krb.Client, error) {
	krb5Config, err := c.loadKerberosConf()
	if err != nil {
		return nil, err
	}

	if c.Password != "" {
		return c.loadKerberosClientWithPassword(krb5Config), nil
	}
	if c.KeyTabString != "" || c.KeyTabFile != "" {
		return c.loadKerberosClientWithKeyTab(krb5Config)
	}

	if c.CCacheString != "" || c.CCacheFile != "" {
		return c.loadKerberosClientWithCCache(krb5Config)
	}

	return nil, nil
}

func (c *Config) loadKerberosConf() (*config.Config, error) {
	if c.ConfigString != "" {
		cfg, err := config.NewFromString(c.ConfigString)
		if err != nil {
			return nil, fmt.Errorf("load krb5 config file %s: %w", c.ConfigFile, err)
		}
		return cfg, nil
	}
	if c.ConfigFile != "" {
		cfg, err := config.Load(c.ConfigFile)
		if err != nil {
			return nil, fmt.Errorf("load krb5 config file %s: %w", c.ConfigFile, err)
		}
		return cfg, nil
	}
	return config.New(), nil
}

func (c *Config) loadKerberosClientWithPassword(krb5Config *config.Config) *krb.Client {
	return krb.NewWithPassword(c.UserName, c.Realm, c.Password, krb5Config)
}

func (c *Config) loadKerberosClientWithKeyTab(krb5Config *config.Config) (*krb.Client, error) {
	kt, err := c.loadKerberosKeyTab()
	if err != nil {
		return nil, err
	}
	return krb.NewWithKeytab(c.UserName, c.Realm, kt, krb5Config), nil
}

func (c *Config) loadKerberosClientWithCCache(krb5Config *config.Config) (*krb.Client, error) {
	cc, err := c.loadKerberosCCache()
	if err != nil {
		return nil, err
	}
	return krb.NewFromCCache(cc, krb5Config)
	//return krb.NewFromCCache(cc, krb5Config, krb.DisablePAFXFAST(true))
}

func (c *Config) loadKerberosKeyTab() (*keytab.Keytab, error) {
	var errs []error
	if c.KeyTabString != "" {
		var kt keytab.Keytab
		err := kt.Unmarshal([]byte(c.KeyTabString))
		if err == nil {
			return &kt, nil
			//return krb.NewWithKeytab(c.opts.UserName, c.Realm, kt, krb5Config, krb.DisablePAFXFAST(true)), nil
		}
		errs = append(errs, fmt.Errorf("load krb5 keytab string %s: %w", c.KeyTabString, err))
	}

	if c.KeyTabFile != "" {
		kt, err := keytab.Load(c.KeyTabFile)
		if err == nil {
			return kt, nil
			//return krb.NewWithKeytab(c.opts.UserName, c.Realm, kt, krb5Config, krb.DisablePAFXFAST(true)), nil
		}
		errs = append(errs, fmt.Errorf("load krb5 keytab file %s: %w", c.KeyTabFile, err))
	}
	return nil, errors.Multi(errs...)
}

func (c *Config) loadKerberosCCache() (*credentials.CCache, error) {
	var errs []error
	if c.CCacheString != "" {
		var cc credentials.CCache
		err := cc.Unmarshal([]byte(c.CCacheString))
		if err == nil {
			return &cc, nil
			//return krb.NewWithKeytab(c.opts.UserName, c.Realm, kt, krb5Config, krb.DisablePAFXFAST(true)), nil
		}
		errs = append(errs, fmt.Errorf("load krb5 keytab string %s: %w", c.KeyTabString, err))
	}

	if c.CCacheFile != "" {
		cc, err := credentials.LoadCCache(c.CCacheFile)
		if err == nil {
			return cc, nil
		}
		errs = append(errs, fmt.Errorf("load krb5 keytab file %s: %w", c.KeyTabFile, err))
	}
	return nil, errors.Multi(errs...)
}
