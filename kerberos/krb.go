package kerberos

import (
	krb "github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
)

func CloneConfig(c *krb.Client) *krb.Client {
	if c == nil {
		return nil
	}
	var kerberosClient = *c

	if c.Config == nil {
		kerberosClient.Config = config.New()
	} else {
		kerberosClient.Config = &(*c.Config)
	}
	return &kerberosClient
}
