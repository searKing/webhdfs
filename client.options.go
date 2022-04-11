// Copyright 2022 The searKing Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package webhdfs

import (
	"net/http"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/searKing/golang/go/exp/types"

	"github.com/searKing/webhdfs/kerberos"
)

func withEndpoint(endpoint string) ClientOption {
	return ClientOptionFunc(func(c *Client) {
		c.opts.Addresses = strings.Split(endpoint, ",")
	})
}

func withUsername(username string) ClientOption {
	return ClientOptionFunc(func(c *Client) {
		c.opts.Username = types.Pointer(username)
	})
}

func WithDisableSSL(disableSSL bool) ClientOption {
	return ClientOptionFunc(func(c *Client) {
		c.opts.DisableSSL = disableSSL
	})
}

func WithValidator(v *validator.Validate) ClientOption {
	return ClientOptionFunc(func(c *Client) {
		c.opts.Validator = v
	})
}

func WithHttpClient(httpCli *http.Client) ClientOption {
	return ClientOptionFunc(func(c *Client) {
		if c.opts == nil {
			c.opts = NewConfig()
		}
		c.opts.HttpConfig.HttpClient = httpCli
	})
}

func WithKerberosConfig(kerberosConfig *kerberos.Config) ClientOption {
	return ClientOptionFunc(func(c *Client) {
		if c.opts == nil {
			c.opts = NewConfig()
		}
		c.opts.HttpConfig.KerberosConfig = kerberosConfig
	})
}

func WithKerberosPassword(username string, spn string, realm string, password string, krb5Con string) ClientOption {
	return WithKerberosConfig(&kerberos.Config{
		UserName:             username,
		ServicePrincipleName: spn,
		Realm:                realm,
		Password:             password,
		ConfigString:         krb5Con,
	})
}

func WithKerberosKeytab(username string, spn string, realm string, keytab string, krb5Con string) ClientOption {
	return WithKerberosConfig(&kerberos.Config{
		UserName:             username,
		ServicePrincipleName: spn,
		Realm:                realm,
		KeyTabString:         keytab,
		ConfigString:         krb5Con,
	})
}

func WithKerberosCCache(username string, spn string, realm string, cc string, krb5Con string) ClientOption {
	return WithKerberosConfig(&kerberos.Config{
		UserName:             username,
		ServicePrincipleName: spn,
		Realm:                realm,
		CCacheString:         cc,
		ConfigString:         krb5Con,
	})
}

func WithKerberosKeytabFile(username string, spn string, realm string, keytabFile string, krb5ConFile string) ClientOption {
	return WithKerberosConfig(&kerberos.Config{
		UserName:             username,
		ServicePrincipleName: spn,
		Realm:                realm,
		KeyTabFile:           keytabFile,
		ConfigFile:           krb5ConFile,
	})
}

func WithKerberosCCacheFile(username string, spn string, realm string, ccFile string, krb5ConFile string) ClientOption {
	return WithKerberosConfig(&kerberos.Config{
		UserName:             username,
		ServicePrincipleName: spn,
		Realm:                realm,
		CCacheString:         ccFile,
		ConfigString:         krb5ConFile,
	})
}
