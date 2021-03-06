package webhdfs

import (
	"net/http"
	"net/url"
	"path"
	"strings"

	path_ "github.com/searKing/golang/go/path"

	http_ "github.com/searKing/webhdfs/http"
)

//go:generate go-option -type "Client"
type Client struct {
	httpClient func() http_.Client
	username   *string

	// options
	opts *Config
}

func New(endpoint string, opts ...ClientOption) (*Client, error) {
	opts = append(opts, withEndpoint(endpoint))

	c := &Client{opts: NewConfig()}
	c.ApplyOptions(opts...)
	return c.opts.Complete().New()
}

func (c *Client) HttpSchema() string {
	if c.opts.DisableSSL {
		return "http"
	}
	return "https"
}

type Request interface {
	RawPath() string
	RawQuery() string
}

func (c *Client) HttpUrl(query Request) url.URL {
	var sep string
	// keep last '/',avoid path.Join clean
	// for hdfs only accept path which starts with '/'
	if strings.HasSuffix(query.RawPath(), string(path_.Separator)) {
		sep = string(path_.Separator)
	}
	return url.URL{
		Scheme:   c.HttpSchema(),
		Path:     path.Join(PathPrefix, query.RawPath()) + sep,
		RawQuery: query.RawQuery(),
	}
}

// ProxyUser returns the authenticated user, may be needed as 'user.name' to authenticate
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Authentication
func (c *Client) ProxyUser() ProxyUser {
	return ProxyUser{Username: c.username}
}

func isSuccessHttpCode(code int) bool {
	return code >= http.StatusOK && code <= http.StatusPartialContent
}
