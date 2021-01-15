package webhdfs

import (
	"net/http"
	"net/url"
	"path"

	"github.com/searKing/golang/go/errors"

	http_ "github.com/searKing/webhdfs/http"
)

//go:generate go-option -type "Client"
type Client struct {
	httpClient http_.Client

	// options
	opts *Config
}

func New(endpoint string, opts ...ClientOption) (*Client, error) {
	opts = append(opts, withEndpoint(endpoint))

	c := &Client{opts: NewConfig()}
	c.ApplyOptions(opts...)
	errs := c.opts.Validate()
	if err := errors.Multi(errs...); err != nil {
		return nil, err
	}
	return c.opts.Complete().New()
}

func (c *Client) HttpSchema() string {
	if c.opts.DisableSSL {
		return "http"
	}
	return "https"
}

func (c *Client) HttpUrl(query interface {
	RawPath() string
	RawQuery() string
}) url.URL {
	return url.URL{
		Scheme:   c.HttpSchema(),
		Path:     path.Join(PathPrefix, query.RawPath()),
		RawQuery: query.RawQuery(),
	}
}

func isSuccessHttpCode(code int) bool {
	return code >= http.StatusOK && code <= http.StatusPartialContent
}
