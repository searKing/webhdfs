package http

import (
	"io"
	"net/http"
	"net/url"
)

type Client interface {
	Head(url string) (resp *http.Response, err error)
	Get(url string) (resp *http.Response, err error)
	Post(url, contentType string, body io.Reader) (resp *http.Response, err error)
	PostForm(url string, data url.Values) (resp *http.Response, err error)
	Do(req *http.Request) (*http.Response, error)
}
