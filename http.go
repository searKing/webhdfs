package webhdfs

import (
	"io"
	"net/http"
	"net/url"

	krb "github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/spnego"
)

type HttpClient interface {
	Head(url string) (resp *http.Response, err error)
	Get(url string) (resp *http.Response, err error)
	Post(url, contentType string, body io.Reader) (resp *http.Response, err error)
	PostForm(url string, data url.Values) (resp *http.Response, err error)
	Do(req *http.Request) (*http.Response, error)
}

func GetHttpClient(httpCl *http.Client, kerberosClient *krb.Client, spn string) HttpClient {
	if kerberosClient == nil {
		if httpCl != nil {
			return httpCl
		}
		return http.DefaultClient
	}
	return spnego.NewClient(kerberosClient, httpCl, spn)
}
