package webhdfs

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/aws/aws-sdk-go/aws"
	strings_ "github.com/searKing/golang/go/strings"

	"github.com/searKing/golang/go/errors"
)

type CheckAccessRequest struct {
	Authentication
	ProxyUser
	CSRF

	// Path of the object to get.
	//
	// Path is a required field
	Path *string `validate:"required"`

	// Fs Action
	// Name				fsaction
	// Description		File system operation read/write/execute
	// Type				String
	// Default Value	null (an invalid value)
	// Valid Values		Strings matching regex pattern  "[r-][w-][x-] "
	// Syntax		 	"[r-][w-][x-] "
	// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Fs_Action
	Fsaction *string `validate:"required"`
}

type CheckAccessResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`
}

func (req *CheckAccessRequest) RawPath() string {
	return aws.StringValue(req.Path)
}
func (req *CheckAccessRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpCheckAccess)
	if req.Authentication.Delegation != nil {
		v.Set("delegation", aws.StringValue(req.Authentication.Delegation))
	}
	if req.ProxyUser.Username != nil {
		v.Set("user.name", aws.StringValue(req.ProxyUser.Username))
	}
	if req.ProxyUser.DoAs != nil {
		v.Set("doas", aws.StringValue(req.ProxyUser.DoAs))
	}

	return v.Encode()
}

func (resp *CheckAccessResponse) UnmarshalHTTP(httpResp *http.Response) error {
	resp.HttpResponse.UnmarshalHTTP(httpResp)
	if isSuccessHttpCode(httpResp.StatusCode) {
		return nil
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return nil
	}
	err = json.Unmarshal(body, &resp)
	if err != nil {
		return fmt.Errorf("parse %s: %w", strings_.Truncate(string(body), MaxHTTPBodyLengthDumped), err)
	}

	if err := resp.Exception(); err != nil {
		return err
	}
	return nil
}

// Check access
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Check_access
func (c *Client) CheckAccess(req *CheckAccessRequest) (*CheckAccessResponse, error) {
	return c.checkAccess(nil, req)
}
func (c *Client) CheckAccessWithContext(ctx context.Context, req *CheckAccessRequest) (*CheckAccessResponse, error) {
	if ctx == nil {
		panic("nil context")
	}
	return c.checkAccess(ctx, req)
}
func (c *Client) checkAccess(ctx context.Context, req *CheckAccessRequest) (*CheckAccessResponse, error) {
	err := c.opts.Validator.Struct(req)
	if err != nil {
		return nil, err
	}

	nameNodes := c.opts.Addresses
	if nameNodes == nil {
		return nil, fmt.Errorf("missing namenode addresses")
	}
	var u = c.HttpUrl(req)

	var errs []error
	for _, addr := range nameNodes {
		u.Host = addr
		httpReq, err := http.NewRequest(http.MethodGet, u.String(), nil)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if req.CSRF.XXsrfHeader != nil {
			httpReq.Header.Set("X-XSRF-HEADER", aws.StringValue(req.CSRF.XXsrfHeader))
		}
		if ctx != nil {
			httpReq = httpReq.WithContext(ctx)
		}
		httpResp, err := c.httpClient.Do(httpReq)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		var resp CheckAccessResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}

		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
