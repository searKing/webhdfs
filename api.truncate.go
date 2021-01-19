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

type TruncateRequest struct {
	Authentication
	ProxyUser
	CSRF

	// Path of the object to get.
	//
	// Path is a required field
	Path *string `validate:"required"`

	// Name				newlength
	// Description		The size the file is to be truncated to.
	// Type				long
	// Valid Values		>= 0
	// Syntax			Any long.
	NewLength *int64 `validate:"required"`
}

type TruncateResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`

	Boolean Boolean `json:"boolean"`
}

func (req *TruncateRequest) RawPath() string {
	return aws.StringValue(req.Path)
}
func (req *TruncateRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpTruncate)
	if req.Authentication.Delegation != nil {
		v.Set("delegation", aws.StringValue(req.Authentication.Delegation))
	}
	if req.ProxyUser.Username != nil {
		v.Set("user.name", aws.StringValue(req.ProxyUser.Username))
	}
	if req.ProxyUser.DoAs != nil {
		v.Set("doas", aws.StringValue(req.ProxyUser.DoAs))
	}

	if req.NewLength != nil {
		v.Set("newlength", fmt.Sprintf("%d", aws.Int64Value(req.NewLength)))
	}
	return v.Encode()
}

func (resp *TruncateResponse) UnmarshalHTTP(httpResp *http.Response) error {
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

// Truncate a File
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Truncate_a_File
func (c *Client) Truncate(req *TruncateRequest) (*TruncateResponse, error) {
	return c.truncate(nil, req)
}
func (c *Client) TruncateWithContext(ctx context.Context, req *TruncateRequest) (*TruncateResponse, error) {
	if ctx == nil {
		panic("nil context")
	}
	return c.truncate(ctx, req)
}
func (c *Client) truncate(ctx context.Context, req *TruncateRequest) (*TruncateResponse, error) {
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

		httpReq, err := http.NewRequest(http.MethodPost, u.String(), nil)
		if err != nil {
			return nil, err
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

		var resp TruncateResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}

		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
