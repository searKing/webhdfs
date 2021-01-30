package webhdfs

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/searKing/golang/go/errors"
	strings_ "github.com/searKing/golang/go/strings"
)

type GetStoragePolicyRequest struct {
	Authentication
	ProxyUser
	CSRF

	// Path of the object to get.
	//
	// Path is a required field
	Path *string
}

type GetStoragePolicyResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`
	BlockStoragePolicy
}

func (req *GetStoragePolicyRequest) RawPath() string {
	return aws.StringValue(req.Path)
}
func (req *GetStoragePolicyRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpGetStoragePolicy)
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

func (resp *GetStoragePolicyResponse) UnmarshalHTTP(httpResp *http.Response) error {
	resp.HttpResponse.UnmarshalHTTP(httpResp)
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return ErrorFromHttpResponse(httpResp)
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

// Get Storage Policy
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Get_Storage_Policy
func (c *Client) GetStoragePolicy(req *GetStoragePolicyRequest) (*GetStoragePolicyResponse, error) {
	return c.getStoragePolicy(nil, req)
}
func (c *Client) GetStoragePolicyWithContext(ctx context.Context, req *GetStoragePolicyRequest) (*GetStoragePolicyResponse, error) {
	if ctx == nil {
		panic("nil context")
	}
	return c.getStoragePolicy(ctx, req)
}
func (c *Client) getStoragePolicy(ctx context.Context, req *GetStoragePolicyRequest) (*GetStoragePolicyResponse, error) {
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
		httpResp, err := c.httpClient().Do(httpReq)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		var resp GetStoragePolicyResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}

		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
