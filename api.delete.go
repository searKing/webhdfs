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
)

type DeleteRequest struct {
	Authentication
	ProxyUser
	CSRF

	// Path of the object to get.
	//
	// Path is a required field
	Path *string `validate:"required"`

	// Name				recursive
	// Description		Should the operation act on the content in the subdirectories?
	// Type				boolean
	// Default Value	false
	// Valid Values		true
	// Syntax			true
	Recursive *bool
}

type DeleteResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`

	Boolean Boolean `json:"boolean"`
}

func (req *DeleteRequest) RawPath() string {
	return aws.StringValue(req.Path)
}
func (req *DeleteRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpDelete)
	if req.Authentication.Delegation != nil {
		v.Set("delegation", aws.StringValue(req.Authentication.Delegation))
	}
	if req.ProxyUser.Username != nil {
		v.Set("user.name", aws.StringValue(req.ProxyUser.Username))
	}
	if req.ProxyUser.DoAs != nil {
		v.Set("doas", aws.StringValue(req.ProxyUser.DoAs))
	}

	if req.Recursive != nil {
		v.Set("recursive", fmt.Sprintf("%t", aws.BoolValue(req.Recursive)))
	}
	return v.Encode()
}

func (resp *DeleteResponse) UnmarshalHTTP(httpResp *http.Response) error {
	resp.HttpResponse.UnmarshalHTTP(httpResp)

	if isSuccessHttpCode(httpResp.StatusCode) {
		return nil
	}
	defer resp.Body.Close()
	// direct or error

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return nil
	}
	err = json.Unmarshal(body, &resp)
	if err != nil {
		return err
	}
	if err := resp.Exception(); err != nil {
		return err
	}
	return nil
}

// Delete a File/Directory
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Delete_a_File.2FDirectory
func (c *Client) Delete(req *DeleteRequest) (*DeleteResponse, error) {
	return c.delete(nil, req)
}
func (c *Client) DeleteWithContext(ctx context.Context, req *DeleteRequest) (*DeleteResponse, error) {
	if ctx == nil {
		panic("nil context")
	}
	return c.delete(ctx, req)
}
func (c *Client) delete(ctx context.Context, req *DeleteRequest) (*DeleteResponse, error) {
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

		httpReq, err := http.NewRequest(http.MethodDelete, u.String(), nil)
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

		var resp DeleteResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}

		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
