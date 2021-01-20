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

type MkdirsRequest struct {
	Authentication
	ProxyUser
	CSRF

	// Path of the object to get.
	//
	// Path is a required field
	Path *string `validate:"required"`

	// Name	permission
	// Description		The permission of a file/directory.
	// Type	Octal
	// Default Value	644 for files, 755 for directories
	// Valid Values		0 - 1777
	// Syntax			Any radix-8 integer (leading zeros may be omitted.)
	Permission *int
}

type MkdirsResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`
	Boolean      Boolean `json:"boolean"`
}

func (req *MkdirsRequest) RawPath() string {
	return aws.StringValue(req.Path)
}
func (req *MkdirsRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpMkdirs)
	if req.Authentication.Delegation != nil {
		v.Set("delegation", aws.StringValue(req.Authentication.Delegation))
	}
	if req.ProxyUser.Username != nil {
		v.Set("user.name", aws.StringValue(req.ProxyUser.Username))
	}
	if req.ProxyUser.DoAs != nil {
		v.Set("doas", aws.StringValue(req.ProxyUser.DoAs))
	}

	if req.Permission != nil {
		v.Set("permission", fmt.Sprintf("%#o", aws.IntValue(req.Permission)))
	}
	return v.Encode()
}

func (resp *MkdirsResponse) UnmarshalHTTP(httpResp *http.Response) error {
	resp.HttpResponse.UnmarshalHTTP(httpResp)
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

// Make a Directory
// If no permissions are specified, the newly created directory will have 755 permission as default.
// No umask mode will be applied from server side (so “fs.permissions.umask-mode” value configuration set on Namenode side will have no effect).
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Make_a_Directory
func (c *Client) Mkdirs(req *MkdirsRequest) (*MkdirsResponse, error) {
	return c.mkdirs(nil, req)
}
func (c *Client) MkdirsWithContext(ctx context.Context, req *MkdirsRequest) (*MkdirsResponse, error) {
	if ctx == nil {
		panic("nil context")
	}
	return c.mkdirs(ctx, req)
}
func (c *Client) mkdirs(ctx context.Context, req *MkdirsRequest) (*MkdirsResponse, error) {
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

		httpReq, err := http.NewRequest(http.MethodPut, u.String(), nil)
		if err != nil {
			return nil, err
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

		var resp MkdirsResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}

		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
