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

type CreateSymlinkRequest struct {
	Authentication
	ProxyUser
	CSRF

	// Path of the object to get.
	//
	// Path is a required field
	Path *string `validate:"required"`

	// Name				destination
	// Description		The destination path.
	// Type				Path
	// Default Value	<empty> (an invalid path)
	// Valid Values		An absolute FileSystem path without scheme and authority.
	// Syntax			Any path.
	Destination *string `validate:"required"`

	// Name				createparent
	// Description		If the parent directories do not exist, should they be created?
	// Type				boolean
	// Default Value	true
	// Valid Values		true, false
	// Syntax			true
	CreateParent *bool
}

type CreateSymlinkResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`
}

func (req *CreateSymlinkRequest) RawPath() string {
	return aws.StringValue(req.Path)
}
func (req *CreateSymlinkRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpCreateSymlink)
	if req.Authentication.Delegation != nil {
		v.Set("delegation", aws.StringValue(req.Authentication.Delegation))
	}
	if req.ProxyUser.Username != nil {
		v.Set("user.name", aws.StringValue(req.ProxyUser.Username))
	}
	if req.ProxyUser.DoAs != nil {
		v.Set("doas", aws.StringValue(req.ProxyUser.DoAs))
	}

	if req.Destination != nil {
		v.Set("destination", fmt.Sprintf("%s", aws.StringValue(req.Destination)))
	}
	if req.CreateParent != nil {
		v.Set("createParent", fmt.Sprintf("%t", aws.BoolValue(req.CreateParent)))
	}
	return v.Encode()
}

func (resp *CreateSymlinkResponse) UnmarshalHTTP(httpResp *http.Response) error {
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

// Create a Symbolic Link
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Create_a_Symbolic_Link
func (c *Client) CreateSymlink(req *CreateSymlinkRequest) (*CreateSymlinkResponse, error) {
	return c.createSymlink(nil, req)
}
func (c *Client) CreateSymlinkWithContext(ctx context.Context, req *CreateSymlinkRequest) (*CreateSymlinkResponse, error) {
	if ctx == nil {
		panic("nil context")
	}
	return c.createSymlink(ctx, req)
}
func (c *Client) createSymlink(ctx context.Context, req *CreateSymlinkRequest) (*CreateSymlinkResponse, error) {
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

		var resp CreateSymlinkResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}

		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
