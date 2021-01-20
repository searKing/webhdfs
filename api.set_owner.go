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

type SetOwnerRequest struct {
	Authentication
	ProxyUser
	CSRF

	// Path of the object to get.
	//
	// Path is a required field
	Path *string `validate:"required"`

	// Name				owner
	// Description		The username who is the owner of a file/directory.
	// Type				String
	// Default Value	<empty> (means keeping it unchanged)
	// Valid Values		Any valid username.
	// Syntax			Any string.
	Owner *string

	// Name				group
	// Description		The name of a group.
	// Type				String
	// Default Value	<empty> (means keeping it unchanged)
	// Valid Values		Any valid group name.
	// Syntax			Any string.
	Group *string
}

type SetOwnerResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`
}

func (req *SetOwnerRequest) RawPath() string {
	return aws.StringValue(req.Path)
}
func (req *SetOwnerRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpSetOwner)
	if req.Authentication.Delegation != nil {
		v.Set("delegation", aws.StringValue(req.Authentication.Delegation))
	}
	if req.ProxyUser.Username != nil {
		v.Set("user.name", aws.StringValue(req.ProxyUser.Username))
	}
	if req.ProxyUser.DoAs != nil {
		v.Set("doas", aws.StringValue(req.ProxyUser.DoAs))
	}

	if req.Owner != nil {
		v.Set("owner", aws.StringValue(req.Owner))
	}
	if req.Group != nil {
		v.Set("group", aws.StringValue(req.Group))
	}
	return v.Encode()
}

func (resp *SetOwnerResponse) UnmarshalHTTP(httpResp *http.Response) error {
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

// Set Owner
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Set_Owner
func (c *Client) SetOwner(req *SetOwnerRequest) (*SetOwnerResponse, error) {
	return c.setOwner(nil, req)
}
func (c *Client) SetOwnerWithContext(ctx context.Context, req *SetOwnerRequest) (*SetOwnerResponse, error) {
	if ctx == nil {
		panic("nil context")
	}
	return c.setOwner(ctx, req)
}
func (c *Client) setOwner(ctx context.Context, req *SetOwnerRequest) (*SetOwnerResponse, error) {
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

		var resp SetOwnerResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}

		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
