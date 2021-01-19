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

type SetReplicationRequest struct {
	Authentication
	ProxyUser
	CSRF

	// Path of the object to get.
	//
	// Path is a required field
	Path *string `validate:"required"`

	// Name			replication
	// Description	The number of replications of a file.
	// Type			short
	// Default Value	Specified in the configuration.
	// Valid Values	> 0
	// Syntax		Any integer.
	Replication *int `validate:"required"`
}

type SetReplicationResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`
	Boolean      Boolean `json:"boolean"`
}

func (req *SetReplicationRequest) RawPath() string {
	return aws.StringValue(req.Path)
}
func (req *SetReplicationRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpSetReplication)
	if req.Authentication.Delegation != nil {
		v.Set("delegation", aws.StringValue(req.Authentication.Delegation))
	}
	if req.ProxyUser.Username != nil {
		v.Set("user.name", aws.StringValue(req.ProxyUser.Username))
	}
	if req.ProxyUser.DoAs != nil {
		v.Set("doas", aws.StringValue(req.ProxyUser.DoAs))
	}

	if req.Replication != nil {
		v.Set("replication", fmt.Sprintf("%d", aws.IntValue(req.Replication)))
	}
	return v.Encode()
}

func (resp *SetReplicationResponse) UnmarshalHTTP(httpResp *http.Response) error {
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

// Replication
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Replication
func (c *Client) SetReplication(req *SetReplicationRequest) (*SetReplicationResponse, error) {
	return c.setReplication(nil, req)
}
func (c *Client) SetReplicationWithContext(ctx context.Context, req *SetReplicationRequest) (*SetReplicationResponse, error) {
	if ctx == nil {
		panic("nil context")
	}
	return c.setReplication(ctx, req)
}
func (c *Client) setReplication(ctx context.Context, req *SetReplicationRequest) (*SetReplicationResponse, error) {
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
		httpResp, err := c.httpClient.Do(httpReq)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		var resp SetReplicationResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}

		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
