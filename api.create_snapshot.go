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

type CreateSnapshotRequest struct {
	Authentication
	ProxyUser
	CSRF

	// Path of the object to get.
	//
	// Path is a required field
	Path *string `validate:"required"`
	// Name				snapshotname
	// Description		The name of the snapshot to be created/deleted. Or the new name for snapshot rename.
	// Type				String
	// Default Value	null
	// Valid Values		Any valid snapshot name.
	// Syntax			Any string.
	// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Snapshot_Name
	Snapshotname *string `json:"snapshotname,omitempty"`
}

type CreateSnapshotResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`

	// Path of the object to get.
	//
	// Path is a required field
	Path *string
}

func (req *CreateSnapshotRequest) RawPath() string {
	return aws.StringValue(req.Path)
}
func (req *CreateSnapshotRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpCreateSnapshot)
	if req.Authentication.Delegation != nil {
		v.Set("delegation", aws.StringValue(req.Authentication.Delegation))
	}
	if req.ProxyUser.Username != nil {
		v.Set("user.name", aws.StringValue(req.ProxyUser.Username))
	}
	if req.ProxyUser.DoAs != nil {
		v.Set("doas", aws.StringValue(req.ProxyUser.DoAs))
	}

	if req.Snapshotname != nil {
		v.Set("snapshotname", aws.StringValue(req.Snapshotname))
	}
	return v.Encode()
}

func (resp *CreateSnapshotResponse) UnmarshalHTTP(httpResp *http.Response) error {
	resp.HttpResponse.UnmarshalHTTP(httpResp)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return nil
	}
	if err = json.Unmarshal(body, &resp); err != nil {
		return err
	}

	if err := resp.Exception(); err != nil {
		return err
	}
	return nil
}

// Create Snapshot
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Create_Snapshot
func (c *Client) CreateSnapshot(req *CreateSnapshotRequest) (*CreateSnapshotResponse, error) {
	return c.createSnapshot(nil, req)
}
func (c *Client) CreateSnapshotWithContext(ctx context.Context, req *CreateSnapshotRequest) (*CreateSnapshotResponse, error) {
	if ctx == nil {
		panic("nil context")
	}
	return c.createSnapshot(ctx, req)
}
func (c *Client) createSnapshot(ctx context.Context, req *CreateSnapshotRequest) (*CreateSnapshotResponse, error) {
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

		var resp CreateSnapshotResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}
		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
