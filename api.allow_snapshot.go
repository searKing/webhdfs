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

type AllowSnapshotRequest struct {
	Authentication
	ProxyUser
	CSRF
	HttpRequest

	// Path of the object to get.
	//
	// Path is a required field
	Path *string `validate:"required"`
}

type AllowSnapshotResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`
}

func (req *AllowSnapshotRequest) RawPath() string {
	return aws.StringValue(req.Path)
}
func (req *AllowSnapshotRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpAllowSnapshot)
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

func (resp *AllowSnapshotResponse) UnmarshalHTTP(httpResp *http.Response) error {
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
		return ErrorFromHttpResponse(httpResp)
	}
	if err = json.Unmarshal(body, &resp); err != nil {
		return err
	}

	if err := resp.Exception(); err != nil {
		return err
	}
	return nil
}

// Renew Delegation Token
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Renew_Delegation_Token
// expire time set by server "dfs.namenode.delegation.token.max-lifetime"
// See: https://hadoop.apache.org/docs/r2.7.1/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml#dfs.namenode.delegation.token.max-lifetime
func (c *Client) AllowSnapshot(req *AllowSnapshotRequest) (*AllowSnapshotResponse, error) {
	return c.allowSnapshot(nil, req)
}
func (c *Client) AllowSnapshotWithContext(ctx context.Context, req *AllowSnapshotRequest) (*AllowSnapshotResponse, error) {
	if ctx == nil {
		panic("nil context")
	}
	return c.allowSnapshot(ctx, req)
}

func (c *Client) allowSnapshot(ctx context.Context, req *AllowSnapshotRequest) (*AllowSnapshotResponse, error) {
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
		httpReq.Close = req.HttpRequest.Close
		if req.CSRF.XXsrfHeader != nil {
			httpReq.Header.Set("X-XSRF-HEADER", aws.StringValue(req.CSRF.XXsrfHeader))
		}

		if ctx != nil {
			httpReq = httpReq.WithContext(ctx)
		}

		if req.HttpRequest.PreSendHandler != nil {
			httpReq, err = req.HttpRequest.PreSendHandler(httpReq)
			if err != nil {
				return nil, fmt.Errorf("pre send handled: %w", err)
			}
		}

		httpResp, err := c.httpClient().Do(httpReq)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		var resp AllowSnapshotResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}
		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
