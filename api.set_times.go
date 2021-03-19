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
	time_ "github.com/searKing/golang/go/time"
)

type SetTimesRequest struct {
	Authentication
	ProxyUser
	CSRF
	HttpRequest

	// Path of the object to get.
	//
	// Path is a required field
	Path *string `validate:"required"`

	// Name				modificationtime
	// Description		The modification time of a file/directory.
	// Type				long
	// Default Value	-1 (means keeping it unchanged)
	// Valid Values		-1 or a timestamp
	// Syntax			Any integer.
	Modificationtime *time_.UnixTimeMillisecond

	// Name				accesstime
	// Description		The access time of a file/directory.
	// Type				long
	// Default Value	-1 (means keeping it unchanged)
	// Valid Values		-1 or a timestamp
	// Syntax			Any integer.
	Accesstime *time_.UnixTimeMillisecond
}

type SetTimesResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`
}

func (req *SetTimesRequest) RawPath() string {
	return aws.StringValue(req.Path)
}
func (req *SetTimesRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpSetTimes)
	if req.Authentication.Delegation != nil {
		v.Set("delegation", aws.StringValue(req.Authentication.Delegation))
	}
	if req.ProxyUser.Username != nil {
		v.Set("user.name", aws.StringValue(req.ProxyUser.Username))
	}
	if req.ProxyUser.DoAs != nil {
		v.Set("doas", aws.StringValue(req.ProxyUser.DoAs))
	}

	if req.Modificationtime != nil {
		v.Set("modificationtime", req.Modificationtime.String())
	}
	if req.Accesstime != nil {
		v.Set("accesstime", req.Accesstime.String())
	}
	return v.Encode()
}

func (resp *SetTimesResponse) UnmarshalHTTP(httpResp *http.Response) error {
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
	err = json.Unmarshal(body, &resp)
	if err != nil {
		return fmt.Errorf("parse %s: %w", strings_.Truncate(string(body), MaxHTTPBodyLengthDumped), err)
	}
	if err := resp.Exception(); err != nil {
		return err
	}
	return nil
}

// Set Access or Modification Time
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Set_Access_or_Modification_Time
func (c *Client) SetTimes(req *SetTimesRequest) (*SetTimesResponse, error) {
	return c.setTimes(nil, req)
}
func (c *Client) SetTimesWithContext(ctx context.Context, req *SetTimesRequest) (*SetTimesResponse, error) {
	if ctx == nil {
		panic("nil context")
	}
	return c.setTimes(ctx, req)
}
func (c *Client) setTimes(ctx context.Context, req *SetTimesRequest) (*SetTimesResponse, error) {
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

		var resp SetTimesResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}

		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
