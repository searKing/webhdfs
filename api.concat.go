package webhdfs

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/searKing/golang/go/errors"
)

type ConcatRequest struct {
	ProxyUser
	CSRF

	// Path of the object to get.
	//
	// Path is a required field
	Path *string `validate:"required"`

	// Name				sources
	// Description		A list of source paths.
	// Type				String
	// Default Value	<empty>
	// Valid Values		A list of comma seperated absolute FileSystem paths without scheme and authority.
	// Syntax			Any string.
	Sources *string
}

type ConcatResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`
}

func (req *ConcatRequest) RawPath() string {
	return aws.StringValue(req.Path)
}
func (req *ConcatRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpConcat)
	if req.ProxyUser.Username != nil {
		v.Set("user.name", aws.StringValue(req.ProxyUser.Username))
	}
	if req.ProxyUser.DoAs != nil {
		v.Set("doas", aws.StringValue(req.ProxyUser.DoAs))
	}
	if req.ProxyUser.Delegation != nil {
		v.Set("delegation", aws.StringValue(req.ProxyUser.Delegation))
	}

	if req.Sources != nil {
		v.Set("sources", aws.StringValue(req.Sources))
	}
	return v.Encode()
}

func (resp *ConcatResponse) UnmarshalHTTP(httpResp *http.Response) error {
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
		return err
	}
	if err := resp.Exception(); err != nil {
		return err
	}
	return nil
}

// Concat File(s)
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Concat_File(s)
// Also see: https://issues.apache.org/jira/browse/HDFS-6641
// the pre-conditions of HDFS concat right now are:
//
// All source files must be in the same directory.
// Replication and block size must be the same for all source files.
// All blocks must be full in all source files except the last source file.
// In the last source file, all blocks must be full except the last block.
func (c *Client) Concat(req *ConcatRequest) (*ConcatResponse, error) {
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

		httpResp, err := c.httpClient.Do(httpReq)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		var resp ConcatResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}

		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
