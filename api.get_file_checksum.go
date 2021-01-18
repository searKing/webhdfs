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

type GetFileChecksumRequest struct {
	Authentication
	ProxyUser
	CSRF

	// Path of the object to get.
	//
	// Path is a required field
	Path *string `validate:"required"`
	// Name				nodirect
	// Description		Disable automatically redirected.
	// Type				bool
	// Default Value	false
	// Valid Values		true|false
	// Syntax			Any Bool.
	NoDirect *bool
}

type GetFileChecksumResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`
	FileChecksum FileChecksum `json:"FileChecksum"`

	NoDirect bool    `json:"-"`
	Location *string `json:"Location"`
}

func (req *GetFileChecksumRequest) RawPath() string {
	return aws.StringValue(req.Path)
}
func (req *GetFileChecksumRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpGetFileChecksum)
	if req.Authentication.Delegation != nil {
		v.Set("delegation", aws.StringValue(req.Authentication.Delegation))
	}
	if req.ProxyUser.Username != nil {
		v.Set("user.name", aws.StringValue(req.ProxyUser.Username))
	}
	if req.ProxyUser.DoAs != nil {
		v.Set("doas", aws.StringValue(req.ProxyUser.DoAs))
	}

	if req.NoDirect != nil {
		v.Set("noredirect", fmt.Sprintf("%t", aws.BoolValue(req.NoDirect)))
	}
	return v.Encode()
}

func (resp *GetFileChecksumResponse) UnmarshalHTTP(httpResp *http.Response) error {
	resp.HttpResponse.UnmarshalHTTP(httpResp)
	if isSuccessHttpCode(httpResp.StatusCode) && !resp.NoDirect {
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

// Get File Checksum
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Get_File_Checksum
func (c *Client) GetFileChecksum(req *GetFileChecksumRequest) (*GetFileChecksumResponse, error) {
	return c.getFileChecksum(nil, req)
}
func (c *Client) GetFileChecksumWithContext(ctx context.Context, req *GetFileChecksumRequest) (*GetFileChecksumResponse, error) {
	if ctx == nil {
		panic("nil context")
	}
	return c.getFileChecksum(ctx, req)
}
func (c *Client) getFileChecksum(ctx context.Context, req *GetFileChecksumRequest) (*GetFileChecksumResponse, error) {
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
		httpResp, err := c.httpClient.Do(httpReq)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		var resp GetFileChecksumResponse
		resp.NameNode = addr
		resp.NoDirect = aws.BoolValue(req.NoDirect)

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}

		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
