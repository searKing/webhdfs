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

type GetSnapshottableDirectoryListRequest struct {
	ProxyUser
	CSRF

	// Name				user.name
	// Description		The authenticated user; see Authentication.
	// Type				String
	// Default Value	null
	// Valid Values		Any valid username.
	// Syntax			Any string.
	// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Username
	Username *string `json:"user.name,omitempty" validate:"required"`
}

type GetSnapshottableDirectoryListResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`
	SnapshottableDirectoryList
}

func (req *GetSnapshottableDirectoryListRequest) RawPath() string {
	return ""
}
func (req *GetSnapshottableDirectoryListRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpGetSnapshottableDirectoryList)
	if req.ProxyUser.Username != nil {
		v.Set("user.name", aws.StringValue(req.ProxyUser.Username))
	}
	if req.ProxyUser.DoAs != nil {
		v.Set("doas", aws.StringValue(req.ProxyUser.DoAs))
	}
	if req.ProxyUser.Delegation != nil {
		v.Set("delegation", aws.StringValue(req.ProxyUser.Delegation))
	}

	if req.Username != nil {
		v.Set("user.name", aws.StringValue(req.Username))
	}
	return v.Encode()
}

func (resp *GetSnapshottableDirectoryListResponse) UnmarshalHTTP(httpResp *http.Response) error {
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

// Get Snapshottable Directory List
// If the USER is not the hdfs super user, the call lists only the snapshottable directories owned by the user.
// If the USER is the hdfs super user, the call lists all the snapshottable directories.
// See also: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Get_Snapshottable_Directory_List
func (c *Client) GetSnapshottableDirectoryList(req *GetSnapshottableDirectoryListRequest) (*GetSnapshottableDirectoryListResponse, error) {
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
		httpResp, err := c.httpClient.Do(httpReq)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		var resp GetSnapshottableDirectoryListResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}
		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
