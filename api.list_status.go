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

type ListStatusRequest struct {
	// Path of the object to get.
	//
	// Path is a required field
	Path *string `validate:"required"`
}

type ListStatusResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`
	FileStatuses FileStatuses `json:"FileStatuses"`
}

func (req *ListStatusRequest) RawPath() string {
	return aws.StringValue(req.Path)
}
func (req *ListStatusRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpListStatus)
	return v.Encode()
}

func (resp *ListStatusResponse) UnmarshalHTTP(httpResp *http.Response) error {
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
		return err
	}

	if err := resp.Exception(); err != nil {
		return err
	}
	return nil
}

// List a File/Directory
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#List_a_Directory
func (c *Client) ListStatus(req *ListStatusRequest) (*ListStatusResponse, error) {
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
		httpResp, err := c.httpClient.Get(u.String())
		if err != nil {
			errs = append(errs, err)
			continue
		}

		var resp ListStatusResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}

		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
