package webhdfs

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/searKing/golang/go/errors"
	time_ "github.com/searKing/golang/go/time"
)

type SetTimesRequest struct {
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
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if isSuccessHttpCode(httpResp.StatusCode) {
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

// Replication
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Replication
func (c *Client) SetTimes(req *SetTimesRequest) (*SetTimesResponse, error) {
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

		req, err := http.NewRequest(http.MethodPut, u.String(), nil)
		if err != nil {
			return nil, err
		}

		httpResp, err := c.httpClient.Do(req)
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
