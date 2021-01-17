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

type UnsetECPolicyRequest struct {
	// Path of the object to get.
	//
	// Path is a required field
	Path *string `validate:"required"`
}

type UnsetECPolicyResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`
}

func (req *UnsetECPolicyRequest) RawPath() string {
	return aws.StringValue(req.Path)
}
func (req *UnsetECPolicyRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpUnsetECPolicy)
	return v.Encode()
}

func (resp *UnsetECPolicyResponse) UnmarshalHTTP(httpResp *http.Response) error {
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

// Unset EC Policy
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Unset_EC_Policy
func (c *Client) UnsetECPolicy(req *UnsetECPolicyRequest) (*UnsetECPolicyResponse, error) {
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

		httpResp, err := c.httpClient.Do(httpReq)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		var resp UnsetECPolicyResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}

		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
