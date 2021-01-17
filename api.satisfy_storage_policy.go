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

type SatisfyStoragePolicyRequest struct {
	// Path of the object to get.
	//
	// Path is a required field
	Path *string `validate:"required"`

	// Name				storagepolicy
	// Description		The name of the storage policy.
	// Type				String
	// Default Value	<empty>
	// Valid Values		Any valid storage policy name; see GETALLSTORAGEPOLICY.
	// Syntax			Any string.
	StoragePolicy *string `validate:"required"`
}

type SatisfyStoragePolicyResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`
}

func (req *SatisfyStoragePolicyRequest) RawPath() string {
	return aws.StringValue(req.Path)
}
func (req *SatisfyStoragePolicyRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpSatisfyStoragePolicy)
	if req.StoragePolicy != nil {
		v.Set("storagepolicy", aws.StringValue(req.StoragePolicy))
	}
	return v.Encode()
}

func (resp *SatisfyStoragePolicyResponse) UnmarshalHTTP(httpResp *http.Response) error {
	resp.HttpResponse.UnmarshalHTTP(httpResp)
	defer resp.Body.Close()
	if isSuccessHttpCode(httpResp.StatusCode) {
		return nil
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
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

// Set Storage Policy
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Set_Storage_Policy
func (c *Client) SatisfyStoragePolicy(req *SatisfyStoragePolicyRequest) (*SatisfyStoragePolicyResponse, error) {
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

		var resp SatisfyStoragePolicyResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}

		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
