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

type SetStoragePolicyRequest struct {
	// Name				storagepolicy
	// Description		The name of the storage policy.
	// Type				String
	// Default Value	<empty>
	// Valid Values		Any valid storage policy name; see GETALLSTORAGEPOLICY.
	// Syntax			Any string.
	StoragePolicy *string `validate:"required"`
}

type SetStoragePolicyResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`
}

func (req *SetStoragePolicyRequest) RawPath() string {
	return ""
}
func (req *SetStoragePolicyRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpSetStoragePolicy)
	if req.StoragePolicy != nil {
		v.Set("storagepolicy", aws.StringValue(req.StoragePolicy))
	}
	return v.Encode()
}

func (resp *SetStoragePolicyResponse) UnmarshalHTTP(httpResp *http.Response) error {
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

// Get Storage Policy
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Get_Storage_Policy
func (c *Client) SetStoragePolicy(req *SetStoragePolicyRequest) (*SetStoragePolicyResponse, error) {
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

		var resp SetStoragePolicyResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}

		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
