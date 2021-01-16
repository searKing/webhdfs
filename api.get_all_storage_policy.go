package webhdfs

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/searKing/golang/go/errors"
)

type GetAllStoragePolicyRequest struct {
}

type GetAllStoragePolicyResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse         `json:"-"`
	BlockStoragePolicies BlockStoragePolicies `json:"BlockStoragePolicies"`
}

func (req *GetAllStoragePolicyRequest) RawPath() string {
	return ""
}
func (req *GetAllStoragePolicyRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpGetAllStoragePolicy)
	return v.Encode()
}

func (resp *GetAllStoragePolicyResponse) UnmarshalHTTP(httpResp *http.Response) error {
	resp.HttpResponse.UnmarshalHTTP(httpResp)

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

// Get all Storage Policies
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Get_all_Storage_Policies
func (c *Client) GetAllStoragePolicy(req *GetAllStoragePolicyRequest) (*GetAllStoragePolicyResponse, error) {
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

		var resp GetAllStoragePolicyResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}

		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
