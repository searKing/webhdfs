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

type GetFileBlockLocationsRequest struct {
	// Path of the object to get.
	//
	// Path is a required field
	Path *string `validate:"required"`
}

type GetFileBlockLocationsResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`
	BlockLocations BlockLocations `json:"BlockLocations"`
}

func (req *GetFileBlockLocationsRequest) RawPath() string {
	return aws.StringValue(req.Path)
}
func (req *GetFileBlockLocationsRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpGetFileBlockLocations)
	return v.Encode()
}

func (resp *GetFileBlockLocationsResponse) UnmarshalHTTP(httpResp *http.Response) error {
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

// Get File Block Locations
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Get_File_Block_Locations
func (c *Client) GetFileBlockLocations(req *GetFileBlockLocationsRequest) (*GetFileBlockLocationsResponse, error) {
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

		var resp GetFileBlockLocationsResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}

		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
