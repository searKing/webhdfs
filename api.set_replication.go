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

type SetReplicationRequest struct {
	// Path of the object to get.
	//
	// Path is a required field
	Path *string `validate:"required"`

	// Name			replication
	// Description	The number of replications of a file.
	// Type			short
	// Default Value	Specified in the configuration.
	// Valid Values	> 0
	// Syntax		Any integer.
	Replication *int `validate:"required"`
}

type SetReplicationResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`
	Boolean      Boolean `json:"boolean"`
}

func (req *SetReplicationRequest) RawPath() string {
	return aws.StringValue(req.Path)
}
func (req *SetReplicationRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpSetReplication)
	if req.Replication != nil {
		v.Set("replication", fmt.Sprintf("%d", aws.IntValue(req.Replication)))
	}
	return v.Encode()
}

func (resp *SetReplicationResponse) UnmarshalHTTP(httpResp *http.Response) error {
	resp.HttpResponse.UnmarshalHTTP(httpResp)
	defer resp.Body.Close()
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

// Replication
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Replication
func (c *Client) SetReplication(req *SetReplicationRequest) (*SetReplicationResponse, error) {
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

		var resp SetReplicationResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}

		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
