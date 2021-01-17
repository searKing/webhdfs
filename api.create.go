package webhdfs

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/searKing/golang/go/errors"
)

type CreateRequest struct {
	// Path of the object to get.
	//
	// Path is a required field
	Path *string `validate:"required"`

	// Object data.
	Body io.Reader

	// Name				overwrite
	// Description		If a file already exists, should it be overwritten?
	// Type				boolean
	// Default Value	false
	// Valid Values		true
	// Syntax			true
	Overwrite *bool
	// Name				blocksize
	// Description		The block size of a file.
	// Type				long
	// Default Value	Specified in the configuration.
	// Valid Values		> 0
	// Syntax			Any integer.
	Blocksize *int64
	// Name				replication
	// Description		The number of replications of a file.
	// Type				short
	// Default Value	Specified in the configuration.
	// Valid Values		> 0
	// Syntax			Any integer.
	Replication *int
	// Name	permission
	// Description		The permission of a file/directory.
	// Type	Octal
	// Default Value	644 for files, 755 for directories
	// Valid Values		0 - 1777
	// Syntax			Any radix-8 integer (leading zeros may be omitted.)
	Permission *int
	// Name				buffersize
	// Description		The size of the buffer used in transferring data.
	// Type				int
	// Default Value	Specified in the configuration.
	// Valid Values		> 0
	// Syntax			Any integer.
	BufferSize *int
	// Name				nodirect
	// Description		Disable automatically redirected.
	// Type				bool
	// Default Value	false
	// Valid Values		true|false
	// Syntax			Any Bool.
	NoDirect *bool
}

type CreateResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`
}

func (req *CreateRequest) RawPath() string {
	return aws.StringValue(req.Path)
}
func (req *CreateRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpCreate)
	if req.Overwrite != nil {
		v.Set("overwrite", fmt.Sprintf("%t", aws.BoolValue(req.Overwrite)))
	}
	if req.Blocksize != nil {
		v.Set("blocksize", fmt.Sprintf("%d", aws.Int64Value(req.Blocksize)))
	}
	if req.Replication != nil {
		v.Set("replication", fmt.Sprintf("%d", aws.IntValue(req.Replication)))
	}
	if req.Permission != nil {
		v.Set("permission", fmt.Sprintf("%#o", aws.IntValue(req.Permission)))
	}
	if req.BufferSize != nil {
		v.Set("buffersize", fmt.Sprintf("%d", aws.IntValue(req.BufferSize)))
	}
	if req.NoDirect != nil {
		v.Set("noredirect", fmt.Sprintf("%t", aws.BoolValue(req.NoDirect)))
	}
	return v.Encode()
}

func (resp *CreateResponse) UnmarshalHTTP(httpResp *http.Response) error {
	resp.HttpResponse.UnmarshalHTTP(httpResp)

	if isSuccessHttpCode(httpResp.StatusCode) {
		return nil
	}
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

// Create and Write to a File
// If no permissions are specified, the newly created file will be assigned with default 644 permission.
// No umask mode will be applied from server side (so “fs.permissions.umask-mode” value configuration set on Namenode side will have no effect).
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Create_and_Write_to_a_File
func (c *Client) Create(req *CreateRequest) (*CreateResponse, error) {
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

		req, err := http.NewRequest(http.MethodPut, u.String(), req.Body)
		if err != nil {
			return nil, err
		}

		httpResp, err := c.httpClient.Do(req)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		var resp CreateResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}

		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
