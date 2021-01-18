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

type ListStatusBatchRequest struct {
	ProxyUser
	CSRF

	// Path of the object to get.
	//
	// Path is a required field
	Path *string `validate:"required"`

	// The pathSuffix of the last item returned in the current batch.
	StartAfter *string
}

type ListStatusBatchResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse     `json:"-"`
	DirectoryListing DirectoryListing `json:"directoryListing"`
}

func (req *ListStatusBatchRequest) RawPath() string {
	return aws.StringValue(req.Path)
}
func (req *ListStatusBatchRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpListStatusBatch)
	if req.ProxyUser.Username != nil {
		v.Set("user.name", aws.StringValue(req.ProxyUser.Username))
	}
	if req.ProxyUser.DoAs != nil {
		v.Set("doas", aws.StringValue(req.ProxyUser.DoAs))
	}
	if req.ProxyUser.Delegation != nil {
		v.Set("delegation", aws.StringValue(req.ProxyUser.Delegation))
	}

	return v.Encode()
}

func (resp *ListStatusBatchResponse) UnmarshalHTTP(httpResp *http.Response) error {
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

// Iteratively List a Directory
// https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Iteratively_List_a_Directory
// If remainingEntries is non-zero, there are additional entries in the directory.
// To query the next batch, set the startAfter parameter to the pathSuffix of the last item returned in the current batch.
// Batch size is controlled by the dfs.ls.limit option on the NameNode.
func (c *Client) ListStatusBatch(req *ListStatusBatchRequest) (*ListStatusBatchResponse, error) {
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

		var resp ListStatusBatchResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}

		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
