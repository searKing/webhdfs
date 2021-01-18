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

type GetSnapshotDiffRequest struct {
	Authentication
	ProxyUser
	CSRF

	// Name				oldsnapshotname
	// Description		The old name of the snapshot to be renamed.
	// Type				String
	// Default Value	null
	// Valid Values		An existing snapshot name.
	// Syntax			Any string.
	// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Old_Snapshot_Name
	Oldsnapshotname *string `json:"oldsnapshotname,omitempty" validate:"required"`
	// Name				snapshotname
	// Description		The name of the snapshot to be created/deleted. Or the new name for snapshot rename.
	// Type				String
	// Default Value	null
	// Valid Values		Any valid snapshot name.
	// Syntax			Any string.
	// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Snapshot_Name
	Snapshotname *string `json:"snapshotname,omitempty" validate:"required"`
}

type GetSnapshotDiffResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse       `json:"-"`
	SnapshotDiffReport SnapshotDiffReport `json:"SnapshotDiffReport"`
}

func (req *GetSnapshotDiffRequest) RawPath() string {
	return ""
}
func (req *GetSnapshotDiffRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpGetSnapshotDiff)
	if req.Authentication.Delegation != nil {
		v.Set("delegation", aws.StringValue(req.Authentication.Delegation))
	}
	if req.ProxyUser.Username != nil {
		v.Set("user.name", aws.StringValue(req.ProxyUser.Username))
	}
	if req.ProxyUser.DoAs != nil {
		v.Set("doas", aws.StringValue(req.ProxyUser.DoAs))
	}

	if req.Oldsnapshotname != nil {
		v.Set("oldsnapshotname", aws.StringValue(req.Oldsnapshotname))
	}
	if req.Snapshotname != nil {
		v.Set("snapshotname", aws.StringValue(req.Snapshotname))
	}
	return v.Encode()
}

func (resp *GetSnapshotDiffResponse) UnmarshalHTTP(httpResp *http.Response) error {
	resp.HttpResponse.UnmarshalHTTP(httpResp)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return nil
	}
	if err = json.Unmarshal(body, &resp); err != nil {
		return err
	}

	if err := resp.Exception(); err != nil {
		return err
	}
	return nil
}

// Get Snapshot Diff
// See also: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Get_Snapshot_Diff
func (c *Client) GetSnapshotDiff(req *GetSnapshotDiffRequest) (*GetSnapshotDiffResponse, error) {
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

		var resp GetSnapshotDiffResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}
		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
