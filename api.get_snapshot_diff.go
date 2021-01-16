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
		httpResp, err := c.httpClient.Get(u.String())
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
