// Copyright 2022 The searKing Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package webhdfs

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/searKing/golang/go/errors"
	"github.com/searKing/golang/go/exp/types"
)

type GetSnapshotDiffRequest struct {
	Authentication
	ProxyUser
	CSRF
	HttpRequest

	// Path of the object to get.
	//
	// Path is a required field
	Path *string `validate:"required"`

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
	return types.Value(req.Path)
}
func (req *GetSnapshotDiffRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpGetSnapshotDiff)
	if req.Authentication.Delegation != nil {
		v.Set("delegation", types.Value(req.Authentication.Delegation))
	}
	if req.ProxyUser.Username != nil {
		v.Set("user.name", types.Value(req.ProxyUser.Username))
	}
	if req.ProxyUser.DoAs != nil {
		v.Set("doas", types.Value(req.ProxyUser.DoAs))
	}

	if req.Oldsnapshotname != nil {
		v.Set("oldsnapshotname", types.Value(req.Oldsnapshotname))
	}
	if req.Snapshotname != nil {
		v.Set("snapshotname", types.Value(req.Snapshotname))
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
		return ErrorFromHttpResponse(httpResp)
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
	return c.getSnapshotDiff(nil, req)
}
func (c *Client) GetSnapshotDiffWithContext(ctx context.Context, req *GetSnapshotDiffRequest) (*GetSnapshotDiffResponse, error) {
	if ctx == nil {
		panic("nil context")
	}
	return c.getSnapshotDiff(ctx, req)
}
func (c *Client) getSnapshotDiff(ctx context.Context, req *GetSnapshotDiffRequest) (*GetSnapshotDiffResponse, error) {
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
		httpReq.Close = req.HttpRequest.Close
		if req.CSRF.XXsrfHeader != nil {
			httpReq.Header.Set("X-XSRF-HEADER", types.Value(req.CSRF.XXsrfHeader))
		}
		if ctx != nil {
			httpReq = httpReq.WithContext(ctx)
		}
		if req.HttpRequest.PreSendHandler != nil {
			httpReq, err = req.HttpRequest.PreSendHandler(httpReq)
			if err != nil {
				return nil, fmt.Errorf("pre send handled: %w", err)
			}
		}

		httpResp, err := c.httpClient().Do(httpReq)
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
