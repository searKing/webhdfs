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

	"github.com/searKing/golang/go/exp/types"

	strings_ "github.com/searKing/golang/go/strings"

	"github.com/searKing/golang/go/errors"
)

type RenameRequest struct {
	Authentication
	ProxyUser
	CSRF
	HttpRequest

	// Path of the object to get.
	//
	// Path is a required field
	Path *string `validate:"required"`

	// Name				destination
	// Description		The destination path.
	// Type				Path
	// Default Value	<empty> (an invalid path)
	// Valid Values		An absolute FileSystem path without scheme and authority.
	// Syntax			Any path.
	Destination *string `validate:"required"`
}

type RenameResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`
	Boolean      Boolean `json:"boolean"`
}

func (req *RenameRequest) RawPath() string {
	return types.Value(req.Path)
}
func (req *RenameRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpRename)
	if req.Authentication.Delegation != nil {
		v.Set("delegation", types.Value(req.Authentication.Delegation))
	}
	if req.ProxyUser.Username != nil {
		v.Set("user.name", types.Value(req.ProxyUser.Username))
	}
	if req.ProxyUser.DoAs != nil {
		v.Set("doas", types.Value(req.ProxyUser.DoAs))
	}

	if req.Destination != nil {
		v.Set("destination", fmt.Sprintf("%s", types.Value(req.Destination)))
	}
	return v.Encode()
}

func (resp *RenameResponse) UnmarshalHTTP(httpResp *http.Response) error {
	resp.HttpResponse.UnmarshalHTTP(httpResp)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return ErrorFromHttpResponse(httpResp)
	}
	err = json.Unmarshal(body, &resp)
	if err != nil {
		return fmt.Errorf("parse %s: %w", strings_.Truncate(string(body), MaxHTTPBodyLengthDumped), err)
	}
	if err := resp.Exception(); err != nil {
		return err
	}
	return nil
}

// Rename a File/Directory
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Rename_a_File.2FDirectory
func (c *Client) Rename(req *RenameRequest) (*RenameResponse, error) {
	return c.rename(nil, req)
}
func (c *Client) RenameWithContext(ctx context.Context, req *RenameRequest) (*RenameResponse, error) {
	if ctx == nil {
		panic("nil context")
	}
	return c.rename(ctx, req)
}
func (c *Client) rename(ctx context.Context, req *RenameRequest) (*RenameResponse, error) {
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

		httpReq, err := http.NewRequest(http.MethodPut, u.String(), nil)
		if err != nil {
			return nil, err
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

		var resp RenameResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}

		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
