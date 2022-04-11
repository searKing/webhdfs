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
	strings_ "github.com/searKing/golang/go/strings"
)

type GetAllStoragePolicyRequest struct {
	Authentication
	ProxyUser
	CSRF
	HttpRequest
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
	if req.Authentication.Delegation != nil {
		v.Set("delegation", types.Value(req.Authentication.Delegation))
	}
	if req.ProxyUser.Username != nil {
		v.Set("user.name", types.Value(req.ProxyUser.Username))
	}
	if req.ProxyUser.DoAs != nil {
		v.Set("doas", types.Value(req.ProxyUser.DoAs))
	}

	return v.Encode()
}

func (resp *GetAllStoragePolicyResponse) UnmarshalHTTP(httpResp *http.Response) error {
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

// Get all Storage Policies
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Get_all_Storage_Policies
func (c *Client) GetAllStoragePolicy(req *GetAllStoragePolicyRequest) (*GetAllStoragePolicyResponse, error) {
	return c.getAllStoragePolicy(nil, req)
}
func (c *Client) GetAllStoragePolicyWithContext(ctx context.Context, req *GetAllStoragePolicyRequest) (*GetAllStoragePolicyResponse, error) {
	if ctx == nil {
		panic("nil context")
	}
	return c.getAllStoragePolicy(ctx, req)
}
func (c *Client) getAllStoragePolicy(ctx context.Context, req *GetAllStoragePolicyRequest) (*GetAllStoragePolicyResponse, error) {
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
