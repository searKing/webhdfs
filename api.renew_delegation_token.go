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

	"github.com/searKing/golang/go/errors"
	time_ "github.com/searKing/golang/go/time"
)

type RenewDelegationTokenRequest struct {
	Authentication
	ProxyUser
	CSRF
	HttpRequest

	// Name				token
	// Description		The delegation token used for the operation.
	// Type				String
	// Default Value	<empty>
	// Valid Values		An encoded token.
	// Syntax			See the note in Delegation.
	Token *string `json:"token,omitempty"`
}

type RenewDelegationTokenResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`
	//Long         Long `json:"long"`
	Long time_.UnixTimeMillisecond `json:"long"`
}

func (req *RenewDelegationTokenRequest) RawPath() string {
	return ""
}
func (req *RenewDelegationTokenRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpRenewDelegationToken)
	if req.Authentication.Delegation != nil {
		v.Set("delegation", types.Value(req.Authentication.Delegation))
	}
	if req.ProxyUser.Username != nil {
		v.Set("user.name", types.Value(req.ProxyUser.Username))
	}
	if req.ProxyUser.DoAs != nil {
		v.Set("doas", types.Value(req.ProxyUser.DoAs))
	}

	if req.Token != nil {
		v.Set("token", types.Value(req.Token))
	}
	return v.Encode()
}

func (resp *RenewDelegationTokenResponse) UnmarshalHTTP(httpResp *http.Response) error {
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

// Renew Delegation Token
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Renew_Delegation_Token
// expire time set by server "dfs.namenode.delegation.token.max-lifetime"
// See: https://hadoop.apache.org/docs/r2.7.1/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml#dfs.namenode.delegation.token.max-lifetime
func (c *Client) RenewDelegationToken(req *RenewDelegationTokenRequest) (*RenewDelegationTokenResponse, error) {
	return c.renewDelegationToken(nil, req)
}
func (c *Client) RenewDelegationTokenWithContext(ctx context.Context, req *RenewDelegationTokenRequest) (*RenewDelegationTokenResponse, error) {
	if ctx == nil {
		panic("nil context")
	}
	return c.renewDelegationToken(ctx, req)
}
func (c *Client) renewDelegationToken(ctx context.Context, req *RenewDelegationTokenRequest) (*RenewDelegationTokenResponse, error) {
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

		var resp RenewDelegationTokenResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}
		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
