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
)

type SetXAttrRequest struct {
	Authentication
	ProxyUser
	CSRF
	HttpRequest

	// Path of the object to get.
	//
	// Path is a required field
	Path *string `validate:"required"`

	// Name				xattr.name
	// Description		The XAttr name of a file/directory.
	// Type				String
	// Default Value	<empty>
	// Valid Values		Any string prefixed with user./trusted./system./security..
	// Syntax			Any string prefixed with user./trusted./system./security..
	XAttrName *string `validate:"required"`
	// Name				xattr.value
	// Description		The XAttr value of a file/directory.
	// Type				String
	// Default Value	<empty>
	// Valid Values		An encoded value.
	// Syntax			Enclosed in double quotes or prefixed with 0x or 0s.
	XAttrValue *string `validate:"required"`
	// Name	flag
	// Description	The XAttr set flag.
	// Type	String
	// Default Value	<empty>
	// Valid Values	CREATE,REPLACE.
	// Syntax	CREATE,REPLACE.
	XAttrFlag *XAttrSetFlag `validate:"required"`
}

type SetXAttrResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`
}

func (req *SetXAttrRequest) RawPath() string {
	return types.Value(req.Path)
}
func (req *SetXAttrRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpSetXAttr)
	if req.Authentication.Delegation != nil {
		v.Set("delegation", types.Value(req.Authentication.Delegation))
	}
	if req.ProxyUser.Username != nil {
		v.Set("user.name", types.Value(req.ProxyUser.Username))
	}
	if req.ProxyUser.DoAs != nil {
		v.Set("doas", types.Value(req.ProxyUser.DoAs))
	}

	if req.XAttrName != nil {
		v.Set("xattr.name", types.Value(req.XAttrName))
	}
	if req.XAttrValue != nil {
		v.Set("xattr.value", types.Value(req.XAttrValue))
	}
	if req.XAttrFlag != nil {
		v.Set("flag", types.Value((*string)(req.XAttrFlag)))
	}
	return v.Encode()
}

func (resp *SetXAttrResponse) UnmarshalHTTP(httpResp *http.Response) error {
	resp.HttpResponse.UnmarshalHTTP(httpResp)
	if isSuccessHttpCode(httpResp.StatusCode) {
		return nil
	}
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

// Set XAttr
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Set_XAttr
func (c *Client) SetXAttr(req *SetXAttrRequest) (*SetXAttrResponse, error) {
	return c.setXAttr(nil, req)
}
func (c *Client) SetXAttrWithContext(ctx context.Context, req *SetXAttrRequest) (*SetXAttrResponse, error) {
	if ctx == nil {
		panic("nil context")
	}
	return c.setXAttr(ctx, req)
}
func (c *Client) setXAttr(ctx context.Context, req *SetXAttrRequest) (*SetXAttrResponse, error) {
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

		var resp SetXAttrResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}
		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
