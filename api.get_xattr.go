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

type GetXAttrRequest struct {
	Authentication
	ProxyUser
	CSRF
	HttpRequest

	// Path of the object to get.
	//
	// Path is a required field
	Path *string `validate:"required"`

	// XAttr 			Name
	// Name				xattr.name
	// Description		The XAttr name of a file/directory.
	// Type				String
	// Default Value	<empty>
	// Valid Values		Any string prefixed with user./trusted./system./security..
	// Syntax			Any string prefixed with user./trusted./system./security..
	XAttrName *string `validate:"required"`

	// Encode values after retrieving them.
	// Valid encodings are “text”, “hex”, and “base64”.
	// Values encoded as text strings are enclosed in double quotes ("),
	// and values encoded as hexadecimal and base64 are prefixed with 0x and 0s, respectively.
	// actually, encoding is not required, so nil is allowed.
	Encoding *XAttrValueEncoding //`validate:"required"`
}

type GetXAttrResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`
	XAttrs       `json:"XAttrs"` // XAttr array.
}

func (req *GetXAttrRequest) RawPath() string {
	return types.Value(req.Path)
}
func (req *GetXAttrRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpGetXAttr)
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
		v.Set(HttpQueryParamKeyXAttrName, types.Value(req.XAttrName))
	}
	if req.Encoding != nil {
		v.Set(HttpQueryParamKeyXAttrValueEncoding, types.Value((*string)(req.Encoding)))
	}
	return v.Encode()
}

func (resp *GetXAttrResponse) UnmarshalHTTP(httpResp *http.Response) error {
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

// Get an XAttr
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Get_an_XAttr
func (c *Client) GetXAttr(req *GetXAttrRequest) (*GetXAttrResponse, error) {
	return c.getXAttr(nil, req)
}
func (c *Client) GetXAttrWithContext(ctx context.Context, req *GetXAttrRequest) (*GetXAttrResponse, error) {
	if ctx == nil {
		panic("nil context")
	}
	return c.getXAttr(ctx, req)
}
func (c *Client) getXAttr(ctx context.Context, req *GetXAttrRequest) (*GetXAttrResponse, error) {
	err := c.opts.Validator.Struct(req)
	if err != nil {
		return nil, err
	}
	if req.Encoding != nil {
		return nil, fmt.Errorf("unknown param %s : %s", HttpQueryParamKeyXAttrValueEncoding, types.Value((*string)(req.Encoding)))
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

		var resp GetXAttrResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}

		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
