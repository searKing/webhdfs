package webhdfs

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/aws/aws-sdk-go/aws"

	strings_ "github.com/searKing/golang/go/strings"

	"github.com/searKing/golang/go/errors"
)

type GetXAttrsRequest struct {
	Authentication
	ProxyUser
	CSRF

	// Path of the object to get.
	//
	// Path is a required field
	Path *string `validate:"required"`

	XAttrNames []string `validate:"required"`

	// Encode values after retrieving them.
	// Valid encodings are “text”, “hex”, and “base64”.
	// Values encoded as text strings are enclosed in double quotes ("),
	// and values encoded as hexadecimal and base64 are prefixed with 0x and 0s, respectively.
	// actually, encoding is not required, so nil is allowed.
	Encoding *XAttrValueEncoding //`validate:"required"`
}

type GetXAttrsResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`
	XAttrs       `json:"XAttrs"` // XAttr array.
}

func (req *GetXAttrsRequest) RawPath() string {
	return aws.StringValue(req.Path)
}
func (req *GetXAttrsRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpGetXAttrs)
	if req.Authentication.Delegation != nil {
		v.Set("delegation", aws.StringValue(req.Authentication.Delegation))
	}
	if req.ProxyUser.Username != nil {
		v.Set("user.name", aws.StringValue(req.ProxyUser.Username))
	}
	if req.ProxyUser.DoAs != nil {
		v.Set("doas", aws.StringValue(req.ProxyUser.DoAs))
	}

	for _, name := range req.XAttrNames {
		v.Add(HttpQueryParamKeyXAttrName, name)
	}
	if req.Encoding != nil {
		v.Set(HttpQueryParamKeyXAttrValueEncoding, aws.StringValue((*string)(req.Encoding)))
	}
	return v.Encode()
}

func (resp *GetXAttrsResponse) UnmarshalHTTP(httpResp *http.Response) error {
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

// Get multiple XAttrs
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Get_multiple_XAttrs
func (c *Client) GetXAttrs(req *GetXAttrsRequest) (*GetXAttrsResponse, error) {
	return c.getXAttrs(nil, req)
}
func (c *Client) GetXAttrsWithContext(ctx context.Context, req *GetXAttrsRequest) (*GetXAttrsResponse, error) {
	if ctx == nil {
		panic("nil context")
	}
	return c.getXAttrs(ctx, req)
}
func (c *Client) getXAttrs(ctx context.Context, req *GetXAttrsRequest) (*GetXAttrsResponse, error) {
	err := c.opts.Validator.Struct(req)
	if err != nil {
		return nil, err
	}
	if req.Encoding != nil {
		return nil, fmt.Errorf("unknown param %s : %s", HttpQueryParamKeyXAttrValueEncoding, aws.StringValue((*string)(req.Encoding)))
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
		if ctx != nil {
			httpReq = httpReq.WithContext(ctx)
		}
		httpResp, err := c.httpClient().Do(httpReq)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		var resp GetXAttrsResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}

		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
