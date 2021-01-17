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

type GetXAttrRequest struct {
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
	Encoding *XAttrValueEncoding `validate:"required"`
}

type GetXAttrResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`
	XAttrs
}

func (req *GetXAttrRequest) RawPath() string {
	return aws.StringValue(req.Path)
}
func (req *GetXAttrRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpGetXAttr)
	if req.XAttrName != nil {
		v.Set(HttpQueryParamKeyXAttrName, aws.StringValue(req.XAttrName))
	}
	if req.Encoding != nil {
		v.Set(HttpQueryParamKeyXAttrValueEncoding, aws.StringValue((*string)(req.Encoding)))
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

// Get an XAttr
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Get_an_XAttr
func (c *Client) GetXAttr(req *GetXAttrRequest) (*GetXAttrResponse, error) {
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
		httpResp, err := c.httpClient.Get(u.String())
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
