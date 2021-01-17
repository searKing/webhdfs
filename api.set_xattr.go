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

type SetXAttrRequest struct {
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
	return aws.StringValue(req.Path)
}
func (req *SetXAttrRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpSetXAttr)
	if req.XAttrName != nil {
		v.Set("xattr.name", aws.StringValue(req.XAttrName))
	}
	if req.XAttrValue != nil {
		v.Set("xattr.value", aws.StringValue(req.XAttrValue))
	}
	if req.XAttrFlag != nil {
		v.Set("flag", aws.StringValue((*string)(req.XAttrFlag)))
	}
	return v.Encode()
}

func (resp *SetXAttrResponse) UnmarshalHTTP(httpResp *http.Response) error {
	resp.HttpResponse.UnmarshalHTTP(httpResp)
	defer resp.Body.Close()
	if isSuccessHttpCode(httpResp.StatusCode) {
		return nil
	}
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

// Renew Delegation Token
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Renew_Delegation_Token
// expire time set by server "dfs.namenode.delegation.token.max-lifetime"
// See: https://hadoop.apache.org/docs/r2.7.1/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml#dfs.namenode.delegation.token.max-lifetime
func (c *Client) SetXAttr(req *SetXAttrRequest) (*SetXAttrResponse, error) {
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

		req, err := http.NewRequest(http.MethodPut, u.String(), nil)
		if err != nil {
			return nil, err
		}

		httpResp, err := c.httpClient.Do(req)
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
