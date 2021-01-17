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

// See also: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Get_Delegation_Token
type GetDelegationTokenRequest struct {
	// Name				renewer
	// Description		The username of the renewer of a delegation token.
	// Type				String
	// Default Value	<empty> (means the current user)
	// Valid Values		Any valid username.
	// Syntax			Any string.
	Renewer *string `json:"renewer,omitempty"`
	// Name				service
	// Description		The name of the service where the token is supposed to be used, e.g. ip:port of the namenode
	// Type				String
	// Default Value	<empty>
	// Valid Values		ip:port in string format or logical name of the service
	// Syntax			Any string.
	Service *string `json:"service,omitempty"`
	// Name				kind
	// Description		The kind of the delegation token requested
	// Type				String
	// Default Value	<empty> (Server sets the default kind for the service)
	// Valid Values		A string that represents token kind e.g “HDFS_DELEGATION_TOKEN” or “WEBHDFS delegation”
	// Syntax			Any string.
	Kind *string `json:"kind,omitempty"`
}

type GetDelegationTokenResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`
	Token        Token `json:"token"`
}

func (req *GetDelegationTokenRequest) RawPath() string {
	return ""
}
func (req *GetDelegationTokenRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpGetDelegationToken)
	if req.Renewer != nil {
		v.Set("renewer", aws.StringValue(req.Renewer))
	}
	if req.Service != nil {
		v.Set("service", aws.StringValue(req.Service))
	}
	if req.Kind != nil {
		v.Set("kind", aws.StringValue(req.Kind))
	}
	return v.Encode()
}

func (resp *GetDelegationTokenResponse) UnmarshalHTTP(httpResp *http.Response) error {
	resp.HttpResponse.UnmarshalHTTP(httpResp)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return nil
	}
	if err = json.Unmarshal(body, &resp); err != nil {
		return err
	}

	if err := resp.Exception(); err != nil {
		return err
	}
	return nil
}

// Get Delegation Token
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Get_Delegation_Token
// expire time set by server "dfs.namenode.delegation.token.max-lifetime"
// See: https://hadoop.apache.org/docs/r2.7.1/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml#dfs.namenode.delegation.token.max-lifetime
func (c *Client) GetDelegationToken(req *GetDelegationTokenRequest) (*GetDelegationTokenResponse, error) {
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

		var resp GetDelegationTokenResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}
		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
