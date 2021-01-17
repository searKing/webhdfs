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

type RenewDelegationTokenRequest struct {
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
	Long         Long `json:"long"`
}

func (req *RenewDelegationTokenRequest) RawPath() string {
	return ""
}
func (req *RenewDelegationTokenRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpRenewDelegationToken)
	if req.Token != nil {
		v.Set("token", aws.StringValue(req.Token))
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
