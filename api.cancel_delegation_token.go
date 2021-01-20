package webhdfs

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/searKing/golang/go/errors"
)

type CancelDelegationTokenRequest struct {
	Authentication
	ProxyUser
	CSRF

	// Name				token
	// Description		The delegation token used for the operation.
	// Type				String
	// Default Value	<empty>
	// Valid Values		An encoded token.
	// Syntax			See the note in Delegation.
	Token *string `json:"token,omitempty"`
}

type CancelDelegationTokenResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`
}

func (req *CancelDelegationTokenRequest) RawPath() string {
	return ""
}
func (req *CancelDelegationTokenRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpCancelDelegationToken)
	if req.Authentication.Delegation != nil {
		v.Set("delegation", aws.StringValue(req.Authentication.Delegation))
	}
	if req.ProxyUser.Username != nil {
		v.Set("user.name", aws.StringValue(req.ProxyUser.Username))
	}
	if req.ProxyUser.DoAs != nil {
		v.Set("doas", aws.StringValue(req.ProxyUser.DoAs))
	}

	if req.Token != nil {
		v.Set("token", aws.StringValue(req.Token))
	}
	return v.Encode()
}

func (resp *CancelDelegationTokenResponse) UnmarshalHTTP(httpResp *http.Response) error {
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

// Renew Delegation Token
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Renew_Delegation_Token
// expire time set by server "dfs.namenode.delegation.token.max-lifetime"
// See: https://hadoop.apache.org/docs/r2.7.1/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml#dfs.namenode.delegation.token.max-lifetime
func (c *Client) CancelDelegationToken(req *CancelDelegationTokenRequest) (*CancelDelegationTokenResponse, error) {
	return c.cancelDelegationToken(nil, req)
}

func (c *Client) CancelDelegationTokenWithContext(ctx context.Context, req *CancelDelegationTokenRequest) (*CancelDelegationTokenResponse, error) {
	if ctx == nil {
		panic("nil context")
	}
	return c.cancelDelegationToken(ctx, req)
}

func (c *Client) cancelDelegationToken(ctx context.Context, req *CancelDelegationTokenRequest) (*CancelDelegationTokenResponse, error) {
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

		var resp CancelDelegationTokenResponse
		resp.NameNode = addr

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}
		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}
