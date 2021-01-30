package webhdfs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/aws/aws-sdk-go/aws"

	http_ "github.com/searKing/golang/go/net/http"
	strings_ "github.com/searKing/golang/go/strings"

	"github.com/searKing/golang/go/errors"
)

type CreateRequest struct {
	Authentication
	ProxyUser
	CSRF

	// Path of the object to get.
	//
	// Path is a required field
	Path *string `validate:"required"`

	// Object data.
	Body io.Reader
	// ContentLength records the length of the associated content.
	// The value -1 indicates that the length is unknown.
	// Values >= 0 indicate that the given number of bytes may
	// be read from Body.
	//
	// For client requests, a value of 0 with a non-nil Body is
	// also treated as unknown.
	// For HttpFs, ContentType and ContentLength is needed with a non-nil Body
	// ContentLength will be computed inner when Body is one of type [*bytes.Buffer, *bytes.Reader, *strings.Reader]
	// See https://issues.cloudera.org/browse/HUE-679
	// Missing or unknown request method
	// Missing URL
	// Missing HTTP Identifier (HTTP/1.0)
	// Request is too large
	// Content-Length missing for POST or PUT requests
	// Illegal character in hostname; underscores are not allowed
	ContentLength *int64

	// Name				overwrite
	// Description		If a file already exists, should it be overwritten?
	// Type				boolean
	// Default Value	false
	// Valid Values		true
	// Syntax			true
	// Actually, HttpFS default to true
	Overwrite *bool
	// Name				blocksize
	// Description		The block size of a file.
	// Type				long
	// Default Value	Specified in the configuration.
	// Valid Values		> 0
	// Syntax			Any integer.
	Blocksize *int64
	// Name				replication
	// Description		The number of replications of a file.
	// Type				short
	// Default Value	Specified in the configuration.
	// Valid Values		> 0
	// Syntax			Any integer.
	Replication *int
	// Name	permission
	// Description		The permission of a file/directory.
	// Type	Octal
	// Default Value	644 for files, 755 for directories
	// Valid Values		0 - 1777
	// Syntax			Any radix-8 integer (leading zeros may be omitted.)
	Permission *int
	// Name				buffersize
	// Description		The size of the buffer used in transferring data.
	// Type				int
	// Default Value	Specified in the configuration.
	// Valid Values		> 0
	// Syntax			Any integer.
	BufferSize *int
	// Name				nodirect
	// Description		Disable automatically redirected.
	// Type				bool
	// Default Value	false
	// Valid Values		true|false
	// Syntax			Any Bool.
	NoDirect *bool
}

type CreateResponse struct {
	NameNode string `json:"-"`
	ErrorResponse
	HttpResponse `json:"-"`

	NoDirect bool    `json:"-"`
	Location *string `json:"Location"`
}

func (req *CreateRequest) RawPath() string {
	return aws.StringValue(req.Path)
}
func (req *CreateRequest) RawQuery() string {
	v := url.Values{}
	v.Set("op", OpCreate)
	if req.Authentication.Delegation != nil {
		v.Set("delegation", aws.StringValue(req.Authentication.Delegation))
	}
	if req.ProxyUser.Username != nil {
		v.Set("user.name", aws.StringValue(req.ProxyUser.Username))
	}
	if req.ProxyUser.DoAs != nil {
		v.Set("doas", aws.StringValue(req.ProxyUser.DoAs))
	}

	if req.Overwrite != nil {
		v.Set("overwrite", fmt.Sprintf("%t", aws.BoolValue(req.Overwrite)))
	}
	if req.Blocksize != nil {
		v.Set("blocksize", fmt.Sprintf("%d", aws.Int64Value(req.Blocksize)))
	}
	if req.Replication != nil {
		v.Set("replication", fmt.Sprintf("%d", aws.IntValue(req.Replication)))
	}
	if req.Permission != nil {
		v.Set("permission", fmt.Sprintf("%#o", aws.IntValue(req.Permission)))
	}
	if req.BufferSize != nil {
		v.Set("buffersize", fmt.Sprintf("%d", aws.IntValue(req.BufferSize)))
	}
	if req.NoDirect != nil {
		v.Set("noredirect", fmt.Sprintf("%t", aws.BoolValue(req.NoDirect)))
	}
	return v.Encode()
}

func (resp *CreateResponse) UnmarshalHTTP(httpResp *http.Response) error {
	resp.HttpResponse.UnmarshalHTTP(httpResp)

	if isSuccessHttpCode(httpResp.StatusCode) && !resp.NoDirect {
		// HttpFS always returns a redirected url in json
		// {"Location":"http://<DATANODE>:<PORT>/webhdfs/v1/<PATH>?op=CREATE..."}
		if httpResp.ContentLength == 0 {
			return nil
		}
	}
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

// Create and Write to a File
// If no permissions are specified, the newly created file will be assigned with default 644 permission.
// No umask mode will be applied from server side (so “fs.permissions.umask-mode” value configuration set on Namenode side will have no effect).
// Parent Dirs will be created automatically.
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Create_and_Write_to_a_File
func (c *Client) Create(req *CreateRequest) (*CreateResponse, error) {
	return c.create(nil, req)
}
func (c *Client) CreateWithContext(ctx context.Context, req *CreateRequest) (*CreateResponse, error) {
	if ctx == nil {
		panic("nil context")
	}
	return c.create(ctx, req)
}
func (c *Client) create(ctx context.Context, req *CreateRequest) (*CreateResponse, error) {
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

		httpReq, err := http.NewRequest(http.MethodPut, u.String(), req.Body)
		if err != nil {
			return nil, err
		}
		_ = http_.RequestWithBodyRewindable(httpReq)
		if req.CSRF.XXsrfHeader != nil {
			httpReq.Header.Set("X-XSRF-HEADER", aws.StringValue(req.CSRF.XXsrfHeader))
		}

		// See :https://issues.cloudera.org/browse/HUE-679
		httpReq.Header.Set("Content-Type", "application/octet-stream")
		if req.ContentLength != nil {
			httpReq.ContentLength = aws.Int64Value(req.ContentLength)
		}

		if ctx != nil {
			httpReq = httpReq.WithContext(ctx)
		}
		httpResp, err := c.httpClient().Do(httpReq)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		var resp CreateResponse
		resp.NameNode = addr
		resp.NoDirect = aws.BoolValue(req.NoDirect)

		if err := resp.UnmarshalHTTP(httpResp); err != nil {
			errs = append(errs, err)
			continue
		}

		return &resp, nil
	}
	return nil, errors.Multi(errs...)
}

type teeReadCloser struct {
	io.Reader
	io.Closer
}
