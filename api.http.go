package webhdfs

import (
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
)

type HttpResponse struct {
	// Indicates that a range of bytes was specified.
	AcceptRanges *string

	// Object data.
	// We guarantee that Body is always non-nil, even on responses without a body or responses with
	// a zero-length body. It is the caller's responsibility to close Body.
	Body io.ReadCloser

	// Specifies caching behavior along the request/reply chain.
	CacheControl *string

	// Specifies presentational information for the object.
	ContentDisposition *string

	// Specifies what content encodings have been applied to the object and thus
	// what decoding mechanisms must be applied to obtain the media-type referenced
	// by the Content-Type header field.
	ContentEncoding *string

	// The language the content is in.
	ContentLanguage *string

	// Size of the body in bytes.
	ContentLength *int64

	// The portion of the object returned in the response.
	ContentRange *string

	// A standard MIME type describing the format of the object data.
	ContentType *string

	// An ETag is an opaque identifier assigned by a web server to a specific version
	// of a resource found at a URL.
	ETag *string

	// The date and time at which the object is no longer cacheable.
	Expires *string `location:"header" locationName:"Expires" type:"string"`

	// Last modified date of the object
	LastModified *time.Time `location:"header" locationName:"Last-Modified" type:"timestamp"`
}

func (resp *HttpResponse) UnmarshalHTTP(httpResp *http.Response) {
	resp.ContentLength = aws.Int64(httpResp.ContentLength)
	{
		ct := httpResp.Header.Get("Content-Type")
		if ct != "" {
			resp.ContentType = aws.String(httpResp.Header.Get("Content-Type"))
		}
	}

	resp.Body = httpResp.Body
	httpResp.Body = http.NoBody
	return
}
