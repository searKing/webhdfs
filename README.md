# webhdfs

Hadoop WebHDFS REST API([2.4.1](https://hadoop.apache.org/docs/r2.4.1/hadoop-project-dist/hadoop-hdfs/WebHDFS.html))
client library for Golang with S3-like interface.
HttpFS HTTP web-service API([2.4.1(https://hadoop.apache.org/docs/r2.4.1/hadoop-hdfs-httpfs/index.html)]) calls are HTTP REST calls that map to a HDFS file system operation. 

## Examples

+ More examples can be found in [client_test.go](https://github.com/searKing/webhdfs/blob/main/client_test.go).
+ Write the file to HDFS and read the file back:

```go
package main

import (
	"io/ioutil"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/searKing/webhdfs"
)

const (
	webHdfsEndpoint              = "quickstart.cloudera:50070"
	KerberosRealm                = "CLOUDERA"
	KerberosUsername             = "hdfs/quickstart.cloudera"
	KerberosServicePrincipleName = "HTTP/quickstart.cloudera"
	KerberosConfigFile           = "/etc/krb5.conf"
	// pick one auth way by 3 ways below, if all below are empty, kerberos is disabled.
	KerberosPassword   = ""
	KerberosKeyTabFile = "/hdfs.keytab"  // /krb5.keytab
	KerberosCCacheFile = "/tmp/krb5cc_0" // /tmp/krb5cc_0
)

func getClient() *webhdfs.Client {
	c, err := webhdfs.New(webHdfsEndpoint, webhdfs.WithDisableSSL(true),
		webhdfs.WithKerberosKeytabFile(KerberosUsername, KerberosServicePrincipleName, KerberosRealm, KerberosKeyTabFile, KerberosConfigFile))
	if err != nil {
		log.Fatalf("create client %s", err)
	}
	return c
}

func main() {
	file := "/data/test/sample.txt"
	{ // upload from hdfs
		resp, err := getClient().Create(&webhdfs.CreateRequest{
			Path:      aws.String(file),
			Body:      strings.NewReader("test_input"),
			Overwrite: aws.Bool(true),
		})
		if err != nil {
			log.Fatalf("webhdfs Open failed: %s", err)
		}
		defer resp.Body.Close()
	}

	{ // download from hdfs
		resp, err := getClient().Open(&webhdfs.OpenRequest{
			Path: aws.String(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Open failed: %s", err)
		}
		defer resp.Body.Close()

		content, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Fatalf("webhdfs Read failed: %s", err)
		}
		log.Printf("content: %s", string(content))

	}
}
```

## TODO

* Improve documentation
* More examples
