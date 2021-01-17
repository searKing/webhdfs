package webhdfs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/searKing/webhdfs"
)

const (
	webHdfsEndpoint              = "quickstart.cloudera:50070"
	KerberosRealm                = "CLOUDERA"
	KerberosUsername             = "hdfs/quickstart.cloudera"
	KerberosServicePrincipleName = "HTTP/quickstart.cloudera"
	KerberosPassword             = ""
	KerberosKeyTabFile           = "internal/hdfs.keytab"   // /krb5.keytab
	KerberosCCacheFile           = "internal/tmp/krb5cc_0"  // /tmp/krb5cc_0
	KerberosConfigFile           = "internal/etc/krb5.conf" // /etc/krb5.conf, /var/kerberos/krb5kdc/kdc.conf
)

func getClient(t *testing.T) *webhdfs.Client {
	c, err := webhdfs.New(webHdfsEndpoint, webhdfs.WithDisableSSL(true),
		webhdfs.WithKerberosKeytabFile(KerberosUsername, KerberosServicePrincipleName, KerberosRealm, KerberosKeyTabFile, KerberosConfigFile))
	if err != nil {
		t.Fatalf("create client %s", err)
	}
	return c
}

func TestClient_GetDelegationToken(t *testing.T) {
	resp, err := getClient(t).GetDelegationToken(&webhdfs.GetDelegationTokenRequest{})
	if err != nil {
		t.Fatalf("webhdfs GetDelegationToken failed: %s", err)
	}
	t.Logf("token: %s", resp.Token.UrlString)
	// client_test.go:34: token: HAAEaGRmcwRoZGZzAIoBdwQhGT6KAXcoLZ0-DgQUnnPe7V99qfc5Of-qqsy62GGYBaMSV0VCSERGUyBkZWxlZ2F0aW9uDzE3Mi4xNy4wLjI6ODAyMA
}

func TestClient_Open(t *testing.T) {
	resp, err := getClient(t).Open(&webhdfs.OpenRequest{
		Path: aws.String("/data/test/core-site.xml"),
	})
	defer resp.Body.Close()
	if err != nil {
		t.Fatalf("webhdfs Open failed: %s", err)
	}
	t.Logf("ContentType: %s", aws.StringValue(resp.ContentType))
	t.Logf("ContentLength: %d", aws.Int64Value(resp.ContentLength))
	// client_test.go:48: ContentType: application/octet-stream
	// client_test.go:49: ContentLength: 3659
}

func TestClient_GetFileStatus(t *testing.T) {
	resp, err := getClient(t).GetFileStatus(&webhdfs.GetFileStatusRequest{
		Path: aws.String("/data/test/core-site.xml"),
	})
	if err != nil {
		t.Fatalf("webhdfs GetFileStatus failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("ContentType: %v", resp.FileStatus)
	t.Logf("AccessTime: %s", resp.FileStatus.AccessTime.Time.String())
	t.Logf("ModificationTime: %s", resp.FileStatus.ModificationTime.Time.String())
	t.Logf("Type: %s", resp.FileStatus.Type)
	//    client_test.go:62: FileStatus: {1610695991369 134217728 0 17458 supergroup 3659 1610605959024 hdfs  644 1  FILE}
	//    client_test.go:63: AccessTime: 53010-12-05 01:09:29 +0800 CST
	//    client_test.go:64: ModificationTime: 53008-01-28 00:10:24 +0800 CST
	//    client_test.go:65: Type: FILE
}

func TestClient_ListStatus(t *testing.T) {
	resp, err := getClient(t).ListStatus(&webhdfs.ListStatusRequest{
		Path: aws.String("/data/test"),
	})
	if err != nil {
		t.Fatalf("webhdfs ListStatus failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("FileStatuses: %v", resp.FileStatuses)
	//    client_test.go:80: FileStatuses: {[{1610614972965 134217728 0 17460 supergroup 83910 1610614973143 hdfs 1.jpg 755 1  FILE} {1610695991369 134217728 0 17458 supergroup 3659 1610605959024 hdfs core-site.xml 644 1  FILE}]}
}

func TestClient_ListStatusBatch(t *testing.T) {
	resp, err := getClient(t).ListStatusBatch(&webhdfs.ListStatusBatchRequest{
		Path: aws.String("/data/test"),
	})
	if err != nil {
		t.Fatalf("webhdfs ListStatusBatch failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("DirectoryListing: %v", resp.DirectoryListing)
	// client_test.go:89: webhdfs ListStatusBatch failed: IllegalArgumentException: Invalid value for webhdfs parameter "op": No enum constant org.apache.hadoop.hdfs.web.resources.GetOpParam.Op.LISTSTATUS_BATCH in java.lang.IllegalArgumentException
}

func TestClient_GetContentSummary(t *testing.T) {
	resp, err := getClient(t).GetContentSummary(&webhdfs.GetContentSummaryRequest{
		Path: aws.String("/data/test"),
	})
	if err != nil {
		t.Fatalf("webhdfs GetContentSummary failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("ContentSummary: %v", resp.ContentSummary)
	// client_test.go:144: ContentSummary: {1 2 87569 -1 87569 -1}
}

func TestClient_GetQuotaUsage(t *testing.T) {
	resp, err := getClient(t).GetQuotaUsage(&webhdfs.GetQuotaUsageRequest{
		Path: aws.String("/data/test"),
	})
	if err != nil {
		t.Fatalf("webhdfs GetQuotaUsage failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("QuotaUsage: %v", resp.QuotaUsage)
	// client_test.go:113: webhdfs GetQuotaUsage failed: IllegalArgumentException: Invalid value for webhdfs parameter "op": No enum constant org.apache.hadoop.hdfs.web.resources.GetOpParam.Op.GETQUOTAUSAGE in java.lang.IllegalArgumentException
}

func TestClient_GetFileChecksum(t *testing.T) {
	resp, err := getClient(t).GetFileChecksum(&webhdfs.GetFileChecksumRequest{
		Path: aws.String("/data/test/core-site.xml"),
	})
	if err != nil {
		t.Fatalf("webhdfs GetFileChecksum failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("FileChecksum: %v", resp.FileChecksum)
	// client_test.go:127: FileChecksum: {MD5-of-0MD5-of-512CRC32C 00000200000000000000000078cbe5985d4a6991c863e26618b7e98300000000 28}
}

func TestClient_GetHomeDirectory(t *testing.T) {
	resp, err := getClient(t).GetHomeDirectory(&webhdfs.GetHomeDirectoryRequest{
		Path: aws.String("/data/test/core-site.xml"),
	})
	if err != nil {
		t.Fatalf("webhdfs GetHomeDirectory failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("Path: %v", resp.Path)
	// client_test.go:138: Path: /user/hdfs
}

func TestClient_GetTrashRoot(t *testing.T) {
	resp, err := getClient(t).GetTrashRoot(&webhdfs.GetTrashRootRequest{
		Path: aws.String("/data/test/core-site.xml"),
	})
	if err != nil {
		t.Fatalf("webhdfs GetTrashRoot failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("Path: %v", resp.Path)
	// client_test.go:149: webhdfs GetTrashRoot failed: IllegalArgumentException: Invalid value for webhdfs parameter "op": No enum constant org.apache.hadoop.hdfs.web.resources.GetOpParam.Op.GETTRASHROOT in java.lang.IllegalArgumentException
}

func TestClient_GetXAttr(t *testing.T) {
	resp, err := getClient(t).GetXAttr(&webhdfs.GetXAttrRequest{
		Path:      aws.String("/data/test/core-site.xml"),
		XAttrName: aws.String("user.name"),
		Encoding:  webhdfs.XAttrValueEncodingText.New(),
	})
	if err != nil {
		t.Fatalf("webhdfs GetXAttr failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("XAttrs: %v", resp.XAttrs)
	// client_test.go:161: webhdfs GetXAttr failed: IOException: At least one of the attributes provided was not found. in java.io.IOException
}

func TestClient_GetXAttrs(t *testing.T) {
	resp, err := getClient(t).GetXAttrs(&webhdfs.GetXAttrsRequest{
		Path:       aws.String("/data/test/core-site.xml"),
		XAttrNames: []string{webhdfs.XAttrNamespaceUser.String() + ".name"},
		Encoding:   webhdfs.XAttrValueEncodingText.New(),
	})
	if err != nil {
		t.Fatalf("webhdfs GetXAttrs failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("XAttrs: %v", resp.XAttrs)
	// client_test.go:175: webhdfs GetXAttr failed: IOException: At least one of the attributes provided was not found. in java.io.IOException
}

func TestClient_GetAllXAttrs(t *testing.T) {
	resp, err := getClient(t).GetAllXAttrs(&webhdfs.GetAllXAttrsRequest{
		Path:     aws.String("/data/test/core-site.xml"),
		Encoding: webhdfs.XAttrValueEncodingText.New(),
	})
	if err != nil {
		t.Fatalf("webhdfs GetAllXAttrs failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("XAttrs: %v", resp.XAttrs)
	// client_test.go:174: XAttrs: {[]}
}

func TestClient_ListXAttrs(t *testing.T) {
	resp, err := getClient(t).ListXAttrs(&webhdfs.ListXAttrsRequest{
		Path: aws.String("/data/test/core-site.xml"),
	})
	if err != nil {
		t.Fatalf("webhdfs ListXAttrs failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("XAttrNames: %v", resp.XAttrNames)
	// client_test.go:203: XAttrNames: {[]}
}

func TestClient_CheckAccess(t *testing.T) {
	resp, err := getClient(t).CheckAccess(&webhdfs.CheckAccessRequest{
		Path:     aws.String("/data/test/core-site.xml"),
		Fsaction: aws.String("[r-][w-][x-]"),
	})
	if err != nil {
		t.Fatalf("webhdfs CheckAccess failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("ContentLength: %v", aws.Int64Value(resp.ContentLength))
	// client_test.go:218: ContentLength: 0
}

func TestClient_GetAllStoragePolicy(t *testing.T) {
	resp, err := getClient(t).GetAllStoragePolicy(&webhdfs.GetAllStoragePolicyRequest{})
	if err != nil {
		t.Fatalf("webhdfs GetAllStoragePolicy failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("BlockStoragePolicies: %v", resp.BlockStoragePolicies)
	// client_test.go:223: webhdfs GetAllStoragePolicy failed: IllegalArgumentException: Invalid value for webhdfs parameter "op": No enum constant org.apache.hadoop.hdfs.web.resources.GetOpParam.Op.GETALLSTORAGEPOLICY in java.lang.IllegalArgumentException
}

func TestClient_GetStoragePolicy(t *testing.T) {
	resp, err := getClient(t).GetStoragePolicy(&webhdfs.GetStoragePolicyRequest{})
	if err != nil {
		t.Fatalf("webhdfs GetStoragePolicy failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("BlockStoragePolicy: %v", resp.BlockStoragePolicy)
	// client_test.go:233: webhdfs GetAllStoragePolicy failed: IllegalArgumentException: Invalid value for webhdfs parameter "op": No enum constant org.apache.hadoop.hdfs.web.resources.GetOpParam.Op.GETSTORAGEPOLICY in java.lang.IllegalArgumentException
}

func TestClient_GetSnapshotDiff(t *testing.T) {
	resp, err := getClient(t).GetSnapshotDiff(&webhdfs.GetSnapshotDiffRequest{
		Oldsnapshotname: aws.String("test_old"),
		Snapshotname:    aws.String("test"),
	})
	if err != nil {
		t.Fatalf("webhdfs GetSnapshotDiff failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("SnapshotDiffReport: %v", resp.SnapshotDiffReport)
	// client_test.go:247: webhdfs GetAllStoragePolicy failed: IllegalArgumentException: Invalid value for webhdfs parameter "op": No enum constant org.apache.hadoop.hdfs.web.resources.GetOpParam.Op.GETSNAPSHOTDIFF in java.lang.IllegalArgumentException
}

func TestClient_GetSnapshottableDirectoryList(t *testing.T) {
	resp, err := getClient(t).GetSnapshottableDirectoryList(&webhdfs.GetSnapshottableDirectoryListRequest{
		Username: aws.String("hdfs"),
	})
	if err != nil {
		t.Fatalf("webhdfs GetSnapshottableDirectoryList failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("SnapshottableDirectoryList: %v", resp.SnapshottableDirectoryList)
	// client_test.go:258: webhdfs GetAllStoragePolicy failed: IllegalArgumentException: Invalid value for webhdfs parameter "op": No enum constant org.apache.hadoop.hdfs.web.resources.GetOpParam.Op.GETSNAPSHOTTABLEDIRECTORYLIST in java.lang.IllegalArgumentException
}

func TestClient_GetFileBlockLocations(t *testing.T) {
	resp, err := getClient(t).GetFileBlockLocations(&webhdfs.GetFileBlockLocationsRequest{
		Path: aws.String("/data/test/core-site.xml"),
	})
	if err != nil {
		t.Fatalf("webhdfs GetFileBlockLocations failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("BlockLocations: %v", resp.BlockLocations)
	// client_test.go:273: webhdfs GetAllStoragePolicy failed: IllegalArgumentException: Invalid value for webhdfs parameter "op": No enum constant org.apache.hadoop.hdfs.web.resources.GetOpParam.Op.GETFILEBLOCKLOCATIONS in java.lang.IllegalArgumentException
}

func TestClient_GetECPolicy(t *testing.T) {
	resp, err := getClient(t).GetECPolicy(&webhdfs.GetECPolicyRequest{
		Path: aws.String("/data/test/core-site.xml"),
	})
	if err != nil {
		t.Fatalf("webhdfs GetFileBlockLocations failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("ECPolicy: %v", resp.ECPolicy)
	// client_test.go:285: webhdfs GetFileBlockLocations failed: IllegalArgumentException: Invalid value for webhdfs parameter "op": No enum constant org.apache.hadoop.hdfs.web.resources.GetOpParam.Op.GETECPOLICY in java.lang.IllegalArgumentException
}
