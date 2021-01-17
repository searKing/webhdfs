package webhdfs_test

import (
	"io/ioutil"
	"strings"
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
	if err != nil {
		t.Fatalf("webhdfs Open failed: %s", err)
	}
	defer resp.Body.Close()
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
	t.Logf("FileStatus: %v", resp.FileStatus)
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

func TestClient_Create(t *testing.T) {
	file := "/data/test/create.txt2222"
	{
		resp, err := getClient(t).Create(&webhdfs.CreateRequest{
			Path: aws.String(file),
			Body: strings.NewReader("测试输入"),
		})
		if err != nil {
			t.Fatalf("webhdfs Open failed: %s", err)
		}
		defer resp.Body.Close()
		t.Logf("ContentType: %s", aws.StringValue(resp.ContentType))
		t.Logf("ContentLength: %d", aws.Int64Value(resp.ContentLength))
	}

	{
		resp, err := getClient(t).Open(&webhdfs.OpenRequest{
			Path: aws.String(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Open failed: %s", err)
		}
		defer resp.Body.Close()
		t.Logf("ContentType: %s", aws.StringValue(resp.ContentType))
		t.Logf("ContentLength: %d", aws.Int64Value(resp.ContentLength))

		content, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("webhdfs Read failed: %s", err)
		}
		t.Logf("content: %s", string(content))

	}
	// client_test.go:301: ContentType:
	// client_test.go:302: ContentLength: 0
	// client_test.go:313: ContentType: application/octet-stream
	// client_test.go:314: ContentLength: 0
	// client_test.go:323: content: 测试输入
}

func TestClient_Mkdirs(t *testing.T) {
	dir := "/data/test/create"
	{
		resp, err := getClient(t).Mkdirs(&webhdfs.MkdirsRequest{
			Path: aws.String(dir),
		})
		if err != nil {
			t.Fatalf("webhdfs Open failed: %s", err)
		}
		defer resp.Body.Close()
		t.Logf("ContentType: %s", aws.StringValue(resp.ContentType))
		t.Logf("ContentLength: %d", aws.Int64Value(resp.ContentLength))
	}

	{
		resp, err := getClient(t).GetFileStatus(&webhdfs.GetFileStatusRequest{
			Path: aws.String(dir),
		})
		if err != nil {
			t.Fatalf("webhdfs Open failed: %s", err)
		}
		defer resp.Body.Close()
		t.Logf("FileStatus: %v", resp.FileStatus)
		t.Logf("AccessTime: %s", resp.FileStatus.AccessTime.Time.String())
		t.Logf("ModificationTime: %s", resp.FileStatus.ModificationTime.Time.String())
		t.Logf("Type: %s", resp.FileStatus.Type)
	}
	// client_test.go:332: ContentType: application/json
	// client_test.go:333: ContentLength: 16
	// client_test.go:344: FileStatus: {0 0 0 18823 supergroup 0 -1307645760304418816 hdfs  755 0  DIRECTORY}
	// client_test.go:345: AccessTime: 1970-01-01 08:00:00 +0800 CST
	// client_test.go:346: ModificationTime: 2021-01-17 18:24:25.335 +0800 CST
	// client_test.go:347: Type: DIRECTORY
}

func TestClient_CreateSymlink(t *testing.T) {
	dir := "/data/test/create"
	{
		resp, err := getClient(t).CreateSymlink(&webhdfs.CreateSymlinkRequest{
			Path:        aws.String(dir),
			Destination: aws.String("/data/test/create_symlink"),
		})
		if err != nil {
			t.Fatalf("webhdfs Open failed: %s", err)
		}
		defer resp.Body.Close()
		t.Logf("ContentType: %s", aws.StringValue(resp.ContentType))
		t.Logf("ContentLength: %d", aws.Int64Value(resp.ContentLength))
	}

	{
		resp, err := getClient(t).GetFileStatus(&webhdfs.GetFileStatusRequest{
			Path: aws.String("/data/test/create_symlink"),
		})
		if err != nil {
			t.Fatalf("webhdfs Open failed: %s", err)
		}
		defer resp.Body.Close()
		t.Logf("FileStatus: %v", resp.FileStatus)
		t.Logf("AccessTime: %s", resp.FileStatus.AccessTime.Time.String())
		t.Logf("ModificationTime: %s", resp.FileStatus.ModificationTime.Time.String())
		t.Logf("Type: %s", resp.FileStatus.Type)
	}
	// client_test.go:376: webhdfs Open failed: UnsupportedOperationException: Symlinks not supported in java.lang.UnsupportedOperationException
}

func TestClient_Rename(t *testing.T) {
	file := "/data/test/create.txt2444"
	{
		resp, err := getClient(t).Create(&webhdfs.CreateRequest{
			Path: aws.String(file),
			Body: strings.NewReader("测试输入"),
		})
		if err != nil {
			t.Fatalf("webhdfs Open failed: %s", err)
		}
		defer resp.Body.Close()
		t.Logf("ContentType: %s", aws.StringValue(resp.ContentType))
		t.Logf("ContentLength: %d", aws.Int64Value(resp.ContentLength))
	}
	{
		resp, err := getClient(t).Rename(&webhdfs.RenameRequest{
			Path:        aws.String(file),
			Destination: aws.String("/data/test/rename"),
		})
		if err != nil {
			t.Fatalf("webhdfs Open failed: %s", err)
		}
		defer resp.Body.Close()
		t.Logf("Boolean: %t", resp.Boolean)
	}

	{
		resp, err := getClient(t).GetFileStatus(&webhdfs.GetFileStatusRequest{
			Path: aws.String("/data/test/rename"),
		})
		if err != nil {
			t.Fatalf("webhdfs Open failed: %s", err)
		}
		defer resp.Body.Close()
		t.Logf("FileStatus: %v", resp.FileStatus)
		t.Logf("AccessTime: %s", resp.FileStatus.AccessTime.Time.String())
		t.Logf("ModificationTime: %s", resp.FileStatus.ModificationTime.Time.String())
		t.Logf("Type: %s", resp.FileStatus.Type)
	}
	// client_test.go:410: ContentType:
	// client_test.go:411: ContentLength: 0
	// client_test.go:422: Boolean: true
	// client_test.go:433: FileStatus: {5606985239695581184 134217728 0 18861 supergroup 12 5607010239695581184 hdfs  755 1  FILE}
	// client_test.go:434: AccessTime: 2021-01-17 20:19:39.966 +0800 CST
	// client_test.go:435: ModificationTime: 2021-01-17 20:19:39.991 +0800 CST
	// client_test.go:436: Type: FILE
}

func TestClient_SetReplication(t *testing.T) {
	file := "/data/test/create.txt"
	resp, err := getClient(t).SetReplication(&webhdfs.SetReplicationRequest{
		Path:        aws.String(file),
		Replication: aws.Int(1),
	})
	if err != nil {
		t.Fatalf("webhdfs Open failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("ContentType: %s", aws.StringValue(resp.ContentType))
	t.Logf("Boolean: %t", resp.Boolean)
	// client_test.go:457: ContentType: application/json
	// client_test.go:458: Boolean: true
}

func TestClient_SetOwner(t *testing.T) {
	file := "/data/test/create.txt"
	resp, err := getClient(t).SetOwner(&webhdfs.SetOwnerRequest{
		Path: aws.String(file),
	})
	if err != nil {
		t.Fatalf("webhdfs SetOwner failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("ContentType: %s", aws.StringValue(resp.ContentType))
	// client_test.go:469: webhdfs Open failed: IllegalArgumentException: Both owner and group are empty. in java.lang.IllegalArgumentException
}

func TestClient_SetPermission(t *testing.T) {
	file := "/data/test/create.txt"
	resp, err := getClient(t).SetPermission(&webhdfs.SetPermissionRequest{
		Path: aws.String(file),
	})
	if err != nil {
		t.Fatalf("webhdfs SetPermission failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("ContentType: %s", aws.StringValue(resp.ContentType))
	// client_test.go:485: ContentType: application/octet-stream
}

func TestClient_SetTimes(t *testing.T) {
	file := "/data/test/create.txt"
	resp, err := getClient(t).SetTimes(&webhdfs.SetTimesRequest{
		Path: aws.String(file),
	})
	if err != nil {
		t.Fatalf("webhdfs SetTimes failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("ContentType: %s", aws.StringValue(resp.ContentType))
	// client_test.go:498: ContentType: application/octet-stream
}

func TestClient_RenewDelegationToken(t *testing.T) {
	token := "HAAEaGRmcwRoZGZzAIoBdxB0SHCKAXc0gMxwMgcUR3A39bW0mraYQRzjjs3X3mH7fdkSV0VCSERGUyBkZWxlZ2F0aW9uDzE3Mi4xNy4wLjI6ODAyMA"
	resp, err := getClient(t).RenewDelegationToken(&webhdfs.RenewDelegationTokenRequest{
		Token: aws.String(token),
	})
	if err != nil {
		t.Fatalf("webhdfs RenewDelegationToken failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("Long: %d", resp.Long)
	// client_test.go:511: Long: 1610975228598
}

func TestClient_CancelDelegationToken(t *testing.T) {
	token := "HAAEaGRmcwRoZGZzAIoBdxB0SHCKAXc0gMxwMgcUR3A39bW0mraYQRzjjs3X3mH7fdkSV0VCSERGUyBkZWxlZ2F0aW9uDzE3Mi4xNy4wLjI6ODAyMA"
	resp, err := getClient(t).CancelDelegationToken(&webhdfs.CancelDelegationTokenRequest{
		Token: aws.String(token),
	})
	if err != nil {
		t.Fatalf("webhdfs CancelDelegationToken failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("ContentLength: %d", aws.Int64Value(resp.ContentLength))
	// client_test.go:521: webhdfs Open failed: InvalidToken: Token not found in org.apache.hadoop.security.token.SecretManager$InvalidToken
}

func TestClient_AllowSnapshot(t *testing.T) {
	file := "/data/test/create.txt"
	resp, err := getClient(t).AllowSnapshot(&webhdfs.AllowSnapshotRequest{
		Path: aws.String(file),
	})
	if err != nil {
		t.Fatalf("webhdfs AllowSnapshot failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("ContentLength: %d", aws.Int64Value(resp.ContentLength))
	// client_test.go:534: webhdfs AllowSnapshot failed: IllegalArgumentException: Invalid value for webhdfs parameter "op": No enum constant org.apache.hadoop.hdfs.web.resources.PutOpParam.Op.ALLOWSNAPSHOT in java.lang.IllegalArgumentException
}

func TestClient_DisallowSnapshot(t *testing.T) {
	file := "/data/test/create.txt"
	resp, err := getClient(t).DisallowSnapshot(&webhdfs.DisallowSnapshotRequest{
		Path: aws.String(file),
	})
	if err != nil {
		t.Fatalf("webhdfs DisallowSnapshot failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("ContentLength: %d", aws.Int64Value(resp.ContentLength))
	// client_test.go:547: webhdfs DisallowSnapshot failed: IllegalArgumentException: Invalid value for webhdfs parameter "op": No enum constant org.apache.hadoop.hdfs.web.resources.PutOpParam.Op.DISALLOWSNAPSHOT in java.lang.IllegalArgumentException
}

func TestClient_CreateSnapshot(t *testing.T) {
	file := "/data/test/"
	resp, err := getClient(t).CreateSnapshot(&webhdfs.CreateSnapshotRequest{
		Path: aws.String(file),
	})
	if err != nil {
		t.Fatalf("webhdfs CreateSnapshot failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("Path: %s", aws.StringValue(resp.Path))
	// client_test.go:560: webhdfs DisallowSnapshot failed: SnapshotException: Directory is not a snapshottable directory: /data/test in org.apache.hadoop.hdfs.protocol.SnapshotException
}

func TestClient_RenameSnapshot(t *testing.T) {
	file := "/data/test/"
	resp, err := getClient(t).RenameSnapshot(&webhdfs.RenameSnapshotRequest{
		Path: aws.String(file),
	})
	if err != nil {
		t.Fatalf("webhdfs RenameSnapshot failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("ContentLength: %d", aws.Int64Value(resp.ContentLength))
	// client_test.go:574: webhdfs DisallowSnapshot failed: Key: 'RenameSnapshotRequest.Oldsnapshotname' Error:Field validation for 'Oldsnapshotname' failed on the 'required' tag
}

func TestClient_SetXAttr(t *testing.T) {
	file := "/data/test/"
	resp, err := getClient(t).SetXAttr(&webhdfs.SetXAttrRequest{
		Path:       aws.String(file),
		XAttrName:  aws.String("user.name"),
		XAttrValue: aws.String("sfdh"),
		XAttrFlag:  webhdfs.XAttrSetFlagCreate.New(),
	})
	if err != nil {
		t.Fatalf("webhdfs SetXAttr failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("ContentLength: %d", aws.Int64Value(resp.ContentLength))
	// client_test.go:589: webhdfs DisallowSnapshot failed: IOException: XAttr: name already exists. The REPLACE flag must be specified. in java.io.IOException
}

func TestClient_RemoveXAttr(t *testing.T) {
	file := "/data/test/"
	resp, err := getClient(t).RemoveXAttr(&webhdfs.RemoveXAttrRequest{
		Path:      aws.String(file),
		XAttrName: aws.String("user.name"),
	})
	if err != nil {
		t.Fatalf("webhdfs RemoveXAttr failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("ContentLength: %d", aws.Int64Value(resp.ContentLength))
	// client_test.go:606: ContentLength: 0
}

func TestClient_SetStoragePolicy(t *testing.T) {
	file := "/data/test/"
	resp, err := getClient(t).SetStoragePolicy(&webhdfs.SetStoragePolicyRequest{
		Path:          aws.String(file),
		StoragePolicy: aws.String("policy"),
	})
	if err != nil {
		t.Fatalf("webhdfs SetStoragePolicy failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("ContentLength: %d", aws.Int64Value(resp.ContentLength))
	// client_test.go:615: webhdfs SetStoragePolicy failed: IllegalArgumentException: Invalid value for webhdfs parameter "op": No enum constant org.apache.hadoop.hdfs.web.resources.PutOpParam.Op.SETSTORAGEPOLICY in java.lang.IllegalArgumentException
}

func TestClient_SatisfyStoragePolicy(t *testing.T) {
	file := "/data/test/"
	resp, err := getClient(t).SatisfyStoragePolicy(&webhdfs.SatisfyStoragePolicyRequest{
		Path:          aws.String(file),
		StoragePolicy: aws.String("policy"),
	})
	if err != nil {
		t.Fatalf("webhdfs SatisfyStoragePolicy failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("ContentLength: %d", aws.Int64Value(resp.ContentLength))
	// client_test.go:631: webhdfs SatisfyStoragePolicy failed: IllegalArgumentException: Invalid value for webhdfs parameter "op": No enum constant org.apache.hadoop.hdfs.web.resources.PutOpParam.Op.SATISFYSTORAGEPOLICY in java.lang.IllegalArgumentException
}

func TestClient_EnableECPolicy(t *testing.T) {
	resp, err := getClient(t).EnableECPolicy(&webhdfs.EnableECPolicyRequest{
		ECPolicy: aws.String("ecpolicy"),
	})
	if err != nil {
		t.Fatalf("webhdfs EnableECPolicy failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("ContentLength: %d", aws.Int64Value(resp.ContentLength))
	// client_test.go:644: webhdfs EnableECPolicy failed: IllegalArgumentException: Invalid value for webhdfs parameter "op": No enum constant org.apache.hadoop.hdfs.web.resources.PutOpParam.Op.ENABLEECPOLICY in java.lang.IllegalArgumentException
}

func TestClient_DisableECPolicy(t *testing.T) {
	resp, err := getClient(t).DisableECPolicy(&webhdfs.DisableECPolicyRequest{
		ECPolicy: aws.String("ecpolicy"),
	})
	if err != nil {
		t.Fatalf("webhdfs DisableECPolicy failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("ContentLength: %d", aws.Int64Value(resp.ContentLength))
	// client_test.go:655: webhdfs DisableECPolicy failed: IllegalArgumentException: Invalid value for webhdfs parameter "op": No enum constant org.apache.hadoop.hdfs.web.resources.PutOpParam.Op.DISABLEECPOLICY in java.lang.IllegalArgumentException
}

func TestClient_SetECPolicy(t *testing.T) {
	file := "/data/test/"
	resp, err := getClient(t).SetECPolicy(&webhdfs.SetECPolicyRequest{
		Path:     aws.String(file),
		ECPolicy: aws.String("ecpolicy"),
	})
	if err != nil {
		t.Fatalf("webhdfs SetECPolicy failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("ContentLength: %d", aws.Int64Value(resp.ContentLength))
	// client_test.go:669: webhdfs SetECPolicy failed: IllegalArgumentException: Invalid value for webhdfs parameter "op": No enum constant org.apache.hadoop.hdfs.web.resources.PutOpParam.Op.SETECPOLICY in java.lang.IllegalArgumentException
}

func TestClient_Append(t *testing.T) {
	file := "/data/test/append.txt"
	{
		resp, err := getClient(t).Create(&webhdfs.CreateRequest{
			Path: aws.String(file),
			Body: strings.NewReader("create"),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
		}
		defer resp.Body.Close()
		t.Logf("ContentType: %s", aws.StringValue(resp.ContentType))
		t.Logf("ContentLength: %d", aws.Int64Value(resp.ContentLength))
	}
	{
		resp, err := getClient(t).Append(&webhdfs.AppendRequest{
			Path: aws.String(file),
			Body: strings.NewReader("append"),
		})
		if err != nil {
			t.Fatalf("webhdfs Append failed: %s", err)
		}
		defer resp.Body.Close()
		t.Logf("ContentType: %s", aws.StringValue(resp.ContentType))
		t.Logf("ContentLength: %d", aws.Int64Value(resp.ContentLength))
	}

	{
		resp, err := getClient(t).Open(&webhdfs.OpenRequest{
			Path: aws.String(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Open failed: %s", err)
		}
		defer resp.Body.Close()
		t.Logf("ContentType: %s", aws.StringValue(resp.ContentType))
		t.Logf("ContentLength: %d", aws.Int64Value(resp.ContentLength))

		content, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("webhdfs Read failed: %s", err)
		}
		t.Logf("content: %s", string(content))

	}
	// client_test.go:687: ContentType:
	// client_test.go:688: ContentLength: 0
	// client_test.go:699: ContentType:
	// client_test.go:700: ContentLength: 0
	// client_test.go:711: ContentType: application/octet-stream
	// client_test.go:712: ContentLength: 66
	// client_test.go:718: content: createappend
}

func TestClient_Concat(t *testing.T) {
	file := "/data/test/append.txt"
	{
		resp, err := getClient(t).Create(&webhdfs.CreateRequest{
			Path: aws.String("/data/test/1.txt"),
			Body: strings.NewReader("create"),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
		}
		defer resp.Body.Close()
	}
	{
		resp, err := getClient(t).Create(&webhdfs.CreateRequest{
			Path: aws.String("/data/test/2.txt"),
			Body: strings.NewReader("create"),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
		}
		defer resp.Body.Close()
	}
	{
		resp, err := getClient(t).Concat(&webhdfs.ConcatRequest{
			Path:    aws.String(file),
			Sources: aws.String("/data/test/11.txt,/data/test/22.txt"),
		})
		if err != nil {
			t.Fatalf("webhdfs Concat failed: %s", err)
		}
		defer resp.Body.Close()
		t.Logf("ContentType: %s", aws.StringValue(resp.ContentType))
		t.Logf("ContentLength: %d", aws.Int64Value(resp.ContentLength))
	}

	{
		resp, err := getClient(t).Open(&webhdfs.OpenRequest{
			Path: aws.String(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Open failed: %s", err)
		}
		defer resp.Body.Close()

		content, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("webhdfs Read failed: %s", err)
		}
		t.Logf("content: %s", string(content))

	}
	// client_test.go:758: webhdfs Concat failed: HadoopIllegalArgumentException: The last block in /data/test/append.txt is not full; last block size = 66 but file block size = 134217728 in org.apache.hadoop.HadoopIllegalArgumentException
}

func TestClient_Truncate(t *testing.T) {
	file := "/data/test/truncate.txt"
	{
		resp, err := getClient(t).Create(&webhdfs.CreateRequest{
			Path: aws.String(file),
			Body: strings.NewReader("create"),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
		}
		defer resp.Body.Close()
	}
	{
		resp, err := getClient(t).Truncate(&webhdfs.TruncateRequest{
			Path:      aws.String(file),
			NewLength: aws.Int64(0),
		})
		if err != nil {
			t.Fatalf("webhdfs Truncate failed: %s", err)
		}
		defer resp.Body.Close()
		t.Logf("ContentType: %s", aws.StringValue(resp.ContentType))
		t.Logf("ContentLength: %d", aws.Int64Value(resp.ContentLength))
	}

	{
		resp, err := getClient(t).Open(&webhdfs.OpenRequest{
			Path: aws.String(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Open failed: %s", err)
		}
		defer resp.Body.Close()

		content, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("webhdfs Read failed: %s", err)
		}
		t.Logf("content: %s", string(content))

	}
	//     client_test.go:801: webhdfs Truncate failed: IllegalArgumentException: Invalid value for webhdfs parameter "op": No enum constant org.apache.hadoop.hdfs.web.resources.PostOpParam.Op.TRUNCATE in java.lang.IllegalArgumentException
}

func TestClient_UnsetStoragePolicy(t *testing.T) {
	file := "/data/test/test.txt"
	{
		resp, err := getClient(t).UnsetStoragePolicy(&webhdfs.UnsetStoragePolicyRequest{
			Path: aws.String(file),
		})
		if err != nil {
			t.Fatalf("webhdfs UnsetStoragePolicy failed: %s", err)
		}
		defer resp.Body.Close()
	}
	// client_test.go:836: webhdfs UnsetStoragePolicy failed: IllegalArgumentException: Invalid value for webhdfs parameter "op": No enum constant org.apache.hadoop.hdfs.web.resources.PostOpParam.Op.UNSETSTORAGEPOLICY in java.lang.IllegalArgumentException
}

func TestClient_UnsetECPolicy(t *testing.T) {
	file := "/data/test/test.txt"
	{
		resp, err := getClient(t).UnsetECPolicy(&webhdfs.UnsetECPolicyRequest{
			Path: aws.String(file),
		})
		if err != nil {
			t.Fatalf("webhdfs UnsetECPolicy failed: %s", err)
		}
		defer resp.Body.Close()
	}
	// client_test.go:850: webhdfs UnsetECPolicy failed: IllegalArgumentException: Invalid value for webhdfs parameter "op": No enum constant org.apache.hadoop.hdfs.web.resources.PostOpParam.Op.UNSETECPOLICY in java.lang.IllegalArgumentException
}

func TestClient_Delete(t *testing.T) {
	file := "/data/test/test.txt"
	{
		resp, err := getClient(t).Delete(&webhdfs.DeleteRequest{
			Path: aws.String(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
		}
		defer resp.Body.Close()
	}
	// client_test.go:850: webhdfs Delete failed: IllegalArgumentException: Invalid value for webhdfs parameter "op": No enum constant org.apache.hadoop.hdfs.web.resources.PostOpParam.Op.UNSETECPOLICY in java.lang.IllegalArgumentException
}

func TestClient_DeleteSnapshot(t *testing.T) {
	file := "/data/test/test.txt"
	{
		resp, err := getClient(t).DeleteSnapshot(&webhdfs.DeleteSnapshotRequest{
			Path:         aws.String(file),
			Snapshotname: aws.String("snapshot"),
		})
		if err != nil {
			t.Fatalf("webhdfs DeleteSnapshot failed: %s", err)
		}
		defer resp.Body.Close()
	}
	// client_test.go:878: webhdfs DeleteSnapshot failed: FileNotFoundException: Directory does not exist: /data/test/test.txt in java.io.FileNotFoundException
}
