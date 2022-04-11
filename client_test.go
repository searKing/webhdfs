// Copyright 2022 The searKing Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package webhdfs_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/searKing/golang/go/exp/types"

	time_ "github.com/searKing/golang/go/time"

	"github.com/searKing/webhdfs"
)

const (
	webHdfsEndpoint              = "quickstart.cloudera:50070"
	httpHdfsEndpoint             = "quickstart.cloudera:14000"
	KerberosRealm                = "CLOUDERA"
	KerberosUsername             = "hdfs/quickstart.cloudera"
	KerberosServicePrincipleName = "HTTP/quickstart.cloudera"
	KerberosPassword             = ""
	KerberosKeyTabFile           = "internal/hdfs.keytab"   // /krb5.keytab
	KerberosCCacheFile           = "internal/tmp/krb5cc_0"  // /tmp/krb5cc_0
	KerberosConfigFile           = "internal/etc/krb5.conf" // /etc/krb5.conf, /var/kerberos/krb5kdc/kdc.conf

	LocalBucket = "internal/test.bucket"
	HdfsBucket  = "test.bucket"
)

func getClient(t *testing.T, endpoint string) *webhdfs.Client {
	c, err := webhdfs.New(endpoint, webhdfs.WithDisableSSL(true),
		webhdfs.WithKerberosKeytabFile(KerberosUsername, KerberosServicePrincipleName, KerberosRealm, KerberosKeyTabFile, KerberosConfigFile))
	if err != nil {
		t.Fatalf("create client %s", err)
	}
	return c
}

func getWebHDFSClient(t *testing.T) *webhdfs.Client {
	return getClient(t, webHdfsEndpoint)
}

func getHttpHDFSClient(t *testing.T) *webhdfs.Client {
	return getClient(t, httpHdfsEndpoint)
}

func TestClient_GetDelegationToken(t *testing.T) {
	c := getWebHDFSClient(t)
	resp, err := c.GetDelegationToken(&webhdfs.GetDelegationTokenRequest{
		ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
	})
	if err != nil {
		t.Fatalf("webhdfs GetDelegationToken failed: %s", err)
	}
	t.Logf("token: %s", resp.Token.UrlString)
	// client_test.go:34: token: HAAEaGRmcwRoZGZzAIoBdwQhGT6KAXcoLZ0-DgQUnnPe7V99qfc5Of-qqsy62GGYBaMSV0VCSERGUyBkZWxlZ2F0aW9uDzE3Mi4xNy4wLjI6ODAyMA
}

func TestClient_Open(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"

	{
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}
	resp, err := c.Open(&webhdfs.OpenRequest{
		ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
		Path:      types.Pointer(file),
	})
	if err != nil {
		t.Fatalf("webhdfs Open failed: %s", err)
		return
	}
	defer resp.Body.Close()
	readData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("webhdfs Open and Read failed: %s", err)
	}

	if bytes.Compare([]byte(writtenData), readData) != 0 {
		t.Fatalf("%s, expected %q, got %q", file, writtenData, readData)
	}
	t.Logf("%s, written %q, read %q", file, writtenData, readData)
	//  Output:
	//    client_test.go:92: /test.bucket/test/found.txt, written "Hello World!", read "Hello World!"

}

func TestClient_Open_NotFound(t *testing.T) {
	c := getWebHDFSClient(t)
	func() {
		file := HdfsBucket + "/test/notfound.txt"
		{
			c := getWebHDFSClient(t)
			resp, err := c.Delete(&webhdfs.DeleteRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				Path:      types.Pointer(file),
			})
			if err != nil {
				t.Fatalf("webhdfs Delete failed: %s", err)
				return
			}
			defer resp.Body.Close()
		}
		resp, err := c.Open(&webhdfs.OpenRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				t.Logf("webhdfs Open failed: %s", err)
			} else {
				t.Fatalf("webhdfs Open failed: %s", err)
			}
			return
		}
		defer resp.Body.Close()
		//    client_test.go:134: webhdfs Open failed: FileNotFoundException: File does not exist: /test.bucket/test/notfound.txt in java.io.FileNotFoundException
	}()
}

func TestClient_GetFileStatus_File(t *testing.T) {
	c := getWebHDFSClient(t)

	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	{
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}
	{ // File
		resp, err := c.GetFileStatus(&webhdfs.GetFileStatusRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs GetFileStatus failed: %s", err)
		}
		defer resp.Body.Close()
		fi := resp.FileStatus
		t.Logf("Dir(%t), %s, %d, %s", fi.IsDir(), fi.Name(), fi.Size(), fi.ModTime())
		if fi.IsDir() {
			t.Errorf("IsDir(): got %t, want %t", fi.IsDir(), false)
		}
		if fi.Name() != path.Base(file) {
			t.Errorf("Name(): got %s, want %s", fi.Name(), path.Base(file))
		}
		if fi.Size() != int64(len(writtenData)) {
			t.Errorf("Size(): got %d, want %d", fi.Size(), int64(len(writtenData)))
		}
		//    client_test.go:173: Dir(false), found.txt, 12, 2021-01-28 10:35:33.913 +0800 CST
	}
}

func TestClient_GetFileStatus_Dir(t *testing.T) {
	c := getWebHDFSClient(t)

	dir := HdfsBucket + "/test"
	{
		resp, err := c.Mkdirs(&webhdfs.MkdirsRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(dir),
		})
		if err != nil {
			t.Fatalf("webhdfs Mkdirs failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}
	{ // Dir
		resp, err := c.GetFileStatus(&webhdfs.GetFileStatusRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(dir),
		})
		if err != nil {
			t.Fatalf("webhdfs GetFileStatus failed: %s", err)
		}
		defer resp.Body.Close()
		fi := resp.FileStatus
		if !fi.IsDir() {
			t.Errorf("IsDir(): got %t, want %t", fi.IsDir(), false)
		}
		if fi.Name() != path.Base(dir) {
			t.Errorf("Name(): got %s, want %s", fi.Name(), path.Base(dir))
		}
		if fi.Size() != 0 {
			t.Errorf("Size(): got %d, want %d", fi.Size(), 0)
		}
		t.Logf("Dir(%t), %s, %d, %s", fi.IsDir(), fi.Name(), fi.Size(), fi.ModTime())
		//    client_test.go:223: Dir(true), test, 0, 2021-01-28 10:57:48.977 +0800 CST
	}
}

func TestClient_GetFileStatus_File_NotFound(t *testing.T) {
	c := getWebHDFSClient(t)

	file := HdfsBucket + "/test/notfound.txt"
	{
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}
	{ // NotFound File
		resp, err := c.GetFileStatus(&webhdfs.GetFileStatusRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})

		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				t.Logf("webhdfs GetFileStatus failed: %s", err)
			} else {
				t.Fatalf("webhdfs GetFileStatus failed: %s", err)
			}
			return
		}
		defer resp.Body.Close()
		fi := resp.FileStatus
		t.Errorf("Dir(%t), %s, %d, %s", fi.IsDir(), fi.Name(), fi.Size(), fi.ModTime())
		//    client_test.go:249: webhdfs Open failed: FileNotFoundException: File does not exist: /test.bucket/test/notfound.txt in java.io.FileNotFoundException
	}
}

func TestClient_ListStatus_File(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	{
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}
	{
		resp, err := c.ListStatus(&webhdfs.ListStatusRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs ListStatus failed: %s", err)
		}
		defer resp.Body.Close()
		fileStatuses := resp.FileStatuses.FileStatus
		for i, fi := range fileStatuses {
			t.Logf("[%d] Dir(%t), %s, %d, %s", i, fi.IsDir(), fi.Name(), fi.Size(), fi.ModTime())
		}

		if len(fileStatuses) != 1 {
			t.Fatalf("len(FileStatus): got %d, want %d", len(fileStatuses), 1)
		}
		fi := fileStatuses[0]
		if fi.IsDir() {
			t.Errorf("IsDir(): got %t, want %t", fi.IsDir(), false)
		}
		if fi.Name() != path.Base(file) {
			t.Errorf("Name(): got %s, want %s", fi.Name(), path.Base(file))
		}
		if fi.Size() != int64(len(writtenData)) {
			t.Errorf("Size(): got %d, want %d", fi.Size(), int64(len(writtenData)))
		}

	}
	//    client_test.go:288: [0] Dir(false), found.txt, 12, 2021-01-28 11:14:04.408 +0800 CST
}

func TestClient_ListStatus_Dir(t *testing.T) {
	c := getWebHDFSClient(t)
	root := HdfsBucket + "/test"
	files := []string{
		path.Join(root, "found.txt"),
		path.Join(root, "/subdir/found.txt"),
	}
	writtenData := "Hello World!"

	func() { // remove root dir
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(root),
			Recursive: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	for _, file := range files {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		resp.Body.Close()
	}
	{
		resp, err := c.ListStatus(&webhdfs.ListStatusRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(root),
		})
		if err != nil {
			t.Fatalf("webhdfs ListStatus failed: %s", err)
		}
		defer resp.Body.Close()
		fileStatuses := resp.FileStatuses.FileStatus
		for i, fi := range fileStatuses {
			t.Logf("[%d] Dir(%t), %s, %d, %s", i, fi.IsDir(), fi.Name(), fi.Size(), fi.ModTime())
		}

		if len(fileStatuses) != len(files) {
			t.Errorf("len(FileStatus): got %d, want %d", len(fileStatuses), len(files))
		}

		var wantFileStatuses = map[string]struct{}{
			"found.txt": {},
			"subdir":    {},
		}
		for _, fi := range fileStatuses {
			if _, has := wantFileStatuses[fi.Name()]; !has {
				t.Errorf("unexpect file status: got Dir(%t), %s, %d, %s", fi.IsDir(), fi.Name(), fi.Size(), fi.ModTime())
			}
		}
	}
	//    client_test.go:354: [0] Dir(false), found.txt, 12, 2021-01-28 11:27:08.86 +0800 CST
	//    client_test.go:354: [1] Dir(true), subdir, 0, 2021-01-28 11:27:09.317 +0800 CST
}

func TestClient_ListStatus_File_NotFound(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/notfound.txt"
	func() {
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Recursive: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	{
		resp, err := c.ListStatus(&webhdfs.ListStatusRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})

		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				t.Logf("webhdfs ListStatus failed: %s", err)
			} else {
				t.Fatalf("webhdfs ListStatus failed: %s", err)
			}
			return
		}
		defer resp.Body.Close()
		fileStatuses := resp.FileStatuses.FileStatus
		for i, fi := range fileStatuses {
			t.Errorf("[%d] Dir(%t), %s, %d, %s", i, fi.IsDir(), fi.Name(), fi.Size(), fi.ModTime())
		}
	}
	//    client_test.go:400: webhdfs ListStatus failed: FileNotFoundException: File /test.bucket/test/notfound.txt does not exist. in java.io.FileNotFoundException
}

func TestClient_ListStatusBatch_File(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	{
		resp, err := c.ListStatusBatch(&webhdfs.ListStatusBatchRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs ListStatusBatch failed: %s", err)
		}
		defer resp.Body.Close()
		fileStatuses := resp.DirectoryListing.PartialListing.FileStatuses.FileStatus
		for i, fi := range fileStatuses {
			t.Logf("[%d] Dir(%t), %s, %d, %s", i, fi.IsDir(), fi.Name(), fi.Size(), fi.ModTime())
		}

		if resp.DirectoryListing.RemainingEntries != 0 {
			t.Errorf("RemainingEntries: got %d, want %d", resp.DirectoryListing.RemainingEntries, 0)
		}
		if len(fileStatuses) != 1 {
			t.Fatalf("len(FileStatus): got %d, want %d", len(fileStatuses), 1)
		}
		fi := fileStatuses[0]
		if fi.IsDir() {
			t.Errorf("IsDir(): got %t, want %t", fi.IsDir(), false)
		}
		if fi.Name() != path.Base("") {
			t.Errorf("Name(): got %s, want %s", fi.Name(), path.Base(""))
		}
		if fi.Size() != int64(len(writtenData)) {
			t.Errorf("Size(): got %d, want %d", fi.Size(), int64(len(writtenData)))
		}
	}
	//    client_test.go:443: [0] Dir(false), ., 12, 2021-01-28 11:35:26.182 +0800 CST
}

func TestClient_ListStatusBatch_Dir(t *testing.T) {
	c := getWebHDFSClient(t)
	root := HdfsBucket + "/test"
	files := []string{
		path.Join(root, "found.txt"),
		path.Join(root, "/subdir/found.txt"),
	}
	writtenData := "Hello World!"

	func() { // remove root dir
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(root),
			Recursive: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	for _, file := range files {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		resp.Body.Close()
	}
	var entries int
	var startAfter string
	for {
		resp, err := c.ListStatusBatch(&webhdfs.ListStatusBatchRequest{
			ProxyUser:  c.ProxyUser(), // optional, user.name, The authenticated user
			Path:       types.Pointer(root),
			StartAfter: types.Pointer(startAfter),
		})
		if err != nil {
			t.Fatalf("webhdfs ListStatusBatch failed: %s", err)
		}
		_ = resp.Body.Close()
		remainingEntries := resp.DirectoryListing.RemainingEntries
		fileStatuses := resp.DirectoryListing.PartialListing.FileStatuses.FileStatus
		for i, fi := range fileStatuses {
			entries++
			startAfter = fi.PathSuffix
			t.Logf("[%d] Dir(%t), %s, %d, %s", i, fi.IsDir(), fi.Name(), fi.Size(), fi.ModTime())
		}
		t.Logf("RemainingEntries: %d", remainingEntries)

		if remainingEntries == 0 {
			break
		}
	}
	if entries != len(files) {
		t.Errorf("len(Entries): got %d, want %d", entries, len(files))
	}
	//    client_test.go:517: [0] Dir(false), found.txt, 12, 2021-01-28 12:43:09.648 +0800 CST
	//    client_test.go:517: [1] Dir(true), subdir, 0, 2021-01-28 12:43:09.905 +0800 CST
	//    client_test.go:519: RemainingEntries: 0
}

func TestClient_ListStatusBatch_File_NotFound(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/notfound.txt"
	func() {
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Recursive: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	{
		resp, err := c.ListStatusBatch(&webhdfs.ListStatusBatchRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})

		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				t.Logf("webhdfs ListStatusBatch failed: %s", err)
			} else {
				t.Fatalf("webhdfs ListStatusBatch failed: %s", err)
			}
			return
		}
		defer resp.Body.Close()
		fileStatuses := resp.DirectoryListing.PartialListing.FileStatuses.FileStatus
		for i, fi := range fileStatuses {
			t.Errorf("[%d] Dir(%t), %s, %d, %s", i, fi.IsDir(), fi.Name(), fi.Size(), fi.ModTime())
		}
	}
	//    client_test.go:489: webhdfs ListStatusBatch failed: FileNotFoundException: File /test.bucket/test/notfound.txt does not exist. in java.io.FileNotFoundException
}

func TestClient_GetContentSummary_File(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	{
		resp, err := c.GetContentSummary(&webhdfs.GetContentSummaryRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs GetContentSummary failed: %s", err)
		}
		defer resp.Body.Close()
		data, err := json.Marshal(resp.ContentSummary)
		if err != nil {
			t.Fatalf("webhdfs GetContentSummary json Marshal failed: %s", err)
			return
		}
		t.Logf("ContentSummary: %s", string(data))

		if resp.ContentSummary.DirectoryCount != 0 {
			t.Errorf("DirectoryCount: got %d, want %d", resp.ContentSummary.DirectoryCount, 0)
		}

		if resp.ContentSummary.FileCount != 1 {
			t.Errorf("FileCount: got %d, want %d", resp.ContentSummary.FileCount, 1)
		}

		if resp.ContentSummary.Length != int64(len(writtenData)) {
			t.Errorf("Length: got %d, want %d", resp.ContentSummary.Length, int64(len(writtenData)))
		}
	}
	//    client_test.go:668: ContentSummary: {
	//        	"directoryCount": 2,
	//        	"fileCount": 2,
	//        	"length": 24,
	//        	"quota": -1,
	//        	"spaceConsumed": 48,
	//        	"spaceQuota": -1,
	//        	"typeQuota": {
	//        		"ARCHIVE": {
	//        			"consumed": 0,
	//        			"quota": 0
	//        		},
	//        		"DISK": {
	//        			"consumed": 0,
	//        			"quota": 0
	//        		},
	//        		"SSD": {
	//        			"consumed": 0,
	//        			"quota": 0
	//        		}
	//        	}
	//        }
}

func TestClient_GetContentSummary_Dir(t *testing.T) {
	c := getWebHDFSClient(t)
	root := HdfsBucket + "/test"
	files := []string{
		path.Join(root, "found.txt"),
		path.Join(root, "/subdir/found.txt"),
	}
	writtenData := "Hello World!"

	func() { // remove root dir
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(root),
			Recursive: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	for _, file := range files {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		resp.Body.Close()
	}

	{
		resp, err := c.GetContentSummary(&webhdfs.GetContentSummaryRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(root),
		})
		if err != nil {
			t.Fatalf("webhdfs GetContentSummary failed: %s", err)
		}
		defer resp.Body.Close()
		data, err := json.MarshalIndent(resp.ContentSummary, "", "\t")
		if err != nil {
			t.Fatalf("webhdfs GetContentSummary json Marshal failed: %s", err)
		}
		t.Logf("ContentSummary: %s", string(data))

		// /test.bucket/test/.
		// /test.bucket/test/subdir
		if resp.ContentSummary.DirectoryCount != 2 {
			t.Errorf("DirectoryCount: got %d, want %d", resp.ContentSummary.DirectoryCount, 2)
		}

		// /test.bucket/test/found.txt
		// /test.bucket/test/subdir/found.txt
		if resp.ContentSummary.FileCount != 2 {
			t.Errorf("FileCount: got %d, want %d", resp.ContentSummary.FileCount, 2)
		}

		if resp.ContentSummary.Length != int64(len(writtenData)*2) {
			t.Errorf("Length: got %d, want %d", resp.ContentSummary.Length, int64(len(writtenData)))
		}
	}

	// client_test.go:144: ContentSummary: {1 2 87569 -1 87569 -1}
}

func TestClient_GetContentSummary_File_NotFound(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/notfound.txt"
	func() {
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Recursive: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	{
		resp, err := c.GetContentSummary(&webhdfs.GetContentSummaryRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				t.Logf("webhdfs ListStatusBatch failed: %s", err)
			} else {
				t.Fatalf("webhdfs ListStatusBatch failed: %s", err)
			}
			return
		}
		defer resp.Body.Close()
		data, err := json.Marshal(resp.ContentSummary)
		if err != nil {
			t.Fatalf("webhdfs GetContentSummary json Marshal failed: %s", err)
		}
		t.Errorf("ContentSummary: %s", string(data))
	}
	//    client_test.go:712: webhdfs ListStatusBatch failed: FileNotFoundException: File does not exist: /test.bucket/test/notfound.txt in java.io.FileNotFoundException
}

func TestClient_GetQuotaUsage(t *testing.T) {
	c := getWebHDFSClient(t)
	resp, err := c.GetQuotaUsage(&webhdfs.GetQuotaUsageRequest{
		ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
		Path:      types.Pointer("/data/test"),
	})
	if err != nil {
		t.Fatalf("webhdfs GetQuotaUsage failed: %s", err)
	}
	defer resp.Body.Close()

	data, err := json.MarshalIndent(resp.QuotaUsage, "", "\t")
	if err != nil {
		t.Fatalf("webhdfs GetQuotaUsage json Marshal failed: %s", err)
		return
	}
	t.Logf("QuotaUsage: %s", string(data))
	//    client_test.go:744: QuotaUsage: {
	//        	"fileAndDirectoryCount": 3,
	//        	"quota": -1,
	//        	"spaceConsumed": 167844,
	//        	"spaceQuota": -1,
	//        	"typeQuota": {
	//        		"ARCHIVE": {
	//        			"consumed": 0,
	//        			"quota": 0
	//        		},
	//        		"DISK": {
	//        			"consumed": 0,
	//        			"quota": 0
	//        		},
	//        		"SSD": {
	//        			"consumed": 0,
	//        			"quota": 0
	//        		}
	//        	}
	//        }
}

func TestClient_GetFileChecksum(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	{
		resp, err := c.GetFileChecksum(&webhdfs.GetFileChecksumRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs GetFileChecksum failed: %s", err)
		}
		defer resp.Body.Close()

		data, err := json.MarshalIndent(resp.FileChecksum, "", "\t")
		if err != nil {
			t.Fatalf("webhdfs GetFileChecksum json Marshal failed: %s", err)
			return
		}
		t.Logf("FileChecksum: %s", string(data))
	}
	//    client_test.go:820: FileChecksum: {
	//        	"algorithm": "MD5-of-0MD5-of-512CRC32C",
	//        	"bytes": "000002000000000000000000cbce76920e8bd8fea88009894bb094a500000000",
	//        	"length": 28
	//        }
}

func TestClient_GetFileChecksum_File_NotFound(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/notfound.txt"
	func() {
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Recursive: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	{
		resp, err := c.GetFileChecksum(&webhdfs.GetFileChecksumRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				t.Logf("webhdfs GetFileChecksum failed: %s", err)
			} else {
				t.Fatalf("webhdfs GetFileChecksum failed: %s", err)
			}
			return
		}
		defer resp.Body.Close()
		data, err := json.Marshal(resp.FileChecksum)
		if err != nil {
			t.Fatalf("webhdfs GetFileChecksum json Marshal failed: %s", err)
		}
		t.Errorf("FileChecksum: %s", string(data))
	}
	//    client_test.go:851: webhdfs GetFileChecksum failed: FileNotFoundException: File does not exist: /test.bucket/test/notfound.txt in java.io.FileNotFoundException
}

func TestClient_GetHomeDirectory(t *testing.T) {
	c := getWebHDFSClient(t)

	resp, err := c.GetHomeDirectory(&webhdfs.GetHomeDirectoryRequest{
		ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
	})
	if err != nil {
		t.Fatalf("webhdfs GetHomeDirectory failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("Path: %v", resp.Path)
	//    client_test.go:877: Path: /user/root
}

func TestClient_GetTrashRoot(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/notfound.txt"
	resp, err := c.GetTrashRoot(&webhdfs.GetTrashRootRequest{
		ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
		Path:      types.Pointer(file),
	})
	if err != nil {
		t.Fatalf("webhdfs GetTrashRoot failed: %s", err)
	}
	defer resp.Body.Close()
	t.Logf("Path: %v", resp.Path)
	//    client_test.go:892: Path: /user/root/.Trash
}

func TestClient_GetXAttr(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	XAttrName := webhdfs.XAttrNamespaceUser.String() + ".name"
	XAttrValue := "example"
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.SetXAttr(&webhdfs.SetXAttrRequest{
			ProxyUser:  c.ProxyUser(), // optional, user.name, The authenticated user
			Path:       types.Pointer(file),
			XAttrName:  types.Pointer(XAttrName),
			XAttrValue: types.Pointer(XAttrValue),
			XAttrFlag:  webhdfs.XAttrSetFlagCreate.New(),
		})
		if err != nil {
			t.Fatalf("webhdfs SetXAttr failed: %s", err)
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.GetXAttr(&webhdfs.GetXAttrRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			XAttrName: types.Pointer(XAttrName),
			//Encoding:  webhdfs.XAttrValueEncodingText.New(),
		})
		if err != nil {
			t.Fatalf("webhdfs GetXAttr failed: %s", err)
		}
		defer resp.Body.Close()
		t.Logf("XAttrs: %v", resp.XAttrs)

		for _, xattr := range resp.XAttrs {
			if xattr.Name != XAttrName {
				t.Errorf("XAttrName: got %s, want %s", xattr.Name, XAttrName)
			}
			if xattr.Value != fmt.Sprintf("%q", XAttrValue) {
				t.Errorf("XAttrValue: got %s, want %s", xattr.Value, XAttrValue)
			}
		}
	}()
	//    client_test.go:939: XAttrs: [{user.name "example"}]
}

func TestClient_GetXAttrs(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	XAttrName := webhdfs.XAttrNamespaceUser.String() + ".name"
	XAttrValue := "example"
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.SetXAttr(&webhdfs.SetXAttrRequest{
			ProxyUser:  c.ProxyUser(), // optional, user.name, The authenticated user
			Path:       types.Pointer(file),
			XAttrName:  types.Pointer(XAttrName),
			XAttrValue: types.Pointer(XAttrValue),
			XAttrFlag:  webhdfs.XAttrSetFlagCreate.New(),
		})
		if err != nil {
			t.Fatalf("webhdfs SetXAttr failed: %s", err)
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.GetXAttrs(&webhdfs.GetXAttrsRequest{
			ProxyUser:  c.ProxyUser(), // optional, user.name, The authenticated user
			Path:       types.Pointer(file),
			XAttrNames: []string{webhdfs.XAttrNamespaceUser.String() + ".name"},
			//Encoding:   webhdfs.XAttrValueEncodingText.New(),
		})
		if err != nil {
			t.Fatalf("webhdfs GetXAttrs failed: %s", err)
		}
		defer resp.Body.Close()
		t.Logf("XAttrs: %v", resp.XAttrs)

		for _, xattr := range resp.XAttrs {
			if xattr.Name != XAttrName {
				t.Errorf("XAttrName: got %s, want %s", xattr.Name, XAttrName)
			}
			if xattr.Value != fmt.Sprintf("%q", XAttrValue) {
				t.Errorf("XAttrValue: got %s, want %s", xattr.Value, XAttrValue)
			}
		}
	}()
	//    client_test.go:997: XAttrs: [{user.name "example"}]
}

func TestClient_GetAllXAttrs(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	XAttrName := webhdfs.XAttrNamespaceUser.String() + ".name"
	XAttrValue := "example"
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.SetXAttr(&webhdfs.SetXAttrRequest{
			ProxyUser:  c.ProxyUser(), // optional, user.name, The authenticated user
			Path:       types.Pointer(file),
			XAttrName:  types.Pointer(XAttrName),
			XAttrValue: types.Pointer(XAttrValue),
			XAttrFlag:  webhdfs.XAttrSetFlagCreate.New(),
		})
		if err != nil {
			t.Fatalf("webhdfs SetXAttr failed: %s", err)
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.GetAllXAttrs(&webhdfs.GetAllXAttrsRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			//Encoding:  webhdfs.XAttrValueEncodingText.New(),
		})
		if err != nil {
			t.Fatalf("webhdfs GetAllXAttrs failed: %s", err)
		}
		defer resp.Body.Close()
		t.Logf("XAttrs: %v", resp.XAttrs)

		for _, xattr := range resp.XAttrs {
			if xattr.Name != XAttrName {
				t.Errorf("XAttrName: got %s, want %s", xattr.Name, XAttrName)
			}
			if xattr.Value != fmt.Sprintf("%q", XAttrValue) {
				t.Errorf("XAttrValue: got %s, want %s", xattr.Value, XAttrValue)
			}
		}
	}()
	//    client_test.go:1053: XAttrs: [{user.name "example"}]
}

func TestClient_ListXAttrs(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	XAttrName := webhdfs.XAttrNamespaceUser.String() + ".name"
	XAttrValue := "example"
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.SetXAttr(&webhdfs.SetXAttrRequest{
			ProxyUser:  c.ProxyUser(), // optional, user.name, The authenticated user
			Path:       types.Pointer(file),
			XAttrName:  types.Pointer(XAttrName),
			XAttrValue: types.Pointer(XAttrValue),
			XAttrFlag:  webhdfs.XAttrSetFlagCreate.New(),
		})
		if err != nil {
			t.Fatalf("webhdfs SetXAttr failed: %s", err)
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.ListXAttrs(&webhdfs.ListXAttrsRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs ListXAttrs failed: %s", err)
		}
		defer resp.Body.Close()
		t.Logf("XAttrNames: %v", resp.XAttrNames)

		data, err := json.MarshalIndent(resp.XAttrNames, "", "\t")
		if err != nil {
			t.Fatalf("webhdfs ListXAttrs json Marshal failed: %s", err)
		}
		t.Logf("Encoded XAttrNames: %v", string(data))

		for _, name := range resp.XAttrNames {
			if name != XAttrName {
				t.Errorf("XAttrName: got %s, want %s", name, XAttrName)
			}
		}
	}()
	//    client_test.go:1108: XAttrNames: [user.name]
	//    client_test.go:1110: Encoded XAttrNames: "[\"user.name\"]"
}

func TestClient_CheckAccess(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()

	func() {
		resp, err := c.CheckAccess(&webhdfs.CheckAccessRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Fsaction:  types.Pointer("rwx"),
		})
		if err != nil {
			t.Fatalf("webhdfs CheckAccess failed: %s", err)
		}
		defer resp.Body.Close()
	}()
	// CheckAccess is not implemented on Hadoop 3.2.1
	//    client_test.go:1151: webhdfs CheckAccess failed: QueryParamException: java.lang.IllegalArgumentException: No enum constant org.apache.hadoop.fs.http.client.HttpFSFileSystem.Operation.CHECKACCESS in com.sun.jersey.api.ParamException$QueryParamException
}

func TestClient_GetAllStoragePolicy(t *testing.T) {
	c := getWebHDFSClient(t)
	resp, err := c.GetAllStoragePolicy(&webhdfs.GetAllStoragePolicyRequest{
		ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user

	})
	if err != nil {
		t.Fatalf("webhdfs GetAllStoragePolicy failed: %s", err)
	}
	defer resp.Body.Close()

	data, err := json.MarshalIndent(resp.BlockStoragePolicies, "", "\t")
	if err != nil {
		t.Fatalf("webhdfs GetAllStoragePolicy json Marshal failed: %s", err)
	}
	t.Logf("GetAllStoragePolicy: %v", string(data))

	//    client_test.go:1172: GetAllStoragePolicy: {
	//        	"BlockStoragePolicy": [
	//        		{
	//        			"id": 1,
	//        			"name": "PROVIDED",
	//        			"storageTypes": [
	//        				"PROVIDED",
	//        				"DISK"
	//        			],
	//        			"replicationFallbacks": [
	//        				"PROVIDED",
	//        				"DISK"
	//        			],
	//        			"creationFallbacks": [
	//        				"PROVIDED",
	//        				"DISK"
	//        			],
	//        			"copyOnCreateFile": false
	//        		},
	//        		{
	//        			"id": 2,
	//        			"name": "COLD",
	//        			"storageTypes": [
	//        				"ARCHIVE"
	//        			],
	//        			"replicationFallbacks": [],
	//        			"creationFallbacks": [],
	//        			"copyOnCreateFile": false
	//        		},
	//        		{
	//        			"id": 5,
	//        			"name": "WARM",
	//        			"storageTypes": [
	//        				"DISK",
	//        				"ARCHIVE"
	//        			],
	//        			"replicationFallbacks": [
	//        				"DISK",
	//        				"ARCHIVE"
	//        			],
	//        			"creationFallbacks": [
	//        				"DISK",
	//        				"ARCHIVE"
	//        			],
	//        			"copyOnCreateFile": false
	//        		},
	//        		{
	//        			"id": 7,
	//        			"name": "HOT",
	//        			"storageTypes": [
	//        				"DISK"
	//        			],
	//        			"replicationFallbacks": [
	//        				"ARCHIVE"
	//        			],
	//        			"creationFallbacks": [],
	//        			"copyOnCreateFile": false
	//        		},
	//        		{
	//        			"id": 10,
	//        			"name": "ONE_SSD",
	//        			"storageTypes": [
	//        				"SSD",
	//        				"DISK"
	//        			],
	//        			"replicationFallbacks": [
	//        				"SSD",
	//        				"DISK"
	//        			],
	//        			"creationFallbacks": [
	//        				"SSD",
	//        				"DISK"
	//        			],
	//        			"copyOnCreateFile": false
	//        		},
	//        		{
	//        			"id": 12,
	//        			"name": "ALL_SSD",
	//        			"storageTypes": [
	//        				"SSD"
	//        			],
	//        			"replicationFallbacks": [
	//        				"DISK"
	//        			],
	//        			"creationFallbacks": [
	//        				"DISK"
	//        			],
	//        			"copyOnCreateFile": false
	//        		},
	//        		{
	//        			"id": 15,
	//        			"name": "LAZY_PERSIST",
	//        			"storageTypes": [
	//        				"RAM_DISK",
	//        				"DISK"
	//        			],
	//        			"replicationFallbacks": [
	//        				"DISK"
	//        			],
	//        			"creationFallbacks": [
	//        				"DISK"
	//        			],
	//        			"copyOnCreateFile": false
	//        		}
	//        	]
	//        }
}

func TestClient_GetStoragePolicy(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.GetStoragePolicy(&webhdfs.GetStoragePolicyRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs GetStoragePolicy failed: %s", err)
		}
		defer resp.Body.Close()

		data, err := json.MarshalIndent(resp.BlockStoragePolicy, "", "\t")
		if err != nil {
			t.Fatalf("webhdfs GetStoragePolicy json Marshal failed: %s", err)
		}
		t.Logf("GetStoragePolicy: %v", string(data))
	}()
	//    client_test.go:1318: GetStoragePolicy: {
	//        	"BlockStoragePolicy": {
	//        		"id": 7,
	//        		"name": "HOT",
	//        		"storageTypes": [
	//        			"DISK"
	//        		],
	//        		"replicationFallbacks": [
	//        			"ARCHIVE"
	//        		],
	//        		"creationFallbacks": [],
	//        		"copyOnCreateFile": false
	//        	}
	//        }
}

func TestClient_GetSnapshotDiff(t *testing.T) {
	c := getWebHDFSClient(t)
	dir := HdfsBucket + "/test"
	func() {
		func() {
			resp, err := c.Mkdirs(&webhdfs.MkdirsRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				Path:      types.Pointer(dir),
			})
			if err != nil {
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.AllowSnapshot(&webhdfs.AllowSnapshotRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				Path:      types.Pointer(dir),
			})
			if err != nil {
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.DeleteSnapshot(&webhdfs.DeleteSnapshotRequest{
				ProxyUser:    c.ProxyUser(), // optional, user.name, The authenticated user
				Path:         types.Pointer(dir),
				Snapshotname: types.Pointer("snapshot.old"),
			})
			if err != nil {
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.DeleteSnapshot(&webhdfs.DeleteSnapshotRequest{
				ProxyUser:    c.ProxyUser(), // optional, user.name, The authenticated user
				Path:         types.Pointer(dir),
				Snapshotname: types.Pointer("snapshot.new"),
			})
			if err != nil {
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.CreateSnapshot(&webhdfs.CreateSnapshotRequest{
				ProxyUser:    c.ProxyUser(), // optional, user.name, The authenticated user
				Path:         types.Pointer(dir),
				Snapshotname: types.Pointer("snapshot.old"),
			})
			if err != nil {
				t.Fatalf("webhdfs CreateSnapshot failed: %s", err)
			}
			defer resp.Body.Close()
			t.Logf("snapshot.old created: %s", types.Value(resp.Path))
		}()
		func() {
			resp, err := c.Create(&webhdfs.CreateRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				Path:      types.Pointer(path.Join(dir, "found.txt")),
				Body:      strings.NewReader("Hello World!"),
				Overwrite: types.Pointer(true),
			})
			if err != nil {
				t.Fatalf("webhdfs Create failed: %s", err)
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.CreateSnapshot(&webhdfs.CreateSnapshotRequest{
				ProxyUser:    c.ProxyUser(), // optional, user.name, The authenticated user
				Path:         types.Pointer(dir),
				Snapshotname: types.Pointer("snapshot.new"),
			})
			if err != nil {
				return
			}
			defer resp.Body.Close()
			t.Logf("snapshot.new created: %s", types.Value(resp.Path))
		}()
	}()
	func() {
		resp, err := c.GetSnapshotDiff(&webhdfs.GetSnapshotDiffRequest{
			ProxyUser:       c.ProxyUser(), // optional, user.name, The authenticated user
			Path:            types.Pointer(dir),
			Oldsnapshotname: types.Pointer("snapshot.old"),
			Snapshotname:    types.Pointer("snapshot.new"),
		})
		if err != nil {
			t.Fatalf("webhdfs GetSnapshotDiff failed: %s", err)
		}
		defer resp.Body.Close()

		data, err := json.MarshalIndent(resp.SnapshotDiffReport, "", "\t")
		if err != nil {
			t.Fatalf("webhdfs SnapshotDiffReport json Marshal failed: %s", err)
		}
		t.Logf("SnapshotDiffReport: %v", string(data))
	}()
	//    client_test.go:1370: snapshot.old created: /test.bucket/test/.snapshot/snapshot.old
	//    client_test.go:1395: snapshot.new created: /test.bucket/test/.snapshot/snapshot.new
	//    client_test.go:1414: SnapshotDiffReport: {
	//        	"diffList": [
	//        		{
	//        			"sourcePath": "",
	//        			"targetPath": "",
	//        			"type": "MODIFY"
	//        		},
	//        		{
	//        			"sourcePath": "found.txt",
	//        			"targetPath": "",
	//        			"type": "CREATE"
	//        		},
	//        		{
	//        			"sourcePath": "found.txt",
	//        			"targetPath": "",
	//        			"type": "DELETE"
	//        		}
	//        	],
	//        	"fromSnapshot": "snapshot.old",
	//        	"snapshotRoot": "/test.bucket/test",
	//        	"toSnapshot": "snapshot.new"
	//        }
}

func TestClient_GetSnapshottableDirectoryList(t *testing.T) {
	c := getWebHDFSClient(t)
	dir := HdfsBucket + "/test"
	func() {
		func() {
			resp, err := c.Mkdirs(&webhdfs.MkdirsRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				Path:      types.Pointer(dir),
			})
			if err != nil {
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.AllowSnapshot(&webhdfs.AllowSnapshotRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				Path:      types.Pointer(dir),
			})
			if err != nil {
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.DeleteSnapshot(&webhdfs.DeleteSnapshotRequest{
				ProxyUser:    c.ProxyUser(), // optional, user.name, The authenticated user
				Path:         types.Pointer(dir),
				Snapshotname: types.Pointer("snapshot.old"),
			})
			if err != nil {
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.DeleteSnapshot(&webhdfs.DeleteSnapshotRequest{
				ProxyUser:    c.ProxyUser(), // optional, user.name, The authenticated user
				Path:         types.Pointer(dir),
				Snapshotname: types.Pointer("snapshot.new"),
			})
			if err != nil {
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.CreateSnapshot(&webhdfs.CreateSnapshotRequest{
				ProxyUser:    c.ProxyUser(), // optional, user.name, The authenticated user
				Path:         types.Pointer(dir),
				Snapshotname: types.Pointer("snapshot.old"),
			})
			if err != nil {
				t.Fatalf("webhdfs CreateSnapshot failed: %s", err)
			}
			defer resp.Body.Close()
			t.Logf("snapshot.old created: %s", types.Value(resp.Path))
		}()
		func() {
			resp, err := c.Create(&webhdfs.CreateRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				Path:      types.Pointer(path.Join(dir, "found.txt")),
				Body:      strings.NewReader("Hello World!"),
				Overwrite: types.Pointer(true),
			})
			if err != nil {
				t.Fatalf("webhdfs Create failed: %s", err)
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.CreateSnapshot(&webhdfs.CreateSnapshotRequest{
				ProxyUser:    c.ProxyUser(), // optional, user.name, The authenticated user
				Path:         types.Pointer(dir),
				Snapshotname: types.Pointer("snapshot.new"),
			})
			if err != nil {
				return
			}
			defer resp.Body.Close()
			t.Logf("snapshot.new created: %s", types.Value(resp.Path))
		}()
	}()
	func() {
		resp, err := c.GetSnapshottableDirectoryList(&webhdfs.GetSnapshottableDirectoryListRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Username:  c.ProxyUser().Username,
		})
		if err != nil {
			t.Fatalf("webhdfs GetSnapshottableDirectoryList failed: %s", err)
		}
		defer resp.Body.Close()

		data, err := json.MarshalIndent(resp.SnapshottableDirectoryList, "", "\t")
		if err != nil {
			t.Fatalf("webhdfs SnapshottableDirectoryList json Marshal failed: %s", err)
		}
		t.Logf("SnapshottableDirectoryList: %v", string(data))
	}()
	//    client_test.go:1498: snapshot.old created: /test.bucket/test/.snapshot/snapshot.old
	//    client_test.go:1523: snapshot.new created: /test.bucket/test/.snapshot/snapshot.new
	//    client_test.go:1540: SnapshottableDirectoryList: [
	//        	{
	//        		"dirStatus": {
	//        			"accessTime": 0,
	//        			"blockSize": 0,
	//        			"childrenNum": 2,
	//        			"fileId": 33861,
	//        			"group": "supergroup",
	//        			"length": 0,
	//        			"modificationTime": -8291856888039309312,
	//        			"owner": "root",
	//        			"pathSuffix": "test",
	//        			"permission": "755",
	//        			"replication": 0,
	//        			"symlink": "",
	//        			"type": "DIRECTORY"
	//        		},
	//        		"parentFullPath": "/test.bucket",
	//        		"snapshotNumber": 3,
	//        		"snapshotQuota": 65536
	//        	}
	//        ]
}

func TestClient_GetFileBlockLocations(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()

	func() {
		resp, err := c.GetFileBlockLocations(&webhdfs.GetFileBlockLocationsRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs GetFileBlockLocations failed: %s", err)
		}
		defer resp.Body.Close()
		t.Logf("BlockLocations: %v", resp.BlockLocations)
	}()
	// GETFILEBLOCKLOCATIONS operation is not implemented on HttpFS Hadoop 3.2.1
	// See : https://issues.apache.org/jira/browse/HDFS-12457
	// GETFILEBLOCKLOCATIONS operation is reverted, with Affects Version/s: 3.0.0-alpha2 Fix Version/s: 3.0.0-beta1
	// See also: https://issues.apache.org/jira/browse/HDFS-6874
	// client_test.go:273: webhdfs GetAllStoragePolicy failed: IllegalArgumentException: Invalid value for webhdfs parameter "op": No enum constant org.apache.hadoop.hdfs.web.resources.GetOpParam.Op.GETFILEBLOCKLOCATIONS in java.lang.IllegalArgumentException
}

func TestClient_GetECPolicy(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.GetECPolicy(&webhdfs.GetECPolicyRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer("/data/test/core-site.xml"),
		})
		if err != nil {
			t.Fatalf("webhdfs GetFileBlockLocations failed: %s", err)
		}
		defer resp.Body.Close()
		t.Logf("ECPolicy: %v", resp.ECPolicy)
	}()
	// GETECPOLICY operation is not implemented on WebHDFS Hadoop 2.6.1
	//    client_test.go:285: webhdfs GetFileBlockLocations failed: IllegalArgumentException: Invalid value for webhdfs parameter "op": No enum constant org.apache.hadoop.hdfs.web.resources.GetOpParam.Op.GETECPOLICY in java.lang.IllegalArgumentException

	// GETECPOLICY operation is not implemented on HttpFS Hadoop 3.2.1
	//    client_test.go:1629: webhdfs GetECPolicy failed: QueryParamException: java.lang.IllegalArgumentException: No enum constant org.apache.hadoop.fs.http.client.HttpFSFileSystem.Operation.GETECPOLICY in com.sun.jersey.api.ParamException$QueryParamException
}

func TestClient_Create_File_Overwrite(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	func() {
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Open(&webhdfs.OpenRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Open failed: %s", err)
			return
		}
		defer resp.Body.Close()
		readData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("webhdfs Open and Read failed: %s", err)
		}

		if bytes.Compare([]byte(writtenData), readData) != 0 {
			t.Fatalf("%s, expected %q, got %q", file, writtenData, readData)
		}
		t.Logf("%s, written %q, read %q", file, writtenData, readData)
	}()
	//  Output:
	//    client_test.go:1677: test.bucket/test/found.txt, written "Hello World!", read "Hello World!"
}

func TestClient_Create_File_AlreadyExist(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	func() {
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(false),
		})
		if err != nil {
			if errors.Is(err, os.ErrExist) {
				t.Logf("webhdfs Create failed: %s", err)
			} else {
				t.Fatalf("webhdfs Create failed: %s", err)
			}
			return
		}
		defer resp.Body.Close()
	}()
	//  Output:
	//    client_test.go:1743: webhdfs Create failed: FileAlreadyExistsException: /test.bucket/test/found.txt for client 10.22.0.30 already exists in org.apache.hadoop.fs.FileAlreadyExistsException
}

func TestClient_Create_Dir(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test.create_dir/"
	writtenData := "Hello World!"
	func() {
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()

	func() { //test.bucket/test.create_dir/
		resp, err := c.Open(&webhdfs.OpenRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Open failed: %s", err)
			return
		}
		defer resp.Body.Close()
		readData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("webhdfs Open and Read failed: %s", err)
		}

		if bytes.Compare([]byte(writtenData), readData) != 0 {
			t.Fatalf("%s, expected %q, got %q", file, writtenData, readData)
		}
		t.Logf("%s, written %q, read %q", file, writtenData, readData)
	}()

	func() { //test.bucket/test.create_dir
		resp, err := c.Open(&webhdfs.OpenRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(path.Clean(file)),
		})
		if err != nil {
			t.Fatalf("webhdfs Open failed: %s", err)
			return
		}
		defer resp.Body.Close()
		readData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("webhdfs Open and Read failed: %s", err)
		}

		if bytes.Compare([]byte(writtenData), readData) != 0 {
			t.Fatalf("%s, expected %q, got %q", path.Clean(file), writtenData, readData)
		}
		t.Logf("%s, written %q, read %q", path.Clean(file), writtenData, readData)
	}()

	//  Output:
	//    client_test.go:1802: test.bucket/test.create_dir/, written "Hello World!", read "Hello World!"
	//    client_test.go:1823: test.bucket/test.create_dir, written "Hello World!", read "Hello World!"
}

func TestClient_Mkdirs(t *testing.T) {
	c := getWebHDFSClient(t)
	dir := HdfsBucket + "/test/mkdirs"
	func() {
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(dir),
			Recursive: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	{
		resp, err := c.Mkdirs(&webhdfs.MkdirsRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(dir),
		})
		if err != nil {
			t.Fatalf("webhdfs Mkdirs failed: %s", err)
		}
		defer resp.Body.Close()
		t.Logf("Boolean: %t", resp.Boolean)
		if !resp.Boolean {
			t.Fatalf("%s, expected %t, got %t", dir, true, resp.Boolean)
		}
	}

	{
		resp, err := c.GetFileStatus(&webhdfs.GetFileStatusRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(dir),
		})
		if err != nil {
			t.Fatalf("webhdfs Open failed: %s", err)
		}
		defer resp.Body.Close()
		fi := resp.FileStatus
		t.Logf("Dir(%t), %s, %d, %s", fi.IsDir(), fi.Name(), fi.Size(), fi.ModTime())
	}
	//    client_test.go:1854: Boolean: true
	//    client_test.go:1870: Dir(true), mkdirs, 0, 2021-01-29 17:52:24.61 +0800 CST
}

func TestClient_CreateSymlink(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	link := HdfsBucket + "/test/found.link.txt"
	writtenData := "Hello World!"
	func() {
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	{
		resp, err := c.CreateSymlink(&webhdfs.CreateSymlinkRequest{
			ProxyUser:   c.ProxyUser(), // optional, user.name, The authenticated user
			Path:        types.Pointer(file),
			Destination: types.Pointer(link),
		})
		if err != nil {
			t.Fatalf("webhdfs CreateSymlink failed: %s", err)
		}
		defer resp.Body.Close()
	}

	{
		resp, err := c.GetFileStatus(&webhdfs.GetFileStatusRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(link),
		})
		if err != nil {
			t.Fatalf("webhdfs GetFileStatus failed: %s", err)
		}
		defer resp.Body.Close()
		fi := resp.FileStatus
		t.Logf("Dir(%t), %s, %d, %s", fi.IsDir(), fi.Name(), fi.Size(), fi.ModTime())
	}

	func() {
		resp, err := c.Open(&webhdfs.OpenRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(link),
		})
		if err != nil {
			t.Fatalf("webhdfs Open failed: %s", err)
			return
		}
		defer resp.Body.Close()
		readData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("webhdfs Open and Read failed: %s", err)
		}

		if bytes.Compare([]byte(writtenData), readData) != 0 {
			t.Fatalf("%s, expected %q, got %q", file, writtenData, readData)
		}
		t.Logf("%s, written %q, read %q", file, writtenData, readData)
	}()

	// CREATESYMLINK operation is not implemented on WebHDFS Hadoop 2.6.1
	//    client_test.go:376: webhdfs CreateSymlink failed: UnsupportedOperationException: Symlinks not supported in java.lang.UnsupportedOperationException
	// CREATESYMLINK operation is not implemented on HttpFS Hadoop 3.2.1
	//    client_test.go:1913: webhdfs CreateSymlink failed: QueryParamException: java.lang.IllegalArgumentException: No enum constant org.apache.hadoop.fs.http.client.HttpFSFileSystem.Operation.CREATESYMLINK in com.sun.jersey.api.ParamException$QueryParamException
}

func TestClient_Rename(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	link := HdfsBucket + "/test/found.rename.txt"
	writtenData := "Hello World!"
	func() {
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Open(&webhdfs.OpenRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Open failed: %s", err)
			return
		}
		defer resp.Body.Close()
		readData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("webhdfs Open and Read failed: %s", err)
		}

		if bytes.Compare([]byte(writtenData), readData) != 0 {
			t.Fatalf("%s, expected %q, got %q", file, writtenData, readData)
		}
	}()

	var renamed bool
	func() {
		resp, err := c.Rename(&webhdfs.RenameRequest{
			ProxyUser:   c.ProxyUser(), // optional, user.name, The authenticated user
			Path:        types.Pointer(file),
			Destination: types.Pointer(link),
		})
		if err != nil {
			t.Fatalf("webhdfs CreateSymlink failed: %s", err)
		}
		defer resp.Body.Close()
		t.Logf("Boolean: %t", resp.Boolean)
		renamed = resp.Boolean
	}()
	func() {
		if renamed {
			func() {
				resp, err := c.GetFileStatus(&webhdfs.GetFileStatusRequest{
					ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
					Path:      types.Pointer(link),
				})
				if err != nil {
					t.Fatalf("webhdfs GetFileStatus failed: %s", err)
				}
				defer resp.Body.Close()
				fi := resp.FileStatus
				t.Logf("Dir(%t), %s, %d, %s", fi.IsDir(), fi.Name(), fi.Size(), fi.ModTime())
			}()

			func() {
				resp, err := c.Open(&webhdfs.OpenRequest{
					ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
					Path:      types.Pointer(link),
				})
				if err != nil {
					t.Fatalf("webhdfs Open failed: %s", err)
					return
				}
				defer resp.Body.Close()
				readData, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					t.Fatalf("webhdfs Open and Read failed: %s", err)
				}

				if bytes.Compare([]byte(writtenData), readData) != 0 {
					t.Fatalf("%s, expected %q, got %q", file, writtenData, readData)
				}
				t.Logf("%s, written %q, read %q", file, writtenData, readData)
			}()
		}
	}()

	//    client_test.go:2018: Boolean: false
}

func TestClient_SetReplication(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	func() {
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.SetReplication(&webhdfs.SetReplicationRequest{
			ProxyUser:   c.ProxyUser(), // optional, user.name, The authenticated user
			Path:        types.Pointer(file),
			Replication: types.Pointer(2),
		})
		if err != nil {
			t.Fatalf("webhdfs Open failed: %s", err)
		}
		defer resp.Body.Close()
		t.Logf("Boolean: %t", resp.Boolean)
	}()
	//    client_test.go:2100: Boolean: true
}

func TestClient_SetOwner(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	owner := "hdfs"
	func() {
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.SetOwner(&webhdfs.SetOwnerRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Owner:     types.Pointer(owner),
		})
		if err != nil {
			t.Fatalf("webhdfs SetOwner failed: %s", err)
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.ListStatus(&webhdfs.ListStatusRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs ListStatus failed: %s", err)
		}
		defer resp.Body.Close()
		fileStatuses := resp.FileStatuses.FileStatus
		for i, fi := range fileStatuses {
			t.Logf("[%d] Dir(%t), %s, %d, %s, %s", i, fi.IsDir(), fi.Name(), fi.Size(), fi.ModTime(), fi.Owner)
		}

		if len(fileStatuses) != 1 {
			t.Fatalf("len(FileStatus): got %d, want %d", len(fileStatuses), 1)
		}
		fi := fileStatuses[0]
		if fi.Owner != owner {
			t.Errorf("Owner: got %s, want %s", fi.Owner, owner)
		}
	}()
	//    client_test.go:2156: [0] Dir(false), found.txt, 12, 2021-01-29 20:15:40.663 +0800 CST, hdfs
}

func TestClient_SetPermission(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	perm := webhdfs.Permission(0666)
	func() {
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.SetPermission(&webhdfs.SetPermissionRequest{
			ProxyUser:  c.ProxyUser(), // optional, user.name, The authenticated user
			Path:       types.Pointer(file),
			Permission: perm.New(),
		})
		if err != nil {
			t.Fatalf("webhdfs SetPermission failed: %s", err)
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.ListStatus(&webhdfs.ListStatusRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs ListStatus failed: %s", err)
		}
		defer resp.Body.Close()
		fileStatuses := resp.FileStatuses.FileStatus
		for i, fi := range fileStatuses {
			t.Logf("[%d] Dir(%t), %s, %d, %s, %s", i, fi.IsDir(), fi.Name(), fi.Size(), fi.ModTime(), fi.Permission)
		}

		if len(fileStatuses) != 1 {
			t.Fatalf("len(FileStatus): got %d, want %d", len(fileStatuses), 1)
		}
		fi := fileStatuses[0]
		if fi.Permission != perm {
			t.Errorf("Owner: got %s, want %s", fi.Permission, perm)
		}
	}()
	//    client_test.go:2221: [0] Dir(false), found.txt, 12, 2021-01-29 20:15:57.264 +0800 CST, 0666
}

func TestClient_SetTimes(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	now := time.Now()
	func() {
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.SetTimes(&webhdfs.SetTimesRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Modificationtime: &time_.UnixTimeMillisecond{
				Time: now,
			},
		})
		if err != nil {
			t.Fatalf("webhdfs SetTimes failed: %s", err)
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.ListStatus(&webhdfs.ListStatusRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs ListStatus failed: %s", err)
		}
		defer resp.Body.Close()
		fileStatuses := resp.FileStatuses.FileStatus
		for i, fi := range fileStatuses {
			t.Logf("[%d] Dir(%t), %s, %d, %s", i, fi.IsDir(), fi.Name(), fi.Size(), fi.ModTime())
		}

		if len(fileStatuses) != 1 {
			t.Fatalf("len(FileStatus): got %d, want %d", len(fileStatuses), 1)
		}
		fi := fileStatuses[0]

		gotTime := time_.UnixTimeMillisecond{Time: now}
		if fi.ModTime().Unix() != gotTime.Unix() {
			t.Errorf("ModTime: got %s, want %s", fi.ModTime(), now)
		}
	}()
	//    client_test.go:2290: [0] Dir(false), found.txt, 12, 2021-01-29 20:46:15.478 +0800 CST
}

func TestClient_RenewDelegationToken(t *testing.T) {
	c := getWebHDFSClient(t)
	var token string
	func() {
		resp, err := c.GetDelegationToken(&webhdfs.GetDelegationTokenRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
		})
		if err != nil {
			t.Fatalf("webhdfs GetDelegationToken failed: %s", err)
		}
		token = resp.Token.UrlString
		t.Logf("token: %s", token)
	}()
	func() {
		resp, err := c.RenewDelegationToken(&webhdfs.RenewDelegationTokenRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Token:     types.Pointer(token),
		})
		if err != nil {
			t.Fatalf("webhdfs RenewDelegationToken failed: %s", err)
		}
		defer resp.Body.Close()
		t.Logf("Long: %s", resp.Long)
		t.Logf("Expire At: %s", resp.Long.Time)
	}()
	//    client_test.go:2317: token: HAAEcm9vdARyb290AIoBd040DmaKAXdyQJJmCAIUJSNUrccvJjR394ICBKwZwGdxD9YSV0VCSERGUyBkZWxlZ2F0aW9uAA
	//    client_test.go:2328: Long: 1612011170454
	//    client_test.go:2329: Expire At: 2021-01-30 20:52:50.454 +0800 CST
}

func TestClient_CancelDelegationToken(t *testing.T) {
	c := getWebHDFSClient(t)
	var token string
	func() {
		resp, err := c.GetDelegationToken(&webhdfs.GetDelegationTokenRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
		})
		if err != nil {
			t.Fatalf("webhdfs GetDelegationToken failed: %s", err)
		}
		token = resp.Token.UrlString
		t.Logf("token: %s", token)
	}()
	func() {
		resp, err := c.CancelDelegationToken(&webhdfs.CancelDelegationTokenRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Token:     types.Pointer(token),
		})
		if err != nil {
			t.Fatalf("webhdfs CancelDelegationToken failed: %s", err)
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.RenewDelegationToken(&webhdfs.RenewDelegationTokenRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Token:     types.Pointer(token),
		})
		if err != nil {
			t.Logf("webhdfs RenewDelegationToken failed: %s", err)
			return
		}
		defer resp.Body.Close()
		t.Errorf("Long: %s", resp.Long)
		t.Errorf("Expire At: %s", resp.Long.Time)
	}()
	//    client_test.go:2347: token: HAAEcm9vdARyb290AIoBd042UqiKAXdyQtaoCgIUAfZ15aXsZjfotrtSwpMHrt7fAfsSV0VCSERGUyBkZWxlZ2F0aW9uAA
	//    client_test.go:2365: webhdfs RenewDelegationToken failed: unexpected http status code: 403 Forbidden
}

func TestClient_AllowSnapshot(t *testing.T) {
	c := getWebHDFSClient(t)
	dir := HdfsBucket + "/test"
	snapshot := "snapshot"
	func() {
		func() {
			resp, err := c.Mkdirs(&webhdfs.MkdirsRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				Path:      types.Pointer(dir),
			})
			if err != nil {
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.AllowSnapshot(&webhdfs.AllowSnapshotRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				Path:      types.Pointer(dir),
			})
			if err != nil {
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.DeleteSnapshot(&webhdfs.DeleteSnapshotRequest{
				ProxyUser:    c.ProxyUser(), // optional, user.name, The authenticated user
				Path:         types.Pointer(dir),
				Snapshotname: types.Pointer(snapshot),
			})
			if err != nil {
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.CreateSnapshot(&webhdfs.CreateSnapshotRequest{
				ProxyUser:    c.ProxyUser(), // optional, user.name, The authenticated user
				Path:         types.Pointer(dir),
				Snapshotname: types.Pointer(snapshot),
			})
			if err != nil {
				t.Fatalf("webhdfs CreateSnapshot failed: %s", err)
			}
			defer resp.Body.Close()
			t.Logf("%s created: %s", snapshot, types.Value(resp.Path))
		}()
	}()
	//    client_test.go:2422: snapshot created: /test.bucket/test/.snapshot/snapshot
}

func TestClient_DisallowSnapshot(t *testing.T) {
	c := getWebHDFSClient(t)
	dir := HdfsBucket + "/test" + strconv.Itoa(time.Now().Nanosecond())
	snapshot := "snapshot"
	func() {
		func() {
			resp, err := c.Delete(&webhdfs.DeleteRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				Path:      types.Pointer(dir),
				Recursive: types.Pointer(true),
			})
			if err != nil {
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.Mkdirs(&webhdfs.MkdirsRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				Path:      types.Pointer(dir),
			})
			if err != nil {
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.DisallowSnapshot(&webhdfs.DisallowSnapshotRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				Path:      types.Pointer(dir),
			})
			if err != nil {
				t.Fatalf("webhdfs DisallowSnapshot failed: %s", err)
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.CreateSnapshot(&webhdfs.CreateSnapshotRequest{
				ProxyUser:    c.ProxyUser(), // optional, user.name, The authenticated user
				Path:         types.Pointer(dir),
				Snapshotname: types.Pointer(snapshot),
			})
			if err != nil {
				t.Logf("webhdfs CreateSnapshot failed: %s", err)
				return
			}
			defer resp.Body.Close()
			t.Errorf("%s created: %s", snapshot, types.Value(resp.Path))
		}()
	}()
	//    client_test.go:2473: webhdfs CreateSnapshot failed: SnapshotException: Directory is not a snapshottable directory: /test.bucket/test663134000 in org.apache.hadoop.hdfs.protocol.SnapshotException
}

func TestClient_DisallowSnapshot_SubDir(t *testing.T) {
	c := getWebHDFSClient(t)
	dir := HdfsBucket + "/test" + strconv.Itoa(time.Now().Nanosecond())
	snapshot := "snapshot"
	func() {
		func() {
			resp, err := c.Delete(&webhdfs.DeleteRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				Path:      types.Pointer(dir),
				Recursive: types.Pointer(true),
			})
			if err != nil {
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.Mkdirs(&webhdfs.MkdirsRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				Path:      types.Pointer(dir),
			})
			if err != nil {
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.AllowSnapshot(&webhdfs.AllowSnapshotRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				Path:      types.Pointer(dir),
			})
			if err != nil {
				t.Logf("webhdfs AllowSnapshot failed: %s", err)
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.CreateSnapshot(&webhdfs.CreateSnapshotRequest{
				ProxyUser:    c.ProxyUser(), // optional, user.name, The authenticated user
				Path:         types.Pointer(dir),
				Snapshotname: types.Pointer(snapshot),
			})
			if err != nil {
				t.Fatalf("webhdfs CreateSnapshot failed: %s", err)
				return
			}
			defer resp.Body.Close()
			t.Logf("%s created: %s", snapshot, types.Value(resp.Path))
		}()
		func() {
			resp, err := c.DisallowSnapshot(&webhdfs.DisallowSnapshotRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				Path:      types.Pointer(dir),
			})
			if err != nil {
				t.Logf("webhdfs DisallowSnapshot failed: %s", err)
				return
			}
			defer resp.Body.Close()
			t.Fatalf("webhdfs DisallowSnapshot succeed unexpected")
		}()
	}()
	//    client_test.go:2531: snapshot created: /test.bucket/test637280000/.snapshot/snapshot
	//    client_test.go:2539: webhdfs DisallowSnapshot failed: SnapshotException: The directory /test.bucket/test637280000 has snapshot(s). Please redo the operation after removing all the snapshots. in org.apache.hadoop.hdfs.protocol.SnapshotException
}

func TestClient_CreateSnapshot(t *testing.T) {
	c := getWebHDFSClient(t)
	dir := HdfsBucket + "/test" + strconv.Itoa(time.Now().Nanosecond())
	snapshot := "snapshot"
	func() {
		func() {
			resp, err := c.Delete(&webhdfs.DeleteRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				Path:      types.Pointer(dir),
				Recursive: types.Pointer(true),
			})
			if err != nil {
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.Mkdirs(&webhdfs.MkdirsRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				Path:      types.Pointer(dir),
			})
			if err != nil {
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.AllowSnapshot(&webhdfs.AllowSnapshotRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				Path:      types.Pointer(dir),
			})
			if err != nil {
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.CreateSnapshot(&webhdfs.CreateSnapshotRequest{
				ProxyUser:    c.ProxyUser(), // optional, user.name, The authenticated user
				Path:         types.Pointer(dir),
				Snapshotname: types.Pointer(snapshot),
			})
			if err != nil {
				t.Fatalf("webhdfs CreateSnapshot failed: %s", err)
				return
			}
			defer resp.Body.Close()
			t.Logf("%s created: %s", snapshot, types.Value(resp.Path))
		}()
	}()
	//    client_test.go:2597: snapshot created: /test.bucket/test582766000/.snapshot/snapshot
}

func TestClient_RenameSnapshot(t *testing.T) {
	c := getWebHDFSClient(t)
	dir := HdfsBucket + "/test" + strconv.Itoa(time.Now().Nanosecond())
	oldSnapshot := "snapshot.old"
	newSnapshot := "snapshot.new"
	func() {
		func() {
			resp, err := c.Delete(&webhdfs.DeleteRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				Path:      types.Pointer(dir),
				Recursive: types.Pointer(true),
			})
			if err != nil {
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.Mkdirs(&webhdfs.MkdirsRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				Path:      types.Pointer(dir),
			})
			if err != nil {
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.AllowSnapshot(&webhdfs.AllowSnapshotRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				Path:      types.Pointer(dir),
			})
			if err != nil {
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.CreateSnapshot(&webhdfs.CreateSnapshotRequest{
				ProxyUser:    c.ProxyUser(), // optional, user.name, The authenticated user
				Path:         types.Pointer(dir),
				Snapshotname: types.Pointer(oldSnapshot),
			})
			if err != nil {
				t.Fatalf("webhdfs CreateSnapshot failed: %s", err)
				return
			}
			defer resp.Body.Close()
			t.Logf("%s created: %s", oldSnapshot, types.Value(resp.Path))
		}()
	}()
	func() {
		resp, err := c.RenameSnapshot(&webhdfs.RenameSnapshotRequest{
			ProxyUser:       c.ProxyUser(), // optional, user.name, The authenticated user
			Path:            types.Pointer(dir),
			Oldsnapshotname: types.Pointer(oldSnapshot),
			Snapshotname:    types.Pointer(newSnapshot),
		})
		if err != nil {
			t.Fatalf("webhdfs CreateSnapshot failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	//    client_test.go:2651: snapshot.old created: /test.bucket/test353832000/.snapshot/snapshot.old
}

func TestClient_SetXAttr(t *testing.T) {
	TestClient_ListXAttrs(t)
	//    client_test.go:1111: XAttrNames: [user.name]
	//    client_test.go:1117: Encoded XAttrNames: "[\"user.name\"]"
}

func TestClient_RemoveXAttr(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	XAttrName := webhdfs.XAttrNamespaceUser.String() + ".name"
	XAttrValue := "example"
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.SetXAttr(&webhdfs.SetXAttrRequest{
			ProxyUser:  c.ProxyUser(), // optional, user.name, The authenticated user
			Path:       types.Pointer(file),
			XAttrName:  types.Pointer(XAttrName),
			XAttrValue: types.Pointer(XAttrValue),
			XAttrFlag:  webhdfs.XAttrSetFlagCreate.New(),
		})
		if err != nil {
			t.Fatalf("webhdfs SetXAttr failed: %s", err)
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.ListXAttrs(&webhdfs.ListXAttrsRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs ListXAttrs failed: %s", err)
		}
		defer resp.Body.Close()
		t.Logf("XAttrNames: %v", resp.XAttrNames)

		data, err := json.MarshalIndent(resp.XAttrNames, "", "\t")
		if err != nil {
			t.Fatalf("webhdfs ListXAttrs json Marshal failed: %s", err)
		}
		t.Logf("Encoded XAttrNames: %v", string(data))

		for _, name := range resp.XAttrNames {
			if name != XAttrName {
				t.Errorf("XAttrName: got %s, want %s", name, XAttrName)
			}
		}
	}()
	func() {
		resp, err := c.RemoveXAttr(&webhdfs.RemoveXAttrRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			XAttrName: types.Pointer(XAttrName),
		})
		if err != nil {
			t.Fatalf("webhdfs RemoveXAttr failed: %s", err)
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.ListXAttrs(&webhdfs.ListXAttrsRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs ListXAttrs failed: %s", err)
		}
		defer resp.Body.Close()
		t.Logf("XAttrNames: %v", resp.XAttrNames)

		data, err := json.MarshalIndent(resp.XAttrNames, "", "\t")
		if err != nil {
			t.Fatalf("webhdfs ListXAttrs json Marshal failed: %s", err)
		}
		t.Logf("Encoded XAttrNames: %v", string(data))

		for _, name := range resp.XAttrNames {
			if name == XAttrName {
				t.Errorf("XAttrName: got %s, want %s", name, XAttrName)
			}
		}
	}()
	func() {
		resp, err := c.RemoveXAttr(&webhdfs.RemoveXAttrRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			XAttrName: types.Pointer(XAttrName),
		})
		if err != nil {
			t.Logf("webhdfs RemoveXAttr failed: %s", err)
			return
		}
		defer resp.Body.Close()
		t.Fatalf("webhdfs RemoveXAttr succeed unexpected for not exist XAttrName")
	}()
	//    client_test.go:2717: XAttrNames: [user.name]
	//    client_test.go:2723: Encoded XAttrNames: "[\"user.name\"]"
	//    client_test.go:2751: XAttrNames: []
	//    client_test.go:2757: Encoded XAttrNames: "[]"
	//    client_test.go:2772: webhdfs RemoveXAttr failed: RemoteException: No matching attributes found for remove operation in org.apache.hadoop.ipc.RemoteException
}

func TestClient_SetStoragePolicy(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	var policyName string
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.GetStoragePolicy(&webhdfs.GetStoragePolicyRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs GetStoragePolicy failed: %s", err)
		}
		defer resp.Body.Close()

		data, err := json.MarshalIndent(resp.BlockStoragePolicy, "", "\t")
		if err != nil {
			t.Fatalf("webhdfs GetStoragePolicy json Marshal failed: %s", err)
		}
		t.Logf("GetStoragePolicy: %v", string(data))
		policyName = resp.BlockStoragePolicy.BlockStoragePolicy.Name
	}()
	func() {
		resp, err := c.SetStoragePolicy(&webhdfs.SetStoragePolicyRequest{
			ProxyUser:     c.ProxyUser(), // optional, user.name, The authenticated user
			Path:          types.Pointer(file),
			StoragePolicy: types.Pointer("HOT"),
		})
		if err != nil {
			t.Fatalf("webhdfs SetStoragePolicy failed: %s", err)
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.GetStoragePolicy(&webhdfs.GetStoragePolicyRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs GetStoragePolicy failed: %s", err)
		}
		defer resp.Body.Close()

		data, err := json.MarshalIndent(resp.BlockStoragePolicy, "", "\t")
		if err != nil {
			t.Fatalf("webhdfs GetStoragePolicy json Marshal failed: %s", err)
		}
		t.Logf("GetStoragePolicy: %v", string(data))
		if resp.BlockStoragePolicy.BlockStoragePolicy.Name != policyName {
			t.Fatalf("%s, expected %s, got %s", file, resp.BlockStoragePolicy.BlockStoragePolicy.Name, policyName)
		}
	}()

	//    client_test.go:2834: GetStoragePolicy: {
	//        	"BlockStoragePolicy": {
	//        		"id": 7,
	//        		"name": "HOT",
	//        		"storageTypes": [
	//        			"DISK"
	//        		],
	//        		"replicationFallbacks": [
	//        			"ARCHIVE"
	//        		],
	//        		"creationFallbacks": [],
	//        		"copyOnCreateFile": false
	//        	}
	//        }
	//    client_test.go:2862: GetStoragePolicy: {
	//        	"BlockStoragePolicy": {
	//        		"id": 7,
	//        		"name": "HOT",
	//        		"storageTypes": [
	//        			"DISK"
	//        		],
	//        		"replicationFallbacks": [
	//        			"ARCHIVE"
	//        		],
	//        		"creationFallbacks": [],
	//        		"copyOnCreateFile": false
	//        	}
	//        }
}

func TestClient_SatisfyStoragePolicy(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	var policyName string
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.GetStoragePolicy(&webhdfs.GetStoragePolicyRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs GetStoragePolicy failed: %s", err)
		}
		defer resp.Body.Close()
		policyName = resp.BlockStoragePolicy.BlockStoragePolicy.Name
	}()
	func() {
		resp, err := c.SetStoragePolicy(&webhdfs.SetStoragePolicyRequest{
			ProxyUser:     c.ProxyUser(), // optional, user.name, The authenticated user
			Path:          types.Pointer(file),
			StoragePolicy: types.Pointer(policyName),
		})
		if err != nil {
			t.Fatalf("webhdfs SetStoragePolicy failed: %s", err)
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.GetStoragePolicy(&webhdfs.GetStoragePolicyRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs GetStoragePolicy failed: %s", err)
		}
		defer resp.Body.Close()

		data, err := json.MarshalIndent(resp.BlockStoragePolicy, "", "\t")
		if err != nil {
			t.Fatalf("webhdfs GetStoragePolicy json Marshal failed: %s", err)
		}
		t.Logf("GetStoragePolicy: %v", string(data))
		if resp.BlockStoragePolicy.BlockStoragePolicy.Name != policyName {
			t.Fatalf("%s, expected %s, got %s", file, resp.BlockStoragePolicy.BlockStoragePolicy.Name, policyName)
		}
	}()
	func() {
		resp, err := c.SatisfyStoragePolicy(&webhdfs.SatisfyStoragePolicyRequest{
			ProxyUser:     c.ProxyUser(), // optional, user.name, The authenticated user
			Path:          types.Pointer(file),
			StoragePolicy: types.Pointer(policyName),
		})
		if err != nil {
			t.Fatalf("webhdfs SatisfyStoragePolicy failed: %s", err)
		}
		defer resp.Body.Close()
	}()

	// SATISFYSTORAGEPOLICY operation is not implemented on WebHDFS Hadoop 2.6.1
	// SATISFYSTORAGEPOLICY operation is not implemented on HttpFS Hadoop 3.2.1
	// client_test.go:631: webhdfs SatisfyStoragePolicy failed: IllegalArgumentException: Invalid value for webhdfs parameter "op": No enum constant org.apache.hadoop.hdfs.web.resources.PutOpParam.Op.SATISFYSTORAGEPOLICY in java.lang.IllegalArgumentException
}

func TestClient_EnableECPolicy(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	policy := "ecpolicy"
	func() {
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		func() {
			resp, err := c.GetECPolicy(&webhdfs.GetECPolicyRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				Path:      types.Pointer(file),
			})
			if err != nil {
				t.Fatalf("webhdfs GetECPolicy"+
					" failed: %s", err)
				return
			}
			defer resp.Body.Close()
			data, err := json.MarshalIndent(resp.ECPolicy, "", "\t")
			if err != nil {
				t.Fatalf("webhdfs GetECPolicy json Marshal failed: %s", err)
			}
			t.Logf("GetECPolicy: %s", string(data))
		}()
		func() {
			resp, err := c.UnsetECPolicy(&webhdfs.UnsetECPolicyRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			})
			if err != nil {
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.SetECPolicy(&webhdfs.SetECPolicyRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				ECPolicy:  types.Pointer(policy),
			})
			if err != nil {
				t.Fatalf("webhdfs SetECPolicy failed: %s", err)
			}
			defer resp.Body.Close()
		}()
	}()

	// GETECPOLICY operation is not implemented on WebHDFS Hadoop 2.6.1
	// GETECPOLICY operation is not implemented on HttpFS Hadoop 3.2.1
	//    client_test.go:3010: webhdfs GetECPolicy failed: QueryParamException: java.lang.IllegalArgumentException: No enum constant org.apache.hadoop.fs.http.client.HttpFSFileSystem.Operation.GETECPOLICY in com.sun.jersey.api.ParamException$QueryParamException
}

func TestClient_DisableECPolicy(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	policy := "RS-10-4-1024k"
	func() {
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		func() {
			resp, err := c.DisableECPolicy(&webhdfs.DisableECPolicyRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				ECPolicy:  types.Pointer(policy),
			})
			if err != nil {
				t.Fatalf("webhdfs DisableECPolicy"+
					" failed: %s", err)
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.SetECPolicy(&webhdfs.SetECPolicyRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				ECPolicy:  types.Pointer(policy),
			})
			if err != nil {
				t.Logf("webhdfs SetECPolicy failed: %s", err)
				return
			}
			defer resp.Body.Close()
			t.Fatalf("webhdfs SetECPolicy succeed unexpected")

		}()
	}()
	//    client_test.go:3083: webhdfs DisableECPolicy failed: WebApplicationException in javax.ws.rs.WebApplicationException
}

func TestClient_SetECPolicy(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	policy := "RS-10-4-1024k"
	func() {
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		//func() {
		//	resp, err := c.EnableECPolicy(&webhdfs.EnableECPolicyRequest{
		//		ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
		//		ECPolicy:  types.Pointer(policy),
		//	})
		//	if err != nil {
		//		t.Fatalf("webhdfs EnableECPolicy"+
		//			" failed: %s", err)
		//		return
		//	}
		//	defer resp.Body.Close()
		//}()
		func() {
			resp, err := c.SetECPolicy(&webhdfs.SetECPolicyRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				ECPolicy:  types.Pointer(policy),
				Path:      types.Pointer(file),
			})
			if err != nil {
				t.Fatalf("webhdfs SetECPolicy failed: %s", err)
				return
			}
			defer resp.Body.Close()
			t.Logf("webhdfs SetECPolicy succeed unexpected")

		}()
	}()
	// SETECPOLICY operation is not implemented on WebHDFS Hadoop 2.6.1
	// SETECPOLICY operation is not implemented on HttpFS Hadoop 3.2.1
	//    client_test.go:3155: webhdfs SetECPolicy failed: QueryParamException: java.lang.IllegalArgumentException: No enum constant org.apache.hadoop.fs.http.client.HttpFSFileSystem.Operation.SETECPOLICY in com.sun.jersey.api.ParamException$QueryParamException
}

func TestClient_Append(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	func() {
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Append(&webhdfs.AppendRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
		})
		if err != nil {
			t.Fatalf("webhdfs Append failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Open(&webhdfs.OpenRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Open failed: %s", err)
			return
		}
		defer resp.Body.Close()
		readData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("webhdfs Open and Read failed: %s", err)
		}
		t.Logf("%s, read %q", file, readData)

		if bytes.Compare([]byte(writtenData+writtenData), readData) != 0 {
			t.Fatalf("%s, expected %q, got %q", file, writtenData+writtenData, readData)
		}
	}()
	//    client_test.go:3221: test.bucket/test/found.txt, read "Hello World!Hello World!"
}

func TestClient_Concat(t *testing.T) {
	c := getWebHDFSClient(t)
	srcOneFile := "/" + HdfsBucket + "/test/found.1.txt"
	srcTwoFile := "/" + HdfsBucket + "/test/found.2.txt"
	concatFile := "/" + HdfsBucket + "/test/concat.found.txt"
	writtenData := "Hello World!"
	func() {
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(srcOneFile),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(srcOneFile),
			Body:      strings.NewReader(writtenData),
		})
		if err != nil {
			t.Fatalf("webhdfs Append failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(srcTwoFile),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(srcTwoFile),
			Body:      strings.NewReader(writtenData),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(concatFile),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(concatFile),
			Body:      strings.NewReader(writtenData),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Concat(&webhdfs.ConcatRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(concatFile),
			Sources:   types.Pointer(strings.Join([]string{srcOneFile, srcTwoFile}, ",")),
		})
		if err != nil {
			t.Fatalf("webhdfs Concat failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Open(&webhdfs.OpenRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(concatFile),
		})
		if err != nil {
			t.Fatalf("webhdfs Open failed: %s", err)
			return
		}
		defer resp.Body.Close()
		readData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("webhdfs Open and Read failed: %s", err)
		}
		t.Logf("%s, read %q", concatFile, readData)

		if bytes.Compare([]byte(strings.Repeat(writtenData, 3)), readData) != 0 {
			t.Fatalf("%s, expected %q, got %q", concatFile, writtenData+writtenData, readData)
		}
	}()
	//    client_test.go:3331: /test.bucket/test/concat.found.txt, read "Hello World!Hello World!Hello World!"
}

func TestClient_TruncateZero(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	newLength := 0
	func() {
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Truncate(&webhdfs.TruncateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			NewLength: types.Pointer(int64(newLength)),
		})
		if err != nil {
			t.Fatalf("webhdfs Append failed: %s", err)
			return
		}
		defer resp.Body.Close()
		t.Logf("Boolean: %t", resp.Boolean)
	}()
	func() {
		resp, err := c.Open(&webhdfs.OpenRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Open failed: %s", err)
			return
		}
		defer resp.Body.Close()
		readData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("webhdfs Open and Read failed: %s", err)
		}
		t.Logf("%s, read %q", file, readData)

		if bytes.Compare([]byte(writtenData[:newLength]), readData) != 0 {
			t.Fatalf("%s, expected %q, got %q", file, writtenData[:newLength], readData)
		}
	}()
	//    client_test.go:3379: Boolean: true
	//    client_test.go:3395: test.bucket/test/found.txt, read ""
}
func TestClient_Truncate(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	newLength := len(writtenData) / 2
	var truncateToZero bool
	func() {
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Truncate(&webhdfs.TruncateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			NewLength: types.Pointer(int64(newLength)),
		})
		if err != nil {
			t.Fatalf("webhdfs Append failed: %s", err)
			return
		}
		defer resp.Body.Close()
		truncateToZero = !resp.Boolean
		t.Logf("Boolean: %t", resp.Boolean)
	}()
	func() {
		resp, err := c.Open(&webhdfs.OpenRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Open failed: %s", err)
			return
		}
		defer resp.Body.Close()
		readData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("webhdfs Open and Read failed: %s", err)
		}
		t.Logf("%s, read %q", file, readData)
		if truncateToZero {
			if bytes.Compare([]byte(writtenData[:0]), readData) != 0 {
				t.Fatalf("%s, expected %q, got %q", file, writtenData[:0], readData)
			}
		} else {
			if bytes.Compare([]byte(writtenData[:newLength]), readData) != 0 {
				t.Fatalf("%s, expected %q, got %q", file, writtenData[:newLength], readData)
			}
		}
	}()
	// newlength param is ignored on HttpFS Hadoop 3.2.1
	//    client_test.go:3445: Boolean: false
	//    client_test.go:3461: test.bucket/test/found.txt, read ""
}

func TestClient_UnsetStoragePolicy(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	var policyName string
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.GetStoragePolicy(&webhdfs.GetStoragePolicyRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs GetStoragePolicy failed: %s", err)
		}
		defer resp.Body.Close()

		data, err := json.MarshalIndent(resp.BlockStoragePolicy, "", "\t")
		if err != nil {
			t.Fatalf("webhdfs GetStoragePolicy json Marshal failed: %s", err)
		}
		t.Logf("GetStoragePolicy: %v", string(data))
		policyName = resp.BlockStoragePolicy.BlockStoragePolicy.Name
	}()
	func() {
		resp, err := c.SetStoragePolicy(&webhdfs.SetStoragePolicyRequest{
			ProxyUser:     c.ProxyUser(), // optional, user.name, The authenticated user
			Path:          types.Pointer(file),
			StoragePolicy: types.Pointer("HOT"),
		})
		if err != nil {
			t.Fatalf("webhdfs SetStoragePolicy failed: %s", err)
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.UnsetStoragePolicy(&webhdfs.UnsetStoragePolicyRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs UnsetStoragePolicy failed: %s", err)
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.GetStoragePolicy(&webhdfs.GetStoragePolicyRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs GetStoragePolicy failed: %s", err)
		}
		defer resp.Body.Close()

		data, err := json.MarshalIndent(resp.BlockStoragePolicy, "", "\t")
		if err != nil {
			t.Fatalf("webhdfs GetStoragePolicy json Marshal failed: %s", err)
		}
		t.Logf("GetStoragePolicy: %v", string(data))
		if resp.BlockStoragePolicy.BlockStoragePolicy.Name != policyName {
			t.Fatalf("%s, expected %s, got %s", file, resp.BlockStoragePolicy.BlockStoragePolicy.Name, policyName)
		}
	}()
	//    client_test.go:3509: GetStoragePolicy: {
	//        	"BlockStoragePolicy": {
	//        		"id": 7,
	//        		"name": "HOT",
	//        		"storageTypes": [
	//        			"DISK"
	//        		],
	//        		"replicationFallbacks": [
	//        			"ARCHIVE"
	//        		],
	//        		"creationFallbacks": [],
	//        		"copyOnCreateFile": false
	//        	}
	//        }
	//    client_test.go:3547: GetStoragePolicy: {
	//        	"BlockStoragePolicy": {
	//        		"id": 7,
	//        		"name": "HOT",
	//        		"storageTypes": [
	//        			"DISK"
	//        		],
	//        		"replicationFallbacks": [
	//        			"ARCHIVE"
	//        		],
	//        		"creationFallbacks": [],
	//        		"copyOnCreateFile": false
	//        	}
	//        }
}

func TestClient_UnsetECPolicy(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	func() {
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.UnsetECPolicy(&webhdfs.UnsetECPolicyRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs UnsetECPolicy failed: %s", err)
			return
		}
		defer resp.Body.Close()
		t.Logf("webhdfs UnsetECPolicy succeed unexpected")

	}()
	// SETECPOLICY operation is not implemented on WebHDFS Hadoop 2.6.1
	//	  client_test.go:850: webhdfs UnsetECPolicy failed: IllegalArgumentException: Invalid value for webhdfs parameter "op": No enum constant org.apache.hadoop.hdfs.web.resources.PostOpParam.Op.UNSETECPOLICY in java.lang.IllegalArgumentException
	// SETECPOLICY operation is not implemented on HttpFS Hadoop 3.2.1
	//    client_test.go:3616: webhdfs UnsetECPolicy failed: QueryParamException: java.lang.IllegalArgumentException: No enum constant org.apache.hadoop.fs.http.client.HttpFSFileSystem.Operation.UNSETECPOLICY in com.sun.jersey.api.ParamException$QueryParamException
}

func TestClient_Delete(t *testing.T) {
	c := getWebHDFSClient(t)
	file := HdfsBucket + "/test/found.txt"
	writtenData := "Hello World!"
	func() {
		resp, err := c.Create(&webhdfs.CreateRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
			Body:      strings.NewReader(writtenData),
			Overwrite: types.Pointer(true),
		})
		if err != nil {
			t.Fatalf("webhdfs Create failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Delete(&webhdfs.DeleteRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			t.Fatalf("webhdfs Delete failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()
	func() {
		resp, err := c.Open(&webhdfs.OpenRequest{
			ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
			Path:      types.Pointer(file),
		})
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				t.Logf("webhdfs Open failed: %s", err)
			} else {
				t.Fatalf("webhdfs Open failed: %s", err)
			}
			return
		}
		defer resp.Body.Close()
		readData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("webhdfs Open and Read failed: %s", err)
		}
		t.Fatalf("%s, read %q", file, readData)
	}()
	//    client_test.go:3664: webhdfs Open failed: FileNotFoundException: File does not exist: /test.bucket/test/found.txt in java.io.FileNotFoundException
}

func TestClient_DeleteSnapshot(t *testing.T) {
	c := getWebHDFSClient(t)
	dir := HdfsBucket + "/test"
	snapshot := "snapshot"
	func() {
		func() {
			resp, err := c.Mkdirs(&webhdfs.MkdirsRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				Path:      types.Pointer(dir),
			})
			if err != nil {
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.AllowSnapshot(&webhdfs.AllowSnapshotRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				Path:      types.Pointer(dir),
			})
			if err != nil {
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.DeleteSnapshot(&webhdfs.DeleteSnapshotRequest{
				ProxyUser:    c.ProxyUser(), // optional, user.name, The authenticated user
				Path:         types.Pointer(dir),
				Snapshotname: types.Pointer(snapshot),
			})
			if err != nil {
				return
			}
			defer resp.Body.Close()
		}()
		func() {
			resp, err := c.GetSnapshottableDirectoryList(&webhdfs.GetSnapshottableDirectoryListRequest{
				ProxyUser: c.ProxyUser(), // optional, user.name, The authenticated user
				Username:  c.ProxyUser().Username,
			})
			if err != nil {
				t.Fatalf("webhdfs GetSnapshottableDirectoryList failed: %s", err)
			}
			defer resp.Body.Close()

			//data, err := json.MarshalIndent(resp.SnapshottableDirectoryList, "", "\t")
			//if err != nil {
			//	t.Fatalf("webhdfs SnapshottableDirectoryList json Marshal failed: %s", err)
			//}
			//t.Logf("SnapshottableDirectoryList: %v", string(data))
			for i, sd := range resp.SnapshottableDirectoryList {
				if sd.ParentFullPath == path.Clean(dir) {
					t.Errorf("[%d] %q should be deleted", i, sd.ParentFullPath)
				}
			}
		}()
	}()
}
