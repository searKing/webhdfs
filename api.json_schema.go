package webhdfs

import (
	"fmt"
	"strings"

	"github.com/searKing/golang/go/time"
)

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#RemoteException_JSON_Schema
type RemoteException struct {
	Exception     string `json:"exception" validate:"required"` // Name of the exception
	Message       string `json:"message" validate:"required"`   // Exception message
	JavaClassName string `json:"javaClassName,omitempty"`       // Java class name of the exception
}

// Error returns the string representation of the error.
// Satisfies the error interface.
func (e RemoteException) Error() string {
	var msg strings.Builder
	msg.WriteString(e.Exception)
	if e.Message != "" {
		msg.WriteString(fmt.Sprintf(": %s", e.Message))
	}
	if e.JavaClassName != "" {
		msg.WriteString(fmt.Sprintf(" in %s", e.JavaClassName))
	}
	return msg.String()
}

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Token_JSON_Schema
type Token struct {
	UrlString string `json:"urlString" validate:"required"` // A delegation token encoded as a URL safe string.
}

//go:generate go-enum -type FileType -trimprefix=FileType --transform=upper
type FileType int

const (
	FileTypeFile FileType = iota
	Directory    FileType = iota
	Symlink      FileType = iota
)

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#FileStatus_JSON_Schema
type FileStatus struct {
	AccessTime       time.UnixTime `json:"accessTime" validate:"required"`       // The access time.
	BlockSize        int           `json:"blockSize" validate:"required"`        // The block size of a file.
	ChildrenNum      int           `json:"childrenNum"`                          // The number of sub files or dirs
	FileId           int           `json:"fileId"`                               // The file id
	Group            string        `json:"group" validate:"required"`            // The group owner.
	Length           int           `json:"length" validate:"required"`           // The number of bytes in a file. in bytes, zero for directories
	ModificationTime time.UnixTime `json:"modificationTime" validate:"required"` // The modification time.
	Owner            string        `json:"owner" validate:"required"`            // The user who is the owner.
	PathSuffix       string        `json:"pathSuffix" validate:"required"`       // The path suffix.
	Permission       string        `json:"permission" validate:"required"`       // The permission represented as a octal string.
	Replication      int           `json:"replication" validate:"required"`      // The number of replication of a file.
	Symlink          string        `json:"symlink"`                              // The link target of a symlink.
	Type             FileType      `json:"type" validate:"required"`             // The type of the path object.
}

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#FileStatuses_JSON_Schema
type FileStatuses struct {
	FileStatuses []FileStatus `json:"FileStatus"` // An array of FileStatus
}

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#DirectoryListing_JSON_Schema
type DirectoryListing struct {
	PartialListing struct {
		FileStatuses FileStatuses `json:"FileStatuses"` // An array of FileStatus
	} `json:"partialListing" validate:"required"`                      // A partial directory listing
	RemainingEntries int `json:"remainingEntries" validate:"required"` // Number of remaining entries
}

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#XAttrs_JSON_Schema
type XAttrs struct {
	XAttrs []XAttr `json:"XAttrs"` // XAttr array.
}

type XAttr struct {
	Name  string `json:"name" validate:"required"`  // XAttr name.
	Value string `json:"value" validate:"required"` // XAttr value.
}

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#XAttrNames_JSON_Schema
type XAttrNames struct {
	XAttrNames string `json:"XAttrNames" validate:"required"` // XAttr names.
}

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Boolean_JSON_Schema
type Boolean struct {
	Boolean bool `json:"boolean" validate:"required"` // A boolean value.
}

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#ContentSummary_JSON_Schema
type ContentSummary struct {
	DirectoryCount int       `json:"directoryCount" validate:"required"` // The number of directories.
	FileCount      int       `json:"fileCount" validate:"required"`      // The number of files.
	Length         int       `json:"length" validate:"required"`         // The number of bytes used by the content.
	Quota          int       `json:"quota" validate:"required"`          // The namespace quota of this directory.
	SpaceConsumed  int       `json:"spaceConsumed" validate:"required"`  // The disk space consumed by the content.
	SpaceQuota     int       `json:"spaceQuota" validate:"required"`     // The disk space quota.
	TypeQuota      TypeQuota `json:"typeQuota" validate:"required"`
}

type Quota struct {
	Consumed int `json:"consumed" validate:"required"` // The storage type space consumed.
	Quota    int `json:"quota" validate:"required"`    // The storage type quota.
}
type TypeQuota struct {
	ARCHIVE Quota `json:"ARCHIVE"`
	DISK    Quota `json:"DISK"`
	SSD     Quota `json:"SSD"`
}

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#QuotaUsage_JSON_Schema
type QuotaUsage struct {
	FileAndDirectoryCount int       `json:"fileAndDirectoryCount" validate:"required"` // The number of files and directories.
	Quota                 int       `json:"quota" validate:"required"`                 // The namespace quota of this directory.
	SpaceConsumed         int       `json:"spaceConsumed" validate:"required"`         // The disk space consumed by the content.
	SpaceQuota            int       `json:"spaceQuota" validate:"required"`            // The disk space quota.
	TypeQuota             TypeQuota `json:"typeQuota" validate:"required"`
}

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#FileChecksum_JSON_Schema
type FileChecksum struct {
	Algorithm string `json:"algorithm" validate:"required"` // The name of the checksum algorithm.
	Bytes     string `json:"bytes" validate:"required"`     // The byte sequence of the checksum in hexadecimal.
	Length    int    `json:"length" validate:"required"`    // The length of the bytes (not the length of the string).
}

// See：https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#BlockStoragePolicies_JSON_Schema
type BlockStoragePolicies struct {
	BlockStoragePolicies []BlockStoragePolicyProperties `json:"BlockStoragePolicy" validate:"required"` // An array of BlockStoragePolicy.
}

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#BlockStoragePolicy_Properties
type BlockStoragePolicyProperties struct {
	Id                   int      `json:"id" validate:"required"`                   // Policy ID.
	Name                 string   `json:"name" validate:"required"`                 // Policy Name.
	StorageTypes         []string `json:"storageTypes" validate:"required"`         // An array of storage types for block placement.
	ReplicationFallbacks []string `json:"replicationFallbacks" validate:"required"` // An array of fallback storage types for replication.
	CreationFallbacks    []string `json:"creationFallbacks" validate:"required"`    // An array of fallback storage types for file creation.
	CopyOnCreate         bool     `json:"copyOnCreateFile" validate:"required"`     // If set then the policy cannot be changed after file creation.
}
