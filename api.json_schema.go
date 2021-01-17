package webhdfs

import (
	"fmt"
	"strings"

	"github.com/searKing/golang/go/time"
)

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
type Boolean = bool // A boolean value.

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

type FileType string

const (
	FileTypeFile FileType = "FILE"
	Directory    FileType = "DIRECTORY"
	Symlink      FileType = "SYMLINK"
)

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#FileStatus_JSON_Schema
type FileStatus = FileStatusProperties

//type FileStatus struct {
//	FileStatus FileStatusProperties `json:"FileStatus" validate:"required"` // See FileStatus Properties.
//}

// JavaScript syntax is used to define fileStatusProperties so that it can be referred in both FileStatus and FileStatuses JSON schemas.
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#FileStatus_Properties
type FileStatusProperties struct {
	AccessTime       time.UnixTimeMillisecond `json:"accessTime" validate:"required"`       // The access time.
	BlockSize        int                      `json:"blockSize" validate:"required"`        // The block size of a file.
	ChildrenNum      int                      `json:"childrenNum"`                          // The number of sub files or dirs
	FileId           int                      `json:"fileId"`                               // The file id
	Group            string                   `json:"group" validate:"required"`            // The group owner.
	Length           int                      `json:"length" validate:"required"`           // The number of bytes in a file. in bytes, zero for directories
	ModificationTime time.UnixTimeMillisecond `json:"modificationTime" validate:"required"` // The modification time.
	Owner            string                   `json:"owner" validate:"required"`            // The user who is the owner.
	PathSuffix       string                   `json:"pathSuffix" validate:"required"`       // The path suffix.
	Permission       string                   `json:"permission" validate:"required"`       // The permission represented as a octal string.
	Replication      int                      `json:"replication" validate:"required"`      // The number of replication of a file.
	Symlink          string                   `json:"symlink"`                              // The link target of a symlink.
	Type             FileType                 `json:"type" validate:"required"`             // The type of the path object. ["FILE", "DIRECTORY", "SYMLINK"]
}

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#FileStatuses_JSON_Schema
type FileStatuses struct {
	FileStatus []FileStatus `json:"FileStatus"` // An array of FileStatus
}

// A DirectoryListing JSON object represents a batch of directory entries while iteratively listing a directory.
// It contains a FileStatuses JSON object as well as iteration information.
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#DirectoryListing_JSON_Schema
type DirectoryListing struct {
	PartialListing struct {
		FileStatuses FileStatuses `json:"FileStatuses"` // An array of FileStatus
	} `json:"partialListing" validate:"required"`                      // A partial directory listing
	RemainingEntries int `json:"remainingEntries" validate:"required"` // Number of remaining entries
}

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Long_JSON_Schema
type Long int64 // A long integer value

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Path_JSON_Schema
type Path string // The string representation a Path.

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

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#ECPolicy_JSON_Schema
type ECPolicy struct {
	Name              string         `json:"name"`
	Schema            ECPolicySchema `json:"schema"`
	CellSize          int            `json:"cellSize"`
	Id                int            `json:"id"`
	Codecname         string         `json:"codecname"`
	NumDataUnits      int            `json:"numDataUnits"`
	NumParityUnits    int            `json:"numParityUnits"`
	Replicationpolicy bool           `json:"replicationpolicy"`
	SystemPolicy      bool           `json:"systemPolicy"`
}

type ECPolicySchema struct {
	CodecName      string      `json:"codecName"`
	NumDataUnits   int         `json:"numDataUnits"`
	NumParityUnits int         `json:"numParityUnits"`
	ExtraOptions   interface{} `json:"extraOptions"`
}

// See：https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#BlockStoragePolicies_JSON_Schema
type BlockStoragePolicies struct {
	BlockStoragePolicies []BlockStoragePolicyProperties `json:"BlockStoragePolicy" validate:"required"` // An array of BlockStoragePolicy.
}

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#BlockStoragePolicy_JSON_Schema
type BlockStoragePolicy struct {
	BlockStoragePolicy BlockStoragePolicyProperties `json:"BlockStoragePolicy" validate:"required"` // An BlockStoragePolicy.
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

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#SnapshotDiffReport_JSON_Schema
type SnapshotDiffReport struct {
	DiffList     []DiffReportEntry `json:"diffList" validate:"required"`     // An array of DiffReportEntry.
	FromSnapshot string            `json:"fromSnapshot" validate:"required"` // Source snapshot.
	SnapshotRoot string            `json:"snapshotRoot" validate:"required"` // String representation of snapshot root path.
	ToSnapshot   string            `json:"toSnapshot" validate:"required"`   // Destination snapshot.
}

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#DiffReport_Entries
type DiffReportEntry struct {
	SourcePath string              `json:"sourcePath" validate:"required"` // Source path name relative to snapshot root.
	TargetPath string              `json:"targetPath" validate:"required"` // Target path relative to snapshot root used for renames.
	Type       DiffReportEntryType `json:"type" validate:"required"`       // Type of diff report entry`["CREATE", "MODIFY", "DELETE", "RENAME"]
}

type DiffReportEntryType string

const (
	DiffReportEntryTypeCreate DiffReportEntryType = "CREATE"
	DiffReportEntryTypeModify DiffReportEntryType = "MODIFY"
	DiffReportEntryTypeDelete DiffReportEntryType = "DELETE"
	DiffReportEntryTypeRename DiffReportEntryType = "RENAME"
)

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#SnapshottableDirectoryList_JSON_Schema
type SnapshottableDirectoryList struct {
	SnapshottableDirectoryList []SnapshottableDirectoryStatus `json:"SnapshottableDirectoryList" validate:"required"` // An array of SnapshottableDirectoryStatus
}

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#SnapshottableDirectoryStatus
type SnapshottableDirectoryStatus struct {
	DirStatus      FileStatusProperties `json:"dirStatus"`                          // Source path name relative to snapshot root.
	ParentFullPath string               `json:"parentFullPath" validate:"required"` // Full path of the parent of snapshottable directory.
	SnapshotNumber int                  `json:"snapshotNumber" validate:"required"` // Number of snapshots created on the snapshottable directory.
	SnapshotQuota  int                  `json:"snapshotQuota" validate:"required"`  // Total number of snapshots allowed on the snapshottable directory.
}

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#BlockLocations_JSON_Schema
type BlockLocations = struct {
	BlockLocations []BlockLocation `json:"BlockLocation"` // An array of BlockLocation
}

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#BlockLocation_JSON_Schema
type BlockLocation = BlockLocationProperties

// JavaScript syntax is used to define blockLocationProperties so that it can be referred in both BlockLocation and BlockLocations JSON schemas.
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#BlockLocation_Properties
type BlockLocationProperties struct {
	CachedHosts   []string      `json:"cachedHosts" validate:"required"`   // Datanode hostnames with a cached replica
	Corrupt       bool          `json:"corrupt" validate:"required"`       // True if the block is corrupted
	Hosts         []string      `json:"hosts" validate:"required"`         // Datanode hostnames store the block
	Length        int           `json:"length" validate:"required"`        // Length of the block
	Names         []string      `json:"names" validate:"required"`         // Datanode IP:xferPort for accessing the block
	Offset        int           `json:"offset" validate:"required"`        // Offset of the block in the file
	StorageTypes  []StorageType `json:"storageTypes" validate:"required"`  // Storage type of each replica, ["RAM_DISK", "SSD", "DISK", "ARCHIVE"]
	TopologyPaths []string      `json:"topologyPaths" validate:"required"` // Datanode addresses in network topology, [ /rack/host:ip ]
}

type StorageType string

const (
	StorageTypeRamDisk StorageType = "RAM_DISK"
	StorageTypeSsd     StorageType = "SSD"
	StorageTypeDisk    StorageType = "DISK"
	StorageTypeArchive StorageType = "ARCHIVE"
)
