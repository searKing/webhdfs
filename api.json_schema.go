package webhdfs

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"

	time_ "github.com/searKing/golang/go/time"
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
	DirectoryCount int64 `json:"directoryCount" validate:"required"` // The number of directories.
	FileCount      int64 `json:"fileCount" validate:"required"`      // The number of files.
	// Length is the total size of the named path, including any subdirectories.
	Length int64 `json:"length" validate:"required"` // The number of bytes used by the content.
	Quota  int64 `json:"quota" validate:"required"`  // The namespace quota of this directory.
	// SpaceConsumed is the total size of the named path, including any
	// subdirectories. Unlike Length, it counts the total replicated size of each
	// file, and represents the total on-disk footprint64 for a tree in HDFS.
	SpaceConsumed int64     `json:"spaceConsumed" validate:"required"` // The disk space consumed by the content.
	SpaceQuota    int64     `json:"spaceQuota" validate:"required"`    // The disk space quota.
	TypeQuota     TypeQuota `json:"typeQuota" validate:"required"`
}

//  See also: http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsQuotaAdminGuide.html for more information.
type Quota struct {
	Consumed int64 `json:"consumed" validate:"required"` // The storage type space consumed.
	Quota    int64 `json:"quota" validate:"required"`    // The storage type quota.
}
type TypeQuota struct {
	ARCHIVE Quota `json:"ARCHIVE"`
	DISK    Quota `json:"DISK"`
	SSD     Quota `json:"SSD"`
}

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#QuotaUsage_JSON_Schema
type QuotaUsage struct {
	FileAndDirectoryCount int64     `json:"fileAndDirectoryCount" validate:"required"` // The number of files and directories.
	Quota                 int64     `json:"quota" validate:"required"`                 // The namespace quota of this directory.
	SpaceConsumed         int64     `json:"spaceConsumed" validate:"required"`         // The disk space consumed by the content.
	SpaceQuota            int64     `json:"spaceQuota" validate:"required"`            // The disk space quota.
	TypeQuota             TypeQuota `json:"typeQuota" validate:"required"`
}

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#FileChecksum_JSON_Schema
type FileChecksum struct {
	Algorithm string `json:"algorithm" validate:"required"` // The name of the checksum algorithm.
	Bytes     string `json:"bytes" validate:"required"`     // The byte sequence of the checksum in hexadecimal.
	Length    int64  `json:"length" validate:"required"`    // The length of the bytes (not the length of the string).
}

type FileType string

const (
	FileTypeFile      FileType = "FILE"
	FileTypeDirectory FileType = "DIRECTORY"
	FileTypeSymlink   FileType = "SYMLINK"
)

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#FileStatus_JSON_Schema
type FileStatus = FileStatusProperties

//type FileStatus struct {
//	FileStatus FileStatusProperties `json:"FileStatus" validate:"required"` // See FileStatus Properties.
//}

// JavaScript syntax is used to define fileStatusProperties so that it can be referred in both FileStatus and FileStatuses JSON schemas.
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#FileStatus_Properties
type FileStatusProperties struct {
	PathPrefix       string                    `json:"-"`                                    // The path prefix, for current file|fir
	AccessTime       time_.UnixTimeMillisecond `json:"accessTime" validate:"required"`       // The access time.
	BlockSize        int64                     `json:"blockSize" validate:"required"`        // The block size of a file.
	ChildrenNum      int64                     `json:"childrenNum"`                          // The number of sub files or dirs
	FileId           int64                     `json:"fileId"`                               // The file id
	Group            string                    `json:"group" validate:"required"`            // The group owner.
	Length           int64                     `json:"length" validate:"required"`           // The number of bytes in a file. in bytes, zero for directories
	ModificationTime time_.UnixTimeMillisecond `json:"modificationTime" validate:"required"` // The modification time.
	Owner            string                    `json:"owner" validate:"required"`            // The user who is the owner.
	PathSuffix       string                    `json:"pathSuffix" validate:"required"`       // The path suffix. for subfile|subdir
	Permission       Permission                `json:"permission" validate:"required"`       // The permission represented as a octal string.
	Replication      int64                     `json:"replication" validate:"required"`      // The number of replication of a file.
	Symlink          string                    `json:"symlink"`                              // The link target of a symlink.
	Type             FileType                  `json:"type" validate:"required"`             // The type of the path object. ["FILE", "DIRECTORY", "SYMLINK"]
}

// FileStatusProperties implements os.FileInfo, and provides information about a file or directory in HDFS.
func (fi *FileStatusProperties) Name() string {
	if fi.PathSuffix != "" {
		return fi.PathSuffix
	}
	return path.Base(path.Join(fi.PathPrefix, fi.PathSuffix))
}

func (fi *FileStatusProperties) Size() int64 {
	return fi.Length
}

func (fi *FileStatusProperties) Mode() os.FileMode {
	mode := os.FileMode(fi.Permission)
	if fi.IsDir() {
		mode |= os.ModeDir
	}

	return mode
}

func (fi *FileStatusProperties) ModTime() time.Time {
	return fi.ModificationTime.Time
}

func (fi *FileStatusProperties) IsDir() bool {
	return fi.Type == FileTypeDirectory
}

// Sys returns the raw *FileStatusProperties message from the namenode.
func (fi *FileStatusProperties) Sys() interface{} {
	return fi
}

// The permission of a file/directory.
// 644 for files, 755 for directories
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Permission
type Permission uint64

const (
	DefaultPermissionFile      Permission = 0644
	DefaultPermissionDirectory Permission = 0755
)

func (p Permission) String() string {
	return fmt.Sprintf("%o", p)
}

// MarshalJSON implements the json.Marshaler interface for XAttrNamespace
func (p Permission) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.String())
}

// UnmarshalJSON implements the json.Unmarshaler interface for XAttrNamespace
func (p *Permission) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("Permission should be a string, got %s", data)
	}

	i, err := strconv.ParseUint(s, 8, 32)
	if err != nil {
		return err
	}
	*p = Permission(i)
	return nil
}

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#FileStatuses_JSON_Schema
type FileStatuses struct {
	FileStatus []FileStatus `json:"FileStatus"` // An array of FileStatus
}

func (s *FileStatuses) Len() int {
	return len(s.FileStatus)
}

func (s *FileStatuses) Swap(i, j int) {
	s.FileStatus[i], s.FileStatus[j] = s.FileStatus[j], s.FileStatus[i]
}

func (s *FileStatuses) Less(i, j int) bool {
	return s.FileStatus[i].PathSuffix < s.FileStatus[j].PathSuffix
}

// A DirectoryListing JSON object represents a batch of directory entries while iteratively listing a directory.
// It contains a FileStatuses JSON object as well as iteration information.
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#DirectoryListing_JSON_Schema
type DirectoryListing struct {
	PartialListing struct {
		FileStatuses FileStatuses `json:"FileStatuses"` // An array of FileStatus
	} `json:"partialListing" validate:"required"`                        // A partial directory listing
	RemainingEntries int64 `json:"remainingEntries" validate:"required"` // Number of remaining entries
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
func (e *RemoteException) Error() string {
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

const (
	JavaClassNameFileNotFoundException            = "java.io.FileNotFoundException"
	JavaClassNameAccessControlException           = "org.apache.hadoop.security.AccessControlException"
	JavaClassNamePathIsNotEmptyDirectoryException = "org.apache.hadoop.fs.PathIsNotEmptyDirectoryException"
	JavaClassNameFileAlreadyExistsException       = "org.apache.hadoop.fs.FileAlreadyExistsException"
	JavaClassNameAlreadyBeingCreatedException     = "org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException"
)

func (e *RemoteException) Unwrap() error {
	switch e.JavaClassName {
	case JavaClassNameFileNotFoundException:
		//return os.ErrNotExist
		return syscall.ENOENT
	case JavaClassNameAccessControlException:
		//return os.ErrPermission
		return syscall.EPERM
	case JavaClassNamePathIsNotEmptyDirectoryException:
		return syscall.ENOTEMPTY
	case JavaClassNameFileAlreadyExistsException:
		//return os.ErrExist
		return syscall.ENOTEMPTY
	case JavaClassNameAlreadyBeingCreatedException:
		//return os.ErrExist
		return syscall.EEXIST
	default:
		return fmt.Errorf("%s", e.Error())
	}
}

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Token_JSON_Schema
type Token struct {
	UrlString string `json:"urlString" validate:"required"` // A delegation token encoded as a URL safe string.
}

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#ECPolicy_JSON_Schema
type ECPolicy struct {
	Name              string         `json:"name"`
	Schema            ECPolicySchema `json:"schema"`
	CellSize          int64          `json:"cellSize"`
	Id                int64          `json:"id"`
	Codecname         string         `json:"codecname"`
	NumDataUnits      int64          `json:"numDataUnits"`
	NumParityUnits    int64          `json:"numParityUnits"`
	Replicationpolicy bool           `json:"replicationpolicy"`
	SystemPolicy      bool           `json:"systemPolicy"`
}

type ECPolicySchema struct {
	CodecName      string      `json:"codecName"`
	NumDataUnits   int64       `json:"numDataUnits"`
	NumParityUnits int64       `json:"numParityUnits"`
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
	Id                   int64    `json:"id" validate:"required"`                   // Policy ID.
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
type SnapshottableDirectoryList = []SnapshottableDirectoryStatus // An array of SnapshottableDirectoryStatus

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#SnapshottableDirectoryStatus
type SnapshottableDirectoryStatus struct {
	DirStatus      FileStatusProperties `json:"dirStatus"`                          // Source path name relative to snapshot root.
	ParentFullPath string               `json:"parentFullPath" validate:"required"` // Full path of the parent of snapshottable directory.
	SnapshotNumber int64                `json:"snapshotNumber" validate:"required"` // Number of snapshots created on the snapshottable directory.
	SnapshotQuota  int64                `json:"snapshotQuota" validate:"required"`  // Total number of snapshots allowed on the snapshottable directory.
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
	Length        int64         `json:"length" validate:"required"`        // Length of the block
	Names         []string      `json:"names" validate:"required"`         // Datanode IP:xferPort for accessing the block
	Offset        int64         `json:"offset" validate:"required"`        // Offset of the block in the file
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
