package webhdfs

import (
	"os"
	"time"
)

// FileStatusProperties implements os.FileInfo, and provides information about a file or
// directory in HDFS.
func (fi *FileStatusProperties) Name() string {
	return fi.PathSuffix
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
