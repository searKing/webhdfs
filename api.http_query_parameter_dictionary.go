package webhdfs

// HTTP Query Parameter Dictionary
const (
	HttpQueryParamKeyXAttrName          = "xattr.name"
	HttpQueryParamKeyXAttrValueEncoding = "encoding"
)

// ACL Spec
// The ACL spec included in ACL modification operations.
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#XAttr_value_encoding
// See also: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html#ACLs_.28Access_Control_Lists.29

// XAttr Name
// The XAttr name of a file/directory.
// Any string prefixed with user./trusted./system./security..
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#XAttr_Name
// See also: https://hadoop.apache.org/docs/r2.6.0/hadoop-project-dist/hadoop-hdfs/ExtendedAttributes.html
//go:generate go-enum -type XAttrNamespace  --linecomment
type XAttrNamespace int

const (
	// In HDFS, there are five valid namespaces: user, trusted, system, security, and raw. Each of these namespaces have different access restrictions.
	// The user namespace is the namespace that will commonly be used by client applications.
	// Access to extended attributes in the user namespace is controlled by the corresponding file permissions.
	XAttrNamespaceUser XAttrNamespace = iota // user
	// The trusted namespace is available only to HDFS superusers.
	XAttrNamespaceTrusted XAttrNamespace = iota // trusted
	// The system namespace is reserved for internal HDFS use.
	// This namespace is not accessible through userspace methods, and is reserved for implementing internal HDFS features.
	XAttrNamespaceSystem XAttrNamespace = iota // system
	// The security namespace is reserved for internal HDFS use.
	// This namespace is generally not accessible through userspace methods.
	// One particular use of security is the security.hdfs.unreadable.by.superuser extended attribute.
	// This xattr can only be set on files, and it will prevent the superuser from reading the file's contents.
	// The superuser can still read and modify file metadata, such as the owner, permissions, etc.
	// This xattr can be set and accessed by any user, assuming normal filesystem permissions.
	// This xattr is also write-once, and cannot be removed once set.
	// This xattr does not allow a value to be set.
	XAttrNamespaceSecurity XAttrNamespace = iota // security
	// The raw namespace is reserved for internal system attributes that sometimes need to be exposed.
	// Like system namespace attributes they are not visible to the user
	// except when getXAttr/getXAttrs is called on a file or directory in the /.reserved/raw HDFS directory hierarchy.
	// These attributes can only be accessed by the superuser.
	// An example of where raw namespace extended attributes are used is the distcp utility.
	// Encryption zone meta data is stored in raw.* extended attributes,
	// so as long as the administrator uses /.reserved/raw pathnames in source and target,
	// the encrypted files in the encryption zones are transparently copied.
	XAttrNamespaceRaw XAttrNamespace = iota // raw
)

// XAttr Value
// The XAttr value of a file/directory.
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#XAttr_Value

// XAttr set flag
// The XAttr set flag.
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#XAttr_set_flag
//go:generate go-enum -type XAttrSetFlag  --linecomment
type XAttrSetFlag int

const (
	XAttrSetFlagCreate  XAttrSetFlag = iota // "CREATE"
	XAttrSetFlagReplace XAttrSetFlag = iota // "REPLACE"
)

// The XAttr value encoding.
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#XAttr_value_encoding
//go:generate go-enum -type XAttrValueEncoding  --linecomment
type XAttrValueEncoding int

const (
	XAttrValueEncodingText   XAttrValueEncoding = iota // text
	XAttrValueEncodingHex    XAttrValueEncoding = iota // hex
	XAttrValueEncodingBase64 XAttrValueEncoding = iota // base64
)

// Access Time
// The access time of a file/directory.
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Access_Time

// Block Size
// The block size of a file.
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Block_Size

// Create Flag
// Enum of possible flags to process while creating a file
// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Create_Flag
