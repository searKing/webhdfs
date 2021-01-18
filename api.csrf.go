package webhdfs

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Cross-Site_Request_Forgery_Prevention
type CSRF struct {
	// Name				X-XSRF-HEADER
	// Description		The name of a custom header that HTTP requests must send when protection against cross-site request forgery (CSRF) is enabled for WebHDFS by setting dfs.webhdfs.rest-csrf.enabled to true.
	// Type				String
	// Default Value	null
	// Valid Values		Any valid username.
	// Syntax			Any string.
	XXsrfHeader *string
}
