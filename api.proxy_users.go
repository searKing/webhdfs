package webhdfs

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Proxy_Users
type ProxyUser struct {
	// Name				user.name
	// Description		The authenticated user; see Authentication.
	// Type				String
	// Default Value	null
	// Valid Values		Any valid username.
	// Syntax			Any string.
	Username *string

	// Name				doas
	// Description		Allowing a proxy user to do as another user.
	// Type				String
	// Default Value	null
	// Valid Values		Any valid username.
	// Syntax			Any string.
	DoAs *string

	// Set by Delegation in struct Authentication
	// Delegation *string
}
