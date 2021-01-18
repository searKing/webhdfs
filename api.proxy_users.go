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

	// Delegation
	// Name				delegation
	// Description		The delegation token used for authentication.
	// Type				String
	// Default Value	<empty>
	// Valid Values		An encoded token.
	// Syntax		See the note below.
	// Note that delegation tokens are encoded as a URL safe string;
	// see encodeToUrlString() and decodeFromUrlString(String) in org.apache.hadoop.security.token.Token for the details of the encoding.
	Delegation *string
}
