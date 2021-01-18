package webhdfs

// See: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Authentication
// When security is off, the authenticated user is the username specified in the user.name query parameter. If the user.name parameter is not set, the server may either set the authenticated user to a default web user, if there is any, or return an error response.
// When security is on, authentication is performed by either Hadoop delegation token or Kerberos SPNEGO. If a token is set in the delegation query parameter, the authenticated user is the user encoded in the token. If the delegation parameter is not set, the user is authenticated by Kerberos SPNEGO.
// Below are examples using the curl command tool.
// Authentication when security is off:
//   curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?[user.name=<USER>&]op=..."
// Authentication using Kerberos SPNEGO when security is on:
//   curl -i --negotiate -u : "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=..."
// Authentication using Hadoop delegation token when security is on:
//   curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?delegation=<TOKEN>&op=..."
// See also: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/HttpAuthentication.html
type Authentication struct {
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
