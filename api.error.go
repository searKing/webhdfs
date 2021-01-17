package webhdfs

// https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Error_Responses
type ErrorResponse struct {
	RemoteException *RemoteException `json:"RemoteException"`
}

func (e ErrorResponse) Exception() error {
	if e.RemoteException == nil {
		return nil
	}
	return e.RemoteException
}

func IsIllegalArgumentException(err error) bool {
	except, ok := err.(*RemoteException)
	if !ok {
		return false
	}
	if except.Exception == "IllegalArgumentException" {
		return true
	}

	return false
}

func IsSecurityException(err error) bool {
	except, ok := err.(*RemoteException)
	if !ok {
		return false
	}
	if except.Exception == "SecurityException" {
		return true
	}

	return false
}

func IsAccessControlException(err error) bool {
	except, ok := err.(*RemoteException)
	if !ok {
		return false
	}
	if except.Exception == "AccessControlException" {
		return true
	}

	return false
}

func IsFileNotFoundException(err error) bool {
	except, ok := err.(*RemoteException)
	if !ok {
		return false
	}
	if except.Exception == "FileNotFoundException" {
		return true
	}

	return false
}
