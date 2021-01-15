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
