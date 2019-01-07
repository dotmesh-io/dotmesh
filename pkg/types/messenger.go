package types

type SubscribeQuery struct {
	RequestID    string
	FilesystemID string
	Type         EventType
}

func (q *SubscribeQuery) GetRequestID() string {
	if q.RequestID == "" {
		return "*"
	}
	return q.RequestID
}

func (q *SubscribeQuery) GetFilesystemID() string {
	if q.FilesystemID == "" {
		return "*"
	}
	return q.FilesystemID
}
