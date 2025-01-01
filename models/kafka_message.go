package models

type Message struct {
	Headers   map[string]string
	Offset    int64
	Topic     string
	Partition int32
	Value     []byte
	Logs      string
}
