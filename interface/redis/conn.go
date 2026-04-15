package redis

import "go-Redis/redis/protocol"

type Connection interface {
	Write([]byte) (int, error)
	Close() error
	Subscribe(channel string)
	UnSubscribe(channel string)
	SubsCount() int
	GetChannels() []string
	GetDBIndex() int
	SelectDB(int)
	SetSlave()
	IsSlave() bool
	SetMaster()
	IsMaster() bool

	SetMultiState(bool)
	InMultiState() bool
	EnqueueCmd([][]byte)
	GetQueuedCmdLine() [][][]byte
	ClearQueuedCmds()
	GetWatching() map[int]map[string]uint32
	ClearWatching()

	AddTxError(reply protocol.Reply)
	GetTxErrors() []protocol.Reply
}
