package redis

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
}
