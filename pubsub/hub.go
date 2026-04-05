package pubsub

import (
	"go-Redis/datastruct/dict"
	"go-Redis/datastruct/lock"
)

type Hub struct {
	subLocker *lock.Locks
	//频道对应的连接
	subs dict.Dict
}

func MakeHub() *Hub {
	return &Hub{
		subLocker: lock.Make(16),
		subs:      dict.MakeConcurrent(4),
	}
}
