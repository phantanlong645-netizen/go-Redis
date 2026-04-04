package pubsub

import (
	"go-Redis/datastruct/dict"
	"go-Redis/datastruct/list"
	"go-Redis/datastruct/lock"
	"go-Redis/interface/redis"
	"go-Redis/redis/protocol"
	"strconv"
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
func makeMsg(t string, channel string, code int64) []byte {
	return []byte(
		"*3\r\n$" + strconv.FormatInt(int64(len(t)), 10) + "\r\n" + t + "\r\n" +
			"$" + strconv.FormatInt(int64(len(channel)), 10) + "\r\n" + channel + "\r\n" +
			":" + strconv.FormatInt(code, 10) + "\r\n",
	)
}
func Subscribe(hub *Hub, c redis.Connection, args [][]byte) protocol.Reply {
	channels := make([]string, len(args))
	for i, b := range args {
		channels[i] = string(b)
	}
	hub.subLocker.Locks(channels...)
	defer hub.subLocker.UnLocks(channels...)
	for _, channel := range channels {
		if subscribe0(hub, channel, c) {
			_, _ = c.Write(makeMsg("subscribe", channel, int64(c.SubsCount())))
		}
	}
	return &protocol.NoReply{}

}
func subscribe0(hub *Hub, channel string, client redis.Connection) bool {
	client.Subscribe(channel)
	raw, ok := hub.subs.Get(channel)
	var subscribers *list.LinkedList
	if ok {
		subscribers, _ = raw.(*list.LinkedList)
	} else {
		subscribers = list.Make()
		hub.subs.Put(channel, subscribers)
	}
	if subscribers.Contains(func(a any) bool {
		return a == client
	}) {
		return false
	}
	subscribers.Add(client)
	return true

}
