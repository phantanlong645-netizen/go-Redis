package pubsub

import (
	"go-Redis/datastruct/list"
	"go-Redis/interface/redis"
	"go-Redis/redis/protocol"
	"strconv"
)

var unSubscribeNothing = []byte("*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n")

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
func UnSubscribe(hub *Hub, c redis.Connection, args [][]byte) protocol.Reply {
	var channels []string
	if len(args) > 0 {
		channels = make([]string, len(args))
		for i, b := range args {
			channels[i] = string(b)
		}
	} else {
		channels = c.GetChannels()
	}
	hub.subLocker.Locks(channels...)
	defer hub.subLocker.UnLocks(channels...)
	if len(channels) == 0 {
		_, _ = c.Write(unSubscribeNothing)
		return &protocol.NoReply{}
	}
	for _, channel := range channels {
		if unSubscribe0(hub, c, channel) {
			_, _ = c.Write(makeMsg("unsubscribe", channel, int64(c.SubsCount())))
		}
	}
	return &protocol.NoReply{}

}
func subscribe0(hub *Hub, channel string, client redis.Connection) bool {

	raw, ok := hub.subs.Get(channel)
	var subscribers *list.LinkedList
	if ok {
		subscribers, _ = raw.(*list.LinkedList)
	} else {
		subscribers = list.Make()
		hub.subs.Put(channel, subscribers)
	}
	alreadySubscribed := subscribers.Contains(func(a any) bool {
		sub, ok := a.(redis.Connection)
		return ok && sub == client
	})
	if alreadySubscribed {
		return false
	}
	client.Subscribe(channel)
	subscribers.Add(client)
	return true
}
func unSubscribe0(hub *Hub, client redis.Connection, channel string) bool {
	raw, ok := hub.subs.Get(channel)
	if !ok {
		return false
	}
	subscriber := raw.(*list.LinkedList)
	removed := subscriber.RemoveAllByVal(func(a any) bool {
		sub, ok := a.(redis.Connection)
		return ok && sub == client
	})
	if removed == 0 {
		return false
	}
	client.UnSubscribe(channel)
	if subscriber.Len() == 0 {
		hub.subs.Remove(channel)
	}
	return true

}
