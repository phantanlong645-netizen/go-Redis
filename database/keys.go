package database

import (
	"go-Redis/redis/protocol"
	"strconv"
	"time"
)

func execPing(db *DB, cmdLine [][]byte) protocol.Reply {
	return protocol.NewStatusReply("PONG")
}
func execDel(db *DB, cmdLine [][]byte) protocol.Reply {
	var deleteCount int64
	for _, arg := range cmdLine {
		key := string(arg)
		if db.IsExpired(key) {
			continue
		}
		if _, ok := db.Data[key]; ok {
			delete(db.Data, key)
			delete(db.TTL, key)
			deleteCount++
		}
	}
	return protocol.NewIntReply(deleteCount)
}
func execExists(db *DB, cmdLine [][]byte) protocol.Reply {
	var count int64
	for _, arg := range cmdLine {
		key := string(arg)
		if db.IsExpired(key) {
			continue
		}
		if _, ok := db.Data[key]; ok {
			count++
		}
	}
	return protocol.NewIntReply(count)
}
func execKeys(db *DB, cmdLine [][]byte) protocol.Reply {
	key := string(cmdLine[0])
	if key != "*" {
		return protocol.NewErrReply("ERR only * pattern is supported")
	}
	result := make([][]byte, 0, len(db.Data))
	for k, _ := range db.Data {
		if db.IsExpired(k) {
			continue
		}
		result = append(result, []byte(k))
	}
	return protocol.NewMultiBulkReply(result)
}
func execType(db *DB, cmdLine [][]byte) protocol.Reply {
	key := string(cmdLine[0])
	if db.IsExpired(key) {
		return protocol.NewStatusReply("none")
	}
	if value, ok := db.Data[key]; ok {
		return protocol.NewStatusReply(value.Type)
	}
	return protocol.NewStatusReply("none")

}
func (db *DB) IsExpired(key string) bool {
	deadLine, ok := db.TTL[key]
	if !ok {
		return false
	}
	if time.Now().After(deadLine) {
		delete(db.TTL, key)
		delete(db.Data, key)
		return true
	}
	return false
}
func execExpire(db *DB, cmdLine [][]byte) protocol.Reply {
	key := string(cmdLine[0])
	if db.IsExpired(key) {
		return protocol.NewIntReply(0)
	}
	if _, ok := db.Data[key]; !ok {
		return protocol.NewIntReply(0)
	}
	seconds, err := strconv.Atoi(string(cmdLine[1]))
	if err != nil || seconds < 0 {
		return protocol.NewErrReply("ERR invalid expire time")
	}
	db.TTL[key] = time.Now().Add(time.Duration(seconds) * time.Second)
	return protocol.NewIntReply(1)
}
func execTTL(db *DB, cmdLine [][]byte) protocol.Reply {
	key := string(cmdLine[0])
	if db.IsExpired(key) {
		return protocol.NewIntReply(-2)
	}
	if _, ok := db.Data[key]; !ok {
		return protocol.NewIntReply(-2)
	}
	expireTime, ok := db.TTL[key]
	if !ok {
		return protocol.NewIntReply(-1)
	}
	ttl := int64(time.Until(expireTime).Seconds())
	if ttl < 0 {
		delete(db.TTL, key)
		delete(db.Data, key)
		return protocol.NewIntReply(-2)
	}
	return protocol.NewIntReply(ttl)
}
