package database

import (
	"go-Redis/redis/protocol"
)

func execHset(db *DB, cmdLine [][]byte) protocol.Reply {
	key := string(cmdLine[0])
	field := string(cmdLine[1])
	value := cmdLine[2]
	if db.IsExpired(key) {

	}
	entity, ok := db.Data[key]
	if !ok {
		hash := make(map[string][]byte)
		hash[field] = value
		db.Data[key] = &DataEntity{
			Type: "hash",
			Data: hash,
		}
		delete(db.TTL, key)
		return protocol.NewIntReply(1)
	}
	//这里是看value的map是否存在，但是不清楚这个map对应的key是否存在  所以下面需要_, ok = hash[field]去判断一下
	hash, ok := entity.Data.(map[string][]byte)
	if !ok {
		return protocol.NewErrReply("ERR wrong type")
	}
	_, ok = hash[field]
	hash[field] = value
	if ok {
		return protocol.NewIntReply(0)
	}
	return protocol.NewIntReply(1)

}
func execHget(db *DB, cmdLine [][]byte) protocol.Reply {
	key := string(cmdLine[0])
	field := string(cmdLine[1])
	if db.IsExpired(key) {
		return protocol.NewNullBulkReply()
	}
	entity, ok := db.Data[key]
	if !ok {
		return protocol.NewNullBulkReply()
	}
	hash, ok := entity.Data.(map[string][]byte)
	if !ok {
		return protocol.NewErrReply("ERR wrong type")
	}
	value, ok := hash[field]
	if !ok {
		return protocol.NewNullBulkReply()
	}
	return protocol.NewBulkReply(value)
}
func execHexists(db *DB, cmdLine [][]byte) protocol.Reply {
	key := string(cmdLine[0])
	field := string(cmdLine[1])
	if db.IsExpired(key) {
		return protocol.NewIntReply(0)
	}
	entity, ok := db.Data[key]
	if !ok {
		return protocol.NewIntReply(0)
	}
	if entity.Type != "hash" {
		return protocol.NewErrReply("ERR wrong type")
	}
	hash, ok2 := entity.Data.(map[string][]byte)
	if !ok2 {
		return protocol.NewIntReply(0)
	}

	if _, ok3 := hash[field]; ok3 {
		return protocol.NewIntReply(1)
	}
	return protocol.NewIntReply(0)
}
func execHdel(db *DB, cmdLine [][]byte) protocol.Reply {
	key := string(cmdLine[0])
	if db.IsExpired(key) {
		return protocol.NewIntReply(0)
	}
	entity, ok := db.Data[key]
	if !ok {
		return protocol.NewIntReply(0)
	}
	if entity.Type != "hash" {
		return protocol.NewErrReply("ERR wrong type")
	}
	hash, ok := entity.Data.(map[string][]byte)
	if !ok {
		return protocol.NewErrReply("ERR wrong type")
	}
	var deleted int64
	for _, arg := range cmdLine[1:] {
		field := string(arg)
		if _, ok2 := hash[field]; ok2 {
			delete(hash, field)
			deleted++
		}
	}
	if len(hash) == 0 {
		delete(db.Data, key)
		delete(db.TTL, key)
	}
	return protocol.NewIntReply(deleted)
}
