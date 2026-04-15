package database

import (
	"go-Redis/redis/protocol"
)

func execSAdd(db *DB, cmdLine [][]byte) protocol.Reply {
	key := string(cmdLine[0])
	db.IsExpired(key)
	entity, ok := db.Data[key]
	if !ok {
		set := make(map[string]struct{})
		var added int64
		for _, arg := range cmdLine[1:] {
			member := string(arg)
			if _, exist := set[member]; !exist {
				set[member] = struct{}{}
				added++
			}
		}
		db.Data[key] = &DataEntity{
			Type: "set",
			Data: set,
		}
		delete(db.TTL, key)
		db.AddVersion(key)
		return protocol.NewIntReply(added)
	}
	if entity.Type != "set" {
		return protocol.NewErrReply("ERR wrong type")
	}
	set, ok := entity.Data.(map[string]struct{})
	if !ok {
		return protocol.NewErrReply("ERR wrong type")
	}
	var added int64
	for _, arg := range cmdLine[1:] {
		member := string(arg)
		if _, exist := set[member]; !exist {
			set[member] = struct{}{}
			added++
		}
	}
	return protocol.NewIntReply(added)
}
func execSRem(db *DB, cmdLine [][]byte) protocol.Reply {
	key := string(cmdLine[0])
	if db.IsExpired(key) {
		return protocol.NewIntReply(0)
	}
	entity, ok := db.Data[key]
	if !ok {
		return protocol.NewIntReply(0)
	}

	if entity.Type != "set" {
		return protocol.NewErrReply("ERR wrong type")
	}

	set, ok := entity.Data.(map[string]struct{})
	if !ok {
		return protocol.NewErrReply("ERR wrong type")
	}
	var deleted int64
	for _, arg := range cmdLine[1:] {
		member := string(arg)
		if _, exist := set[member]; exist {
			delete(db.Data, key)
			deleted++
		}
	}
	if len(set) == 0 {
		delete(db.TTL, key)
		delete(set, key)
	}
	db.AddVersion(key)
	return protocol.NewIntReply(deleted)
}
func execSIsmember(db *DB, cmdLine [][]byte) protocol.Reply {
	key := string(cmdLine[0])
	member := string(cmdLine[1])

	if db.IsExpired(key) {
		return protocol.NewIntReply(0)
	}

	entity, ok := db.Data[key]
	if !ok {
		return protocol.NewIntReply(0)
	}

	if entity.Type != "set" {
		return protocol.NewErrReply("ERR wrong type")
	}

	set, ok := entity.Data.(map[string]struct{})
	if !ok {
		return protocol.NewErrReply("ERR wrong type")
	}

	if _, exists := set[member]; exists {
		return protocol.NewIntReply(1)
	}
	return protocol.NewIntReply(0)
}
