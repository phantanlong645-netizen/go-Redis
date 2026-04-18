package database

import (
	"go-Redis/datastruct/set"
	"go-Redis/redis/protocol"
)

func execSAdd(db *DB, cmdLine [][]byte) protocol.Reply {
	key := string(cmdLine[0])
	db.IsExpired(key)
	entity, ok := db.Data[key]
	if !ok {
		set := set.Make()
		var added int64
		for _, arg := range cmdLine[1:] {
			member := string(arg)
			result := set.Add(member)
			if result == 1 {
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
	set, ok := entity.Data.(*set.Set)
	if !ok {
		return protocol.NewErrReply("ERR wrong type")
	}
	var added int64
	for _, arg := range cmdLine[1:] {
		member := string(arg)
		result := set.Add(member)
		if result == 1 {
			added++
		}
	}
	if added > 0 {
		db.AddVersion(key)
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

	set, ok := entity.Data.(*set.Set)
	if !ok {
		return protocol.NewErrReply("ERR wrong type")
	}
	var deleted int64
	for _, arg := range cmdLine[1:] {
		member := string(arg)
		deleted += int64(set.Remove(member))
	}
	if deleted == 0 {
		return protocol.NewIntReply(0)
	}
	if set.Len() == 0 {
		delete(db.TTL, key)
		delete(db.Data, key)
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

	set2, ok := entity.Data.(*set.Set)
	if !ok {
		return protocol.NewErrReply("ERR wrong type")
	}

	if exists := set2.Has(member); exists {
		return protocol.NewIntReply(1)
	}
	return protocol.NewIntReply(0)
}
