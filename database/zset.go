package database

import (
	zset2 "go-Redis/datastruct/zset"
	"go-Redis/redis/protocol"
	"strconv"
)

func execZAdd(db *DB, cmdLine [][]byte) protocol.Reply {
	key := string(cmdLine[0])
	db.IsExpired(key)
	score, err := strconv.ParseFloat(string(cmdLine[1]), 64)
	if err != nil {
		return protocol.NewErrReply("ERR invalid score")
	}
	member := string(cmdLine[2])
	entity, ok := db.Data[key]
	var zset *zset2.SortedSet
	if !ok {
		zset = zset2.Make()
		db.Data[key] = &DataEntity{
			Type: "zset",
			Data: zset,
		}
		delete(db.TTL, key)
	} else {
		if entity.Type != "zset" {
			return protocol.NewErrReply("ERR wrong type")
		}
		var typeOK bool
		zset, typeOK = entity.Data.(*zset2.SortedSet)
		if !typeOK {
			return protocol.NewErrReply("ERR wrong type")
		}
	}
	if old, exists := zset.Get(member); exists && old.Score == score {
		return protocol.NewIntReply(0)
	}

	added := zset.Add(member, score)
	db.AddVersion(key)
	if added {
		return protocol.NewIntReply(1)
	}
	return protocol.NewIntReply(0)
}
func execZRange(db *DB, cmdLine [][]byte) protocol.Reply {
	key := string(cmdLine[0])

	if db.IsExpired(key) {
		return protocol.NewMultiBulkReply([][]byte{})
	}

	entity, ok := db.Data[key]
	if !ok {
		return protocol.NewMultiBulkReply([][]byte{})
	}

	if entity.Type != "zset" {
		return protocol.NewErrReply("ERR wrong type")
	}

	zset, ok := entity.Data.(*zset2.SortedSet)
	if !ok {
		return protocol.NewErrReply("ERR wrong type")
	}

	start, err := strconv.Atoi(string(cmdLine[1]))
	if err != nil {
		return protocol.NewErrReply("ERR invalid start index")
	}
	stop, err := strconv.Atoi(string(cmdLine[2]))
	if err != nil {
		return protocol.NewErrReply("ERR invalid stop index")
	}

	length := int(zset.Len())
	if length == 0 {
		return protocol.NewMultiBulkReply([][]byte{})
	}

	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start >= length || start > stop {
		return protocol.NewMultiBulkReply([][]byte{})
	}

	result := make([][]byte, 0, stop-start+1)
	for i := start; i <= stop; i++ {
		element, exists := zset.GetByRank(int64(i + 1)) // 1-based rank
		if !exists {
			break
		}
		result = append(result, []byte(element.Member))
	}
	return protocol.NewMultiBulkReply(result)
}

type zsetEntry struct {
	member string
	score  float64
}

func execZScore(db *DB, cmdLine [][]byte) protocol.Reply {
	key := string(cmdLine[0])
	member := string(cmdLine[1])

	if db.IsExpired(key) {
		return protocol.NewNullBulkReply()
	}

	entity, ok := db.Data[key]
	if !ok {
		return protocol.NewNullBulkReply()
	}

	if entity.Type != "zset" {
		return protocol.NewErrReply("ERR wrong type")
	}

	zset, ok := entity.Data.(*zset2.SortedSet)
	if !ok {
		return protocol.NewErrReply("ERR wrong type")
	}

	element, ok := zset.Get(member)
	if !ok {
		return protocol.NewNullBulkReply()
	}

	return protocol.NewBulkReply([]byte(strconv.FormatFloat(element.Score, 'f', -1, 64)))
}
