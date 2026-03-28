package database

import (
	"go-Redis/redis/protocol"
	"sort"
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
	if !ok {
		db.Data[key] = &DataEntity{
			Type: "zset",
			Data: map[string]float64{
				member: score,
			},
		}
		delete(db.TTL, key)
		return protocol.NewIntReply(1)
	}
	if entity.Type != "zset" {
		return protocol.NewErrReply("ERR wrong type")
	}
	set, ok := entity.Data.(map[string]float64)
	if !ok {
		return protocol.NewErrReply("ERR wrong type")
	}
	_, exist := set[member]
	set[member] = score
	if exist {
		return protocol.NewIntReply(0)
	} else {
		return protocol.NewIntReply(1)
	}
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

	zset, ok := entity.Data.(map[string]float64)
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
	entries := make([]zsetEntry, 0, len(zset))
	for member, score := range zset {
		entries = append(entries, zsetEntry{
			member: member,
			score:  score,
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].score == entries[j].score {
			return entries[i].member < entries[j].member
		}
		return entries[i].score < entries[j].score
	})
	length := len(entries)
	if length == 0 {
		return protocol.NewMultiBulkReply([][]byte{})
	}

	// 处理负索引，例如 -1 表示最后一个元素。
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
		result = append(result, []byte(entries[i].member))
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

	zset, ok := entity.Data.(map[string]float64)
	if !ok {
		return protocol.NewErrReply("ERR wrong type")
	}

	score, ok := zset[member]
	if !ok {
		return protocol.NewNullBulkReply()
	}

	return protocol.NewBulkReply([]byte(strconv.FormatFloat(score, 'f', -1, 64)))
}
