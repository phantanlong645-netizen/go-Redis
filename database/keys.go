package database

import (
	"go-Redis/redis/protocol"
)

func execPing(db *DB, cmdLine [][]byte) protocol.Reply {
	return protocol.NewStatusReply("PONG")
}
func execDel(db *DB, cmdLine [][]byte) protocol.Reply {
	var deleteCount int64
	for _, arg := range cmdLine {
		key := string(arg)
		if _, ok := db.Data[key]; ok {
			delete(db.Data, key)
			deleteCount++
		}
	}
	return protocol.NewIntReply(deleteCount)
}
func execExists(db *DB, cmdLine [][]byte) protocol.Reply {
	var count int64
	for _, arg := range cmdLine {
		key := string(arg)
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
		result = append(result, []byte(k))
	}
	return protocol.NewMultiBulkReply(result)
}
func execType(db *DB, cmdLine [][]byte) protocol.Reply {
	key := string(cmdLine[0])
	if value, ok := db.Data[key]; ok {
		return protocol.NewStatusReply(value.Type)
	}
	return protocol.NewStatusReply("none")

}
