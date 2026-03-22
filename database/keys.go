package database

import "go-Redis/redis/protocol"

func execPing(db *DB, cmdLine [][]byte) protocol.Reply {
	if len(cmdLine) != 0 {
		return protocol.NewErrReply("ERR wrong number of arguments for PING")
	}
	return protocol.NewStatusReply("PONG")
}
func execDel(db *DB, cmdLine [][]byte) protocol.Reply {
	if len(cmdLine) < 1 {
		return protocol.NewErrReply("ERR wrong number of arguments for DEL")
	}
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
