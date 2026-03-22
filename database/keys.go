package database

import "go-Redis/redis/protocol"

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
