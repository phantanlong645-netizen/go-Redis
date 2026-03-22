package database

import "go-Redis/redis/protocol"

func execGet(db *DB, cmdLine [][]byte) protocol.Reply {
	if value, ok := db.Data[string(cmdLine[0])]; !ok {
		return protocol.NewNullBulkReply()
	} else {
		return protocol.NewBulkReply(value)
	}
}
func execSet(db *DB, cmdLine [][]byte) protocol.Reply {
	db.Data[string(cmdLine[0])] = cmdLine[1]
	return protocol.NewStatusReply("OK")
}
