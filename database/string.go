package database

import "go-Redis/redis/protocol"

func execGet(db *DB, cmdLine [][]byte) protocol.Reply {
	if len(cmdLine) != 1 {
		return protocol.NewErrReply("ERR wrong number of arguments for GET")
	}
	if value, ok := db.Data[string(cmdLine[0])]; !ok {
		return protocol.NewNullBulkReply()
	} else {
		return protocol.NewBulkReply(value)
	}
}
func execSet(db *DB, cmdLine [][]byte) protocol.Reply {
	if len(cmdLine) != 2 {
		return protocol.NewErrReply("ERR wrong number of arguments for SET")
	}
	db.Data[string(cmdLine[0])] = cmdLine[1]
	return protocol.NewStatusReply("OK")
}
