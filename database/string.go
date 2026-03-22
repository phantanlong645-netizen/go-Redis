package database

import "go-Redis/redis/protocol"

func execGet(db *DB, cmdLine [][]byte) protocol.Reply {
	entity, ok := db.Data[string(cmdLine[0])]
	if !ok {
		return protocol.NewNullBulkReply()
	}
	value, ok := entity.Data.([]byte)
	if !ok {
		return protocol.NewErrReply("ERR wrong type")
	}
	return protocol.NewBulkReply(value)
}
func execSet(db *DB, cmdLine [][]byte) protocol.Reply {
	db.Data[string(cmdLine[0])] = &DataEntity{
		Data: cmdLine[1],
		Type: "string",
	}
	return protocol.NewStatusReply("OK")
}
