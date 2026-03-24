package database

import "go-Redis/redis/protocol"

func execGet(db *DB, cmdLine [][]byte) protocol.Reply {
	key := string(cmdLine[0])
	if db.IsExpired(key) {
		return protocol.NewNullBulkReply()
	}
	entity, ok := db.Data[key]
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
	delete(db.TTL, string(cmdLine[0]))
	return protocol.NewStatusReply("OK")
}
func execMGET(db *DB, cmdLine [][]byte) protocol.Reply {
	result := make([][]byte, 0, len(cmdLine))
	for _, v := range cmdLine {
		key := string(v)
		if db.IsExpired(key) {
			result = append(result, []byte(""))
			continue
		}
		entity, ok := db.Data[key]
		if !ok {
			result = append(result, []byte(""))
			continue
		}
		value, ok := entity.Data.([]byte)
		if !ok {
			result = append(result, []byte(""))
			continue
		}
		result = append(result, value)
	}
	return protocol.NewMultiBulkReply(result)
}
func execMSET(db *DB, cmdLine [][]byte) protocol.Reply {
	if len(cmdLine)%2 != 0 {
		return protocol.NewErrReply("ERR wrong number of arguments for MSET")
	}
	for i := 0; i < len(cmdLine); i = i + 2 {
		key := cmdLine[i]
		value := cmdLine[i+1]
		db.Data[string(key)] = &DataEntity{
			Data: value,
			Type: "string",
		}
		delete(db.TTL, string(key))
	}
	return protocol.NewStatusReply("OK")
}
