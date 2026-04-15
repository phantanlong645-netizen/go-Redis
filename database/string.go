package database

import (
	"go-Redis/redis/protocol"
	"strconv"
)

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
	key := string(cmdLine[0])
	db.Data[key] = &DataEntity{
		Data: cmdLine[1],
		Type: "string",
	}
	delete(db.TTL, key)
	db.AddVersion(key)
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
	keys := make([]string, 0, len(cmdLine)/2)
	for i := 0; i < len(cmdLine); i = i + 2 {
		key := cmdLine[i]
		value := cmdLine[i+1]
		db.Data[string(key)] = &DataEntity{
			Data: value,
			Type: "string",
		}
		delete(db.TTL, string(key))
		keys = append(keys, string(key))
	}
	db.AddVersion(keys...)
	return protocol.NewStatusReply("OK")
}
func execIncr(db *DB, cmdLine [][]byte) protocol.Reply {
	key := string(cmdLine[0])
	if db.IsExpired(key) {
		db.Data[key] = &DataEntity{
			Type: "string",
			Data: []byte("1"),
		}
		delete(db.TTL, key)
		db.AddVersion(key)
		return protocol.NewIntReply(1)
	}
	entity, ok := db.Data[key]
	if !ok {
		db.Data[key] = &DataEntity{
			Type: "string",
			Data: []byte("1"),
		}
		delete(db.TTL, key)
		db.AddVersion(key)
		return protocol.NewIntReply(1)
	}
	if entity.Type != "string" {
		return protocol.NewErrReply("ERR wrong type")
	}
	value, ok := entity.Data.([]byte)
	if !ok {
		return protocol.NewErrReply("ERR wrong type")
	}
	num, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil {
		return protocol.NewErrReply("ERR value is not an integer")
	}
	num++
	db.AddVersion(key)
	entity.Data = []byte(strconv.FormatInt(num, 10))
	return protocol.NewIntReply(num)

}
func execDecr(db *DB, cmdLine [][]byte) protocol.Reply {
	key := string(cmdLine[0])
	if db.IsExpired(key) {
		db.Data[key] = &DataEntity{
			Type: "string",
			Data: []byte("-1"),
		}
		delete(db.TTL, key)
		db.AddVersion(key)
		return protocol.NewIntReply(-1)
	}
	entity, ok := db.Data[key]
	if !ok {
		db.Data[key] = &DataEntity{
			Type: "string",
			Data: []byte("-1"),
		}
		delete(db.TTL, key)
		db.AddVersion(key)
		return protocol.NewIntReply(-1)
	}
	if entity.Type != "string" {
		return protocol.NewErrReply("ERR wrong type")
	}
	value, ok := entity.Data.([]byte)
	if !ok {
		return protocol.NewErrReply("ERR wrong type")
	}
	num, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil {
		return protocol.NewErrReply("ERR value is not an integer")
	}
	num--
	db.AddVersion(key)
	entity.Data = []byte(strconv.FormatInt(num, 10))
	return protocol.NewIntReply(num)
}
