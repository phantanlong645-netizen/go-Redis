package database

import (
	"go-Redis/redis/protocol"
	"strconv"
)

func execLPush(db *DB, cmdLine [][]byte) protocol.Reply {
	key := string(cmdLine[0])
	db.IsExpired(key)
	entity, ok := db.Data[key]
	if !ok {
		list := make([][]byte, 0, len(cmdLine)-1)
		for i := 1; i < len(cmdLine); i++ {
			list = append([][]byte{cmdLine[i]}, list...)
		}
		db.Data[key] = &DataEntity{
			Type: "list",
			Data: list,
		}
		delete(db.TTL, key)
		return protocol.NewIntReply(int64(len(list)))
	}
	if entity.Type != "list" {
		return protocol.NewErrReply("ERR wrong type")
	}
	list, ok := entity.Data.([][]byte)
	if !ok {
		return protocol.NewErrReply("ERR wrong type")
	}
	for i := 1; i < len(cmdLine); i++ {
		list = append([][]byte{cmdLine[i]}, list...)
	}
	entity.Data = list
	return protocol.NewIntReply(int64(len(list)))
}
func execLPop(db *DB, cmdLine [][]byte) protocol.Reply {
	key := string(cmdLine[0])
	if db.IsExpired(key) {
		return protocol.NewNullBulkReply()
	}
	entity, ok := db.Data[key]
	if !ok {
		return protocol.NewNullBulkReply()
	}
	if entity.Type != "list" {
		return protocol.NewErrReply("ERR wrong type")
	}
	list, ok := entity.Data.([][]byte)
	if !ok {
		return protocol.NewErrReply("ERR wrong type")
	}
	if len(list) == 0 {
		return protocol.NewNullBulkReply()
	}
	value := list[0]
	list = list[1:]
	if len(list) == 0 {
		delete(db.Data, key)
		delete(db.TTL, key)
	} else {
		entity.Data = list
	}
	return protocol.NewBulkReply(value)
}
func execLRange(db *DB, cmdLine [][]byte) protocol.Reply {
	key := string(cmdLine[0])
	if db.IsExpired(key) {
		return protocol.NewMultiBulkReply([][]byte{})
	}
	entity, ok := db.Data[key]
	if !ok {
		return protocol.NewMultiBulkReply([][]byte{})
	}
	if entity.Type != "list" {
		return protocol.NewErrReply("ERR wrong type")
	}
	list, ok := entity.Data.([][]byte)
	start, err := strconv.Atoi(string(cmdLine[1]))
	if err != nil {
		return protocol.NewErrReply("ERR invalid start index")
	}
	stop, err := strconv.Atoi(string(cmdLine[2]))
	if err != nil {
		return protocol.NewErrReply("ERR invalid stop index")
	}
	length := len(list)
	if length == 0 {
		return protocol.NewMultiBulkReply([][]byte{})
	}
	if start < 0 {
		start = start + length
	}
	if stop < 0 {
		stop = stop + length
	}
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start >= length || stop < 0 {
		return protocol.NewMultiBulkReply([][]byte{})
	}
	res := make([][]byte, 0, stop-start-1)
	for i := start; i <= stop; i++ {
		res = append(res, list[i])
	}
	return protocol.NewMultiBulkReply(res)
}
func execRPush(db *DB, cmdLine [][]byte) protocol.Reply {
	key := string(cmdLine[0])
	db.IsExpired(key)
	entity, ok := db.Data[key]
	if !ok {
		list := make([][]byte, 0, len(cmdLine)-1)
		for i := 1; i < len(cmdLine); i++ {
			list = append(list, cmdLine[i])
		}
		db.Data[key] = &DataEntity{
			Type: "list",
			Data: list,
		}
		delete(db.TTL, key)
		return protocol.NewIntReply(int64(len(list)))
	}
	if entity.Type != "list" {
		return protocol.NewErrReply("ERR wrong type")
	}
	list, ok := entity.Data.([][]byte)
	if !ok {
		return protocol.NewErrReply("ERR wrong type")
	}
	for i := 1; i < len(cmdLine); i++ {
		list = append(list, cmdLine[i])
	}
	entity.Data = list
	return protocol.NewIntReply(int64(len(list)))
}
